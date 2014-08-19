package uk.co.omegaprime;

import au.com.bytecode.opencsv.CSVReader;
import uk.co.omegaprime.thunder.Cursor;
import uk.co.omegaprime.thunder.Database;
import uk.co.omegaprime.thunder.Index;
import uk.co.omegaprime.thunder.Transaction;
import uk.co.omegaprime.thunder.schema.Latin1StringSchema;
import uk.co.omegaprime.thunder.schema.LocalDateSchema;
import uk.co.omegaprime.thunder.schema.Schema;
import static uk.co.omegaprime.TemporalDenseLoader.FieldKey;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.zip.ZipInputStream;

public class TemporalSparseLoader {
    public static class FieldValue {
        public static Schema<FieldValue> SCHEMA = Schema.zipWith(new Latin1StringSchema(64),                FieldValue::getValue,
                                                                 Schema.nullable(LocalDateSchema.INSTANCE), FieldValue::getToDate,
                                                                 FieldValue::new);

        private final String value;
        private final LocalDate toDate;

        public FieldValue(String value, LocalDate toDate) {
            this.value = value;
            this.toDate = toDate;
        }

        public String getValue() { return value; }
        public LocalDate getToDate() { return toDate; }

        public FieldValue setToDate(LocalDate toDate) { return new FieldValue(value, toDate); }
    }

    public static void load(Database db, Transaction tx, LocalDate date, ZipInputStream zis) throws IOException {
        final CSVReader reader = new CSVReader(new InputStreamReader(zis), '|');
        String[] headers = reader.readNext();
        if (headers == null || headers.length == 1) {
            // Empty file
            return;
        }

        int idBBGlobalIx = -1;
        final Cursor<FieldKey, FieldValue>[] cursors = (Cursor<FieldKey, FieldValue>[]) new Cursor[headers.length];
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equals("ID_BB_GLOBAL")) {
                idBBGlobalIx = i;
            } else {
                final String indexName = headers[i].replace(" ", "");
                final Index<FieldKey, FieldValue> index = db.createIndex(tx, indexName, FieldKey.SCHEMA, FieldValue.SCHEMA);
                cursors[i] = index.createCursor(tx);
            }
        }

        if (idBBGlobalIx < 0) {
            throw new IllegalArgumentException("No ID_BB_GLOBAL field");
        }

        int items = 0;
        long startTime = System.nanoTime();

        String[] line;
        while ((line = reader.readNext()) != null) {
            if (line.length < headers.length) {
                continue;
            }

            final FieldKey fieldKey = new FieldKey(line[idBBGlobalIx], date);
            for (int i = 0; i < headers.length; i++) {
                if (i == idBBGlobalIx) continue;

                final Cursor<FieldKey, FieldValue> cursor = cursors[i];
                final String value = line[i].trim();

                items++;

                if (value.length() == 0) {
                    if (cursor.moveFloor(fieldKey) && cursor.getKey().getIDBBGlobal().equals(fieldKey.getIDBBGlobal())) {
                        // There is a range in the map starting before the date of interest: we might have to truncate it
                        final FieldValue fieldValue = cursor.getValue();
                        if (fieldValue.getToDate() == null || fieldValue.getToDate().isAfter(date)) {
                            cursor.put(fieldValue.setToDate(date));
                        }
                    }
                } else {
                    final boolean mustCreate;
                    if (cursor.moveFloor(fieldKey) && cursor.getKey().getIDBBGlobal().equals(fieldKey.getIDBBGlobal())) {
                        final FieldValue fieldValue = cursor.getValue();
                        if (fieldValue.getToDate() == null || fieldValue.getToDate().isAfter(date)) {
                            // There is a range in the map enclosing the date of interest
                            if (fieldValue.value.equals(value)) {
                                mustCreate = false;
                            } else {
                                cursor.put(fieldValue.setToDate(date));
                                mustCreate = true;
                            }
                        } else {
                            // The earlier range has nothing to say about this date: just add
                            mustCreate = true;
                        }
                    } else {
                        // This is the earliest date for the (security, field) pair: just add
                        mustCreate = true;
                    }

                    if (mustCreate) {
                        cursor.put(fieldKey, new FieldValue(value, null));
                    }
                }
            }
        }

        long duration = System.nanoTime() - startTime;
        System.out.println("Loaded " + items + " in " + duration + "ns (" + (duration / items) + "ns/item)");
    }
}
