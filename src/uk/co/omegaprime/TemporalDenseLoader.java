package uk.co.omegaprime;

import au.com.bytecode.opencsv.CSVReader;
import uk.co.omegaprime.thunder.Cursor;
import uk.co.omegaprime.thunder.Database;
import uk.co.omegaprime.thunder.Environment;
import uk.co.omegaprime.thunder.Transaction;
import uk.co.omegaprime.thunder.schema.Latin1StringSchema;
import uk.co.omegaprime.thunder.schema.LocalDateSchema;
import uk.co.omegaprime.thunder.schema.Schema;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.zip.ZipInputStream;

public class TemporalDenseLoader {
    public static class FieldKey {
        public static Schema<FieldKey> SCHEMA = Schema.zipWith(new Latin1StringSchema(20), FieldKey::getIDBBGlobal,
                                                               LocalDateSchema.INSTANCE,   FieldKey::getDate,
                                                               FieldKey::new);

        private final String idBBGlobal;
        private final LocalDate date;

        public FieldKey(String idBBGlobal, LocalDate date) {
            this.idBBGlobal = idBBGlobal;
            this.date = date;
        }

        public String getIDBBGlobal() { return idBBGlobal; }
        public LocalDate getDate() { return date; }
    }

    public static void load(Environment db, Transaction tx, LocalDate date, ZipInputStream zis) throws IOException {
        final CSVReader reader = new CSVReader(new InputStreamReader(zis), '|');
        String[] headers = reader.readNext();
        if (headers == null || headers.length == 1) {
            // Empty file
            return;
        }

        int idBBGlobalIx = -1;
        final Cursor<FieldKey, String>[] cursors = (Cursor<FieldKey, String>[]) new Cursor[headers.length];
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equals("ID_BB_GLOBAL")) {
                idBBGlobalIx = i;
            } else {
                final String dbName = headers[i].replace(" ", "");
                final Database<FieldKey, String> database = db.createDatabase(tx, dbName, FieldKey.SCHEMA, new Latin1StringSchema(64));
                cursors[i] = database.createCursor(tx);
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

            final FieldKey key = new FieldKey(line[idBBGlobalIx], date);
            for (int i = 0; i < headers.length; i++) {
                if (i == idBBGlobalIx) continue;

                final Cursor<FieldKey, String> cursor = cursors[i];
                final String value = line[i].trim();

                items++;

                if (value.length() == 0) {
                    if (cursor.moveTo(key)) {
                        cursor.delete();
                    }
                } else {
                    cursor.put(key, value);
                }
            }
        }

        for (Cursor cursor : cursors) {
            cursor.close();
        }

        long duration = System.nanoTime() - startTime;
        System.out.println("Loaded " + items + " in " + (duration / 1000 / 1000.0) + "ms (" + (duration / items) + "ns/item)");
    }
}
