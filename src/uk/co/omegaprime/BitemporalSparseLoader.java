package uk.co.omegaprime;

import au.com.bytecode.opencsv.CSVReader;
import uk.co.omegaprime.thunder.*;
import uk.co.omegaprime.thunder.schema.IntegerSchema;
import uk.co.omegaprime.thunder.schema.Latin1StringSchema;
import uk.co.omegaprime.thunder.schema.LocalDateSchema;
import uk.co.omegaprime.thunder.schema.Schema;
import static uk.co.omegaprime.TemporalSparseLoader.FieldValue;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.zip.ZipInputStream;

public class BitemporalSparseLoader {
    public static class SourceTemporalFieldKey {
        public static Schema<SourceTemporalFieldKey> SCHEMA = Schema.zipWith(new Latin1StringSchema(20), SourceTemporalFieldKey::getIDBBGlobal,
                                                                             IntegerSchema.INSTANCE,     SourceTemporalFieldKey::getSourceID,
                                                                             SourceTemporalFieldKey::new);

        private final String idBBGlobal;
        private final int sourceID;

        public SourceTemporalFieldKey(String idBBGlobal, int sourceID) {
            this.idBBGlobal = idBBGlobal;
            this.sourceID = sourceID;
        }

        public String getIDBBGlobal() { return idBBGlobal; }
        public int getSourceID() { return sourceID; }
    }

    public static class FieldKey {
        public static Schema<FieldKey> SCHEMA;

        static {
            final Schema<Pair<String, LocalDate>> SCHEMA0 = Schema.zip(new Latin1StringSchema(20), LocalDateSchema.INSTANCE);
            SCHEMA = Schema.zipWith(SCHEMA0, (FieldKey key) -> new Pair<>(key.getIDBBGlobal(), key.getDate()),
                                    IntegerSchema.INSTANCE, FieldKey::getSourceID,
                                    (Pair<String, LocalDate> pair, Integer sourceID) -> new FieldKey(pair.k, pair.v, sourceID));
        }

        private final String idBBGlobal;
        private final LocalDate date;
        private final int sourceID;

        public FieldKey(String idBBGlobal, LocalDate date, int sourceID) {
            this.idBBGlobal = idBBGlobal;
            this.date = date;
            this.sourceID = sourceID;
        }

        public String getIDBBGlobal() { return idBBGlobal; }
        public LocalDate getDate() { return date; }
        public int getSourceID() { return sourceID; }
    }

    public static class FieldValue {
        public static Schema<FieldValue> SCHEMA;

        static {
            final Schema<Pair<LocalDate, Integer>> SCHEMA0 = Schema.zip(Schema.nullable(LocalDateSchema.INSTANCE), Schema.nullable(IntegerSchema.INSTANCE));
            SCHEMA = Schema.zipWith(SCHEMA0, (FieldValue value) -> new Pair<>(value.getToDate(), value.getToSourceID()),
                                    new Latin1StringSchema(64), FieldValue::getValue,
                                    (Pair<LocalDate, Integer> pair, String value) -> new FieldValue(pair.k, pair.v, value));
        }

        private final LocalDate toDate;
        private final Integer toSourceID;
        private final String value;

        public FieldValue(LocalDate toDate, Integer toSourceID, String value) {
            this.toDate = toDate;
            this.toSourceID = toSourceID;
            this.value = value;
        }

        public LocalDate getToDate() { return toDate; }
        public Integer getToSourceID() { return toSourceID; }
        public String getValue() { return value; }

        public FieldValue setToDate(LocalDate toDate) { return new FieldValue(toDate, toSourceID, value); }
    }

    public static class Source {
        public static final Schema<Source> SCHEMA = Schema.zipWith(new Latin1StringSchema(10), Source::getPartition,
                                                                   LocalDateSchema.INSTANCE,   Source::getDate,
                                                                   Source::new);

        private final String partition;
        private final LocalDate date;

        public Source(String partition, LocalDate date) {
            this.partition = partition;
            this.date = date;
        }

        public String getPartition() { return partition; }
        public LocalDate getDate() { return date; }
    }

    public static void load(Database db, Transaction tx, LocalDate date, ZipInputStream zis) throws IOException {
        final CSVReader reader = new CSVReader(new InputStreamReader(zis), '|');
        String[] headers = reader.readNext();
        if (headers == null || headers.length == 1) {
            // Empty file
            return;
        }

        // TODO:
        // Eliminate duplicate rows + non-changing changes
        //

        final Cursor<Integer, Source> sourceCursor = db.createIndex(tx, "Source", IntegerSchema.INSTANCE, Source.SCHEMA).createCursor(tx);
        final int sourceID = sourceCursor.moveLast() ? sourceCursor.getKey() + 1 : 0;
        sourceCursor.put(sourceID, new Source("trivial", date));

        // FIXME: handle updating of these
        final Cursor<SourceTemporalFieldKey, String>    lastKnownDeliveryCursor = db.createIndex(tx, "LastKnownDelivery", SourceTemporalFieldKey.SCHEMA, new Latin1StringSchema(10)).createCursor(tx);
        final Cursor<SourceTemporalFieldKey, LocalDate> lastKnownDateCursor     = db.createIndex(tx, "LastKnownDate",     SourceTemporalFieldKey.SCHEMA, LocalDateSchema.INSTANCE  ).createCursor(tx);

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

            final String idBBGlobal = line[idBBGlobalIx];
            final FieldKey fieldKey = new FieldKey(idBBGlobal, date, sourceID);
            for (int i = 0; i < headers.length; i++) {
                if (i == idBBGlobalIx) continue;

                final Cursor<FieldKey, FieldValue> cursor = cursors[i];
                final String value = line[i].trim();

                items++;

                if (cursor.moveFloor(fieldKey)) {
                    boolean found = false;
                    FieldKey existingFieldKey;
                    FieldValue existingFieldValue = null;
                    do {
                        existingFieldKey = cursor.getKey();
                        if (!existingFieldKey.idBBGlobal.equals(fieldKey.idBBGlobal) || cursor.getKey().date.isAfter(date)) {
                            break;
                        }

                        existingFieldValue = cursor.getValue();
                        if (existingFieldValue.getToDate() == null || existingFieldValue.getToDate().isAfter(date)) {
                            if (existingFieldValue.getToSourceID() == null) {
                                found = true;
                                break;
                            }
                        }
                    } while (cursor.moveNext());

                    if (found) {
                        // This range intersects the date we are trying to add
                        //
                        // We already have a tuple in the DB of the form (FromSource, ToSource, FromDate, ToDate):
                        //  (S_f, null, F, T) -> v
                        // We need to split this into at most 4 tuples:
                        //  1. (S_f, null, F,   D) -> v
                        //  2. (S_f, null, D+1, T) -> v
                        //  3. (S_f, S,    D, D+1) -> v
                        //  4. (S,   null, D, D+1) -> v'
                        // Where:
                        //  a) Any tuple can be omitted if FromDate >= min(ToDate, max(LKBD( [FromSource, ISNULL(ToSource, S)) ))
                        //  b) The first D+1 ToDate can be set to null if D+1 is > max(LKBD( [FromSource, S) ))
                        //  c) The second D+1 ToDate can be set to null if D+1 is > the LKBD for S
                        // FIXME: implement a b c conditions
                        cursor.put(new FieldValue(date, null, existingFieldValue.getValue()));
                        cursor.put(new FieldKey(idBBGlobal, date.plusDays(1), existingFieldKey.getSourceID()),
                                   existingFieldValue);
                        cursor.put(new FieldKey(idBBGlobal, date, existingFieldKey.getSourceID()),
                                   new FieldValue(date.plusDays(1), sourceID, existingFieldValue.getValue()));
                    }
                }

                if (value.length() != 0) {
                    cursor.put(new FieldKey(idBBGlobal, date, sourceID),
                               new FieldValue(date.plusDays(1), null, value));
                }
            }
        }

        long duration = System.nanoTime() - startTime;
        System.out.println("Loaded " + items + " in " + duration + "ns (" + (duration / items) + "ns/item)");
    }
}
