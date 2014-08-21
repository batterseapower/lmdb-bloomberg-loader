package uk.co.omegaprime;

import au.com.bytecode.opencsv.CSVReader;
import uk.co.omegaprime.thunder.*;
import uk.co.omegaprime.thunder.schema.IntegerSchema;
import uk.co.omegaprime.thunder.schema.Latin1StringSchema;
import uk.co.omegaprime.thunder.schema.LocalDateSchema;
import uk.co.omegaprime.thunder.schema.Schema;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.HashSet;
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

    public static class SparseSourceTemporalFieldValue<T> {
        public static <T> Schema<SparseSourceTemporalFieldValue<T>> schema(Schema<T> tSchema) {
            return Schema.zipWith(Schema.nullable(IntegerSchema.INSTANCE), SparseSourceTemporalFieldValue::getToSourceID,
                                  tSchema,                                 SparseSourceTemporalFieldValue::getValue,
                                  SparseSourceTemporalFieldValue::new);
        }

        private final Integer toSourceID;
        private final T value;

        public SparseSourceTemporalFieldValue(Integer toSourceID, T value) {
            this.toSourceID = toSourceID;
            this.value = value;
        }

        public Integer getToSourceID() { return toSourceID; }
        public T getValue() { return value; }
    }

    public static class TemporalFieldKey {
        public static Schema<TemporalFieldKey> SCHEMA;

        static {
            final Schema<Pair<String, LocalDate>> SCHEMA0 = Schema.zip(new Latin1StringSchema(20), LocalDateSchema.INSTANCE);
            SCHEMA = Schema.zipWith(SCHEMA0, (TemporalFieldKey key) -> new Pair<>(key.getIDBBGlobal(), key.getDate()),
                                    IntegerSchema.INSTANCE, TemporalFieldKey::getSourceID,
                                    (Pair<String, LocalDate> pair, Integer sourceID) -> new TemporalFieldKey(pair.k, pair.v, sourceID));
        }

        private final String idBBGlobal;
        private final LocalDate date;
        private final int sourceID;

        public TemporalFieldKey(String idBBGlobal, LocalDate date, int sourceID) {
            this.idBBGlobal = idBBGlobal;
            this.date = date;
            this.sourceID = sourceID;
        }

        public String getIDBBGlobal() { return idBBGlobal; }
        public LocalDate getDate() { return date; }
        public int getSourceID() { return sourceID; }
    }

    public static class SparseTemporalFieldValue {
        public static Schema<SparseTemporalFieldValue> SCHEMA;

        static {
            final Schema<Pair<LocalDate, Integer>> SCHEMA0 = Schema.zip(Schema.nullable(LocalDateSchema.INSTANCE), Schema.nullable(IntegerSchema.INSTANCE));
            SCHEMA = Schema.zipWith(SCHEMA0, (SparseTemporalFieldValue value) -> new Pair<>(value.getToDate(), value.getToSourceID()),
                                    new Latin1StringSchema(64), SparseTemporalFieldValue::getValue,
                                    (Pair<LocalDate, Integer> pair, String value) -> new SparseTemporalFieldValue(pair.k, pair.v, value));
        }

        private final LocalDate toDate;
        private final Integer toSourceID;
        private final String value;

        public SparseTemporalFieldValue(LocalDate toDate, Integer toSourceID, String value) {
            this.toDate = toDate;
            this.toSourceID = toSourceID;
            this.value = value;
        }

        public LocalDate getToDate() { return toDate; }
        public Integer getToSourceID() { return toSourceID; }
        public String getValue() { return value; }

        public SparseTemporalFieldValue setToDate(LocalDate toDate) { return new SparseTemporalFieldValue(toDate, toSourceID, value); }
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

    private static <T> T putSparseSourceTemporalCursor(Cursor<SourceTemporalFieldKey, SparseSourceTemporalFieldValue<T>> cursor, int sourceID, String idBBGlobal, T value) {
        // FIXME: deal with the case where value is null.. could overload it to mean "delete"?

        final boolean mustCreate;
        final SourceTemporalFieldKey key = new SourceTemporalFieldKey(idBBGlobal, sourceID);
        if (cursor.moveFloor(key)) {
            final SourceTemporalFieldKey existingKey = cursor.getKey();
            if (existingKey.getIDBBGlobal().equals(idBBGlobal)) {
                final SparseSourceTemporalFieldValue<T> existingValue = cursor.getValue();
                if (existingValue.getToSourceID() == null || existingValue.getToSourceID() > sourceID) {
                    // There is an existing entry in the DB covering the product and source that we are trying to insert
                    if (existingValue.getValue().equals(value)) {
                        mustCreate = false;
                    } else {
                        if (sourceID > existingKey.getSourceID()) {
                            cursor.put(new SparseSourceTemporalFieldValue<T>(sourceID, value));
                        } else {
                            cursor.delete();
                        }
                        // FIXME: split existing range. NB: careful with to source == null
                        if ()
                    }
                } else {
                    mustCreate = true;
                }
            } else {
                mustCreate = true;
            }
        } else {
            mustCreate = true;
        }

        if (mustCreate) {
            cursor.put(key, new SparseSourceTemporalFieldValue<T>(null, value));
        }
    }

    public static void load(Database db, Transaction tx, LocalDate date, String delivery, ZipInputStream zis) throws IOException {
        final CSVReader reader = new CSVReader(new InputStreamReader(zis), '|');
        String[] headers = reader.readNext();
        if (headers == null || headers.length == 1) {
            // Empty file
            return;
        }

        final Cursor<Integer, Source> sourceCursor = db.createIndex(tx, "Source", IntegerSchema.INSTANCE, Source.SCHEMA).createCursor(tx);
        final int sourceID = sourceCursor.moveLast() ? sourceCursor.getKey() + 1 : 0;
        sourceCursor.put(sourceID, new Source(delivery, date));

        // FIXME: handle updating of these
        final Cursor<SourceTemporalFieldKey, String>    lastKnownDeliveryCursor = db.createIndex(tx, "LastKnownDelivery", SourceTemporalFieldKey.SCHEMA, new Latin1StringSchema(10)).createCursor(tx);
        final Cursor<SourceTemporalFieldKey, LocalDate> lastKnownDateCursor     = db.createIndex(tx, "LastKnownDate",     SourceTemporalFieldKey.SCHEMA, LocalDateSchema.INSTANCE  ).createCursor(tx);

        int idBBGlobalIx = -1;
        final Cursor<TemporalFieldKey, SparseTemporalFieldValue>[] cursors = (Cursor<TemporalFieldKey, SparseTemporalFieldValue>[]) new Cursor[headers.length];
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equals("ID_BB_GLOBAL")) {
                idBBGlobalIx = i;
            } else {
                final String indexName = headers[i].replace(" ", "");
                final Index<TemporalFieldKey, SparseTemporalFieldValue> index = db.createIndex(tx, indexName, TemporalFieldKey.SCHEMA, SparseTemporalFieldValue.SCHEMA);
                cursors[i] = index.createCursor(tx);
            }
        }

        if (idBBGlobalIx < 0) {
            throw new IllegalArgumentException("No ID_BB_GLOBAL field");
        }

        int items = 0;
        long startTime = System.nanoTime();

        final HashSet<String> seenIdBBGlobals = new HashSet<>();
        String[] line;
        while ((line = reader.readNext()) != null) {
            if (line.length < headers.length) {
                continue;
            }

            final String idBBGlobal = line[idBBGlobalIx];
            seenIdBBGlobals.add(idBBGlobal);
            final LocalDate lastKnownDate = ??;

            final TemporalFieldKey fieldKey = new TemporalFieldKey(idBBGlobal, date, sourceID);
            for (int i = 0; i < headers.length; i++) {
                if (i == idBBGlobalIx) continue;

                final Cursor<TemporalFieldKey, SparseTemporalFieldValue> cursor = cursors[i];
                final String value = line[i].trim();

                items++;

                if (cursor.moveFloor(fieldKey)) {
                    boolean found = false;
                    TemporalFieldKey existingFieldKey;
                    SparseTemporalFieldValue existingFieldValue = null;
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
                        if (existingFieldValue.getValue().equals(value)) {
                            continue;
                        }

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
                        //  a) TODO (only have a weak version comparing to ToDate right now): Any tuple may be omitted if FromDate >= min(ToDate, max(LKBD( [FromSource, ISNULL(ToSource, S)) ))
                        //  b) TODO: The D+1 ToDate for 3. may be set to null if D+1 is > max(LKBD( [FromSource, S) ))
                        //  c) The D+1 ToDate for 4. may be set to null if D+1 is > the LKBD for S
                        if (existingFieldValue.getToDate() == null || date.isBefore(existingFieldValue.getToDate())) {
                            cursor.put(new SparseTemporalFieldValue(date, null, existingFieldValue.getValue()));
                        } else {
                            cursor.delete();
                        }
                        if (existingFieldValue.getToDate() == null || date.plusDays(1).isBefore(existingFieldValue.getToDate())) {
                            cursor.put(new TemporalFieldKey(idBBGlobal, date.plusDays(1), existingFieldKey.getSourceID()),
                                       existingFieldValue);
                        }
                        cursor.put(new TemporalFieldKey(idBBGlobal, date, existingFieldKey.getSourceID()),
                                   new SparseTemporalFieldValue(date.plusDays(1), sourceID, existingFieldValue.getValue()));
                    }
                }

                if (value.length() != 0) {
                    cursor.put(new TemporalFieldKey(idBBGlobal, date, sourceID),
                               new SparseTemporalFieldValue(date.isBefore(lastKnownDate) ? date.plusDays(1) : null, null, value));
                }
            }
        }

        for (String idBBGlobal : ??) {
            if (seenIdBBGlobals.contains(idBBGlobal)) continue;

            FIXME // Kill not-found product
        }

        long duration = System.nanoTime() - startTime;
        System.out.println("Loaded " + items + " in " + duration + "ns (" + (duration / items) + "ns/item)");
    }
}
