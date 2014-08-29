package uk.co.omegaprime;

import au.com.bytecode.opencsv.CSVReader;
import uk.co.omegaprime.thunder.*;
import uk.co.omegaprime.thunder.schema.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Objects;
import java.util.zip.ZipInputStream;

public class BitemporalSparseLoader {
    private static final Schema<String> ID_BB_GLOBAL_SCHEMA = new Latin1StringSchema(20);
    private static final Schema<Integer> SOURCE_ID_SCHEMA = IntegerSchema.INSTANCE;

    public static class SourceTemporalFieldKey {
        public static Schema<SourceTemporalFieldKey> SCHEMA = Schema.zipWith(ID_BB_GLOBAL_SCHEMA, SourceTemporalFieldKey::getIDBBGlobal,
                                                                             SOURCE_ID_SCHEMA,    SourceTemporalFieldKey::getSourceID,
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

    public static class SparseSourceTemporalCursor<V> {
        private final Cursor<SourceTemporalFieldKey, SparseSourceTemporalFieldValue<V>> cursor;
        private final SubcursorView<String, Integer, SparseSourceTemporalFieldValue<V>> subcursor;
        private final int currentSourceID;

        private boolean positioned;

        public SparseSourceTemporalCursor(Cursor<SourceTemporalFieldKey, SparseSourceTemporalFieldValue<V>> cursor, int currentSourceID) {
            this.cursor = cursor;
            this.subcursor = new SubcursorView<>(cursor, ID_BB_GLOBAL_SCHEMA, SOURCE_ID_SCHEMA);
            this.currentSourceID = currentSourceID;
        }

        public boolean moveTo(String idBBGlobal) {
            positioned = false;
            subcursor.reposition(idBBGlobal);

            if (subcursor.moveFloor(currentSourceID)) {
                final SparseSourceTemporalFieldValue<V> value = subcursor.getValue();
                if (value.getToSourceID() == null || value.getToSourceID() > currentSourceID) {
                    positioned = true;
                }
            }

            return positioned;
        }

        public String getKey() {
            return cursor.getKey().getIDBBGlobal();
        }

        public V getValue() {
            if (!positioned) throw new IllegalStateException("May not getValue() if we aren't positioned");
            return subcursor.getValue().getValue();
        }

        public boolean delete() {
            if (positioned) {
                subcursor.put(new SparseSourceTemporalFieldValue<V>(currentSourceID, getValue()));
                positioned = false;
                return true;
            } else {
                return false;
            }
        }

        public V put(V value) {
            final V existingValue;
            if (positioned) {
                existingValue = getValue();
                if (Objects.equals(existingValue, value)) {
                    return value;
                }
                subcursor.put(new SparseSourceTemporalFieldValue<V>(currentSourceID, existingValue));
            } else {
                existingValue = null;
            }

            subcursor.put(currentSourceID, new SparseSourceTemporalFieldValue<>(null, value));
            return existingValue;
        }

        private boolean isAlive() {
            SparseSourceTemporalFieldValue<V> temporalValue = cursor.getValue();
            return (cursor.getKey().getSourceID() <= currentSourceID && (temporalValue.getToSourceID() == null || temporalValue.getToSourceID() > currentSourceID));
        }

        private boolean scanForwardForAlive() {
            do {
                if (isAlive()) {
                    // TODO: this causes us to seek "cursor" again, needlessly. Make more efficient?
                    subcursor.reposition(cursor.getKey().getIDBBGlobal());
                    return true;
                }
            } while (cursor.moveNext());

            return false;
        }

        public boolean moveFirst() { return cursor.moveFirst() && scanForwardForAlive(); }
        public boolean moveNext()  { return cursor.moveNext()  && scanForwardForAlive(); }
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

    public static class SparseTemporalFieldValue<V> {
        public static <T> Schema<SparseTemporalFieldValue<T>> schema(Schema<T> tSchema) {
            final Schema<Pair<LocalDate, Integer>> SCHEMA0 = Schema.<LocalDate, Integer>zip(Schema.nullable(LocalDateSchema.INSTANCE), Schema.nullable(IntegerSchema.INSTANCE));
            return Schema.zipWith(SCHEMA0, (SparseTemporalFieldValue<T> value) -> new Pair<>(value.getToDate(), value.getToSourceID()),
                                  tSchema, SparseTemporalFieldValue::getValue,
                                  (Pair<LocalDate, Integer> pair, T value) -> new SparseTemporalFieldValue<>(pair.k, pair.v, value));
        }

        private final LocalDate toDate;
        private final Integer toSourceID;
        private final V value;

        public SparseTemporalFieldValue(LocalDate toDate, Integer toSourceID, V value) {
            this.toDate = toDate;
            this.toSourceID = toSourceID;
            this.value = value;
        }

        public LocalDate getToDate() { return toDate; }
        public Integer getToSourceID() { return toSourceID; }
        public V getValue() { return value; }

        public SparseTemporalFieldValue<V> setToDate(LocalDate toDate) { return new SparseTemporalFieldValue<>(toDate, toSourceID, value); }
    }

    public static class SparseTemporalCursor<V> {
        private final Cursor<TemporalFieldKey, SparseTemporalFieldValue<V>> cursor;
        private final int currentSourceID;
        private final SubcursorView<String, Pair<LocalDate, Integer>, SparseTemporalFieldValue<V>> subcursor;
        private final SubcursorView<Pair<String, LocalDate>, Integer, SparseTemporalFieldValue<V>> dateSubcursor;

        public SparseTemporalCursor(Cursor<TemporalFieldKey, SparseTemporalFieldValue<V>> cursor, int currentSourceID) {
            this.cursor = cursor;
            this.currentSourceID = currentSourceID;
            this.subcursor     = new SubcursorView<>(cursor, ID_BB_GLOBAL_SCHEMA, Schema.zip(LocalDateSchema.INSTANCE, SOURCE_ID_SCHEMA));
            this.dateSubcursor = new SubcursorView<>(cursor, Schema.zip(ID_BB_GLOBAL_SCHEMA, LocalDateSchema.INSTANCE), SOURCE_ID_SCHEMA);
        }
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

    private static <T> T putSparseSourceTemporalCursor(Cursor<SourceTemporalFieldKey, SparseSourceTemporalFieldValue<T>> cursor, int maxSourceID, int sourceID, String idBBGlobal, T value) {
        // FIXME: deal with the case where value is null.. could overload it to mean "delete"?

        final SourceTemporalFieldKey key = new SourceTemporalFieldKey(idBBGlobal, sourceID);
        if (cursor.moveFloor(key)) {
            final SourceTemporalFieldKey existingKey = cursor.getKey();
            if (existingKey.getIDBBGlobal().equals(idBBGlobal)) {
                final SparseSourceTemporalFieldValue<T> existingValue = cursor.getValue();
                int effectiveSourceTo = existingValue.getToSourceID() == null ? maxSourceID+1 : existingValue.getToSourceID();
                if (effectiveSourceTo > sourceID) {
                    // There is an existing entry in the DB covering the product and source that we are trying to insert
                    if (!existingValue.getValue().equals(value)) {
                        // TODO: any way of optimising insertion after current subcursor location?
                        if (sourceID > existingKey.getSourceID()) {
                            cursor.put(new SparseSourceTemporalFieldValue<T>(sourceID, existingValue.getValue()));
                        } else {
                            cursor.delete();
                        }
                        cursor.put(key, new SparseSourceTemporalFieldValue<T>(sourceID == maxSourceID ? null : sourceID+1, value));
                        if (effectiveSourceTo > sourceID + 1) {
                            cursor.put(new SourceTemporalFieldKey(idBBGlobal, sourceID + 1), existingValue);
                        }
                    }
                    return existingValue.getValue();
                }
            }
        }

        cursor.put(key, new SparseSourceTemporalFieldValue<T>(null, value));
        return null;
    }

    private static LocalDate maxDate(LocalDate a, LocalDate b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.isAfter(b) ? a : b;
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
        final SparseSourceTemporalCursor<String>    lastKnownDeliveryCursor     = new SparseSourceTemporalCursor<>(db.createIndex(tx, "LastKnownDelivery",     SourceTemporalFieldKey.SCHEMA, SparseSourceTemporalFieldValue.schema(new Latin1StringSchema(10))).createCursor(tx), sourceID);
        final SparseSourceTemporalCursor<LocalDate> itemLastKnownDateCursor     = new SparseSourceTemporalCursor<>(db.createIndex(tx, "ItemLastKnownDate",     SourceTemporalFieldKey.SCHEMA, SparseSourceTemporalFieldValue.schema(LocalDateSchema.INSTANCE)  ).createCursor(tx), sourceID);
        final SparseSourceTemporalCursor<LocalDate> deliveryLastKnownDateCursor = new SparseSourceTemporalCursor<>(db.createIndex(tx, "DeliveryLastKnownDate", SourceTemporalFieldKey.SCHEMA, SparseSourceTemporalFieldValue.schema(LocalDateSchema.INSTANCE)  ).createCursor(tx), sourceID); // Actually keyed by delivery, not ID_BB_GLOBAL

        // Given:
        //  * idBBGlobal
        //  * sourceID
        // The last known date of the item is the max of:
        //  * itemLastKnownDateCursor(sourceID, idBBGlobal)
        //  * deliveryLastKnownDateCursor(sourceID, lastKnownDeliveryCursor(sourceID, idBBGlobal))
        // The last known date monotonically increases as we add more sources to the chain, and
        // tells you the final date over which a static field with a missing end date should be valid.
        //
        // When we see a new source we update the corresponding deliveryLastKnownDateCursor, which implicitly
        // pads forward all statics for anything currently in that delivery. When we change the delivery in
        // which a idBBGlobal appears we update itemLastKnownDateCursor to record the LKD in the old delivery.
        // This is necessary because the new delivery may have a lower date than the one we are moving from
        // but we don't want to reduce the LKD for this idBBGlobal.

        deliveryLastKnownDateCursor.moveTo(delivery);
        final LocalDate deliveryOldLastKnownDate = deliveryLastKnownDateCursor.put(date);

        int idBBGlobalIx = -1;
        final Cursor<TemporalFieldKey, SparseTemporalFieldValue<String>>[] cursors = (Cursor<TemporalFieldKey, SparseTemporalFieldValue<String>>[]) new Cursor[headers.length];
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equals("ID_BB_GLOBAL")) {
                idBBGlobalIx = i;
            } else {
                final String indexName = headers[i].replace(" ", "");
                final Index<TemporalFieldKey, SparseTemporalFieldValue<String>> index = db.createIndex(tx, indexName, TemporalFieldKey.SCHEMA, SparseTemporalFieldValue.schema(new Latin1StringSchema(64)));
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

            final LocalDate itemLastKnownDate = itemLastKnownDateCursor.moveTo(idBBGlobal) ? itemLastKnownDateCursor.getValue() : null;

            lastKnownDeliveryCursor.moveTo(idBBGlobal);
            final String oldDelivery = lastKnownDeliveryCursor.put(delivery);
            final LocalDate oldDeliveryLastKnownDate = (oldDelivery != null && deliveryLastKnownDateCursor.moveTo(oldDelivery)) ? deliveryLastKnownDateCursor.getValue() : null;
            final LocalDate priorLastKnownDate       = maxDate(oldDeliveryLastKnownDate, itemLastKnownDate);
            final LocalDate lastKnownDate            = maxDate(priorLastKnownDate, date);
            if (oldDelivery != null && !delivery.equals(oldDelivery) && priorLastKnownDate != null) {
                itemLastKnownDateCursor.put(priorLastKnownDate);
            }

            final TemporalFieldKey fieldKey = new TemporalFieldKey(idBBGlobal, date, sourceID);
            for (int i = 0; i < headers.length; i++) {
                if (i == idBBGlobalIx) continue;

                final Cursor<TemporalFieldKey, SparseTemporalFieldValue<String>> cursor = cursors[i];
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

        /*
        if (lastKnownDeliveryCursor.moveFirst()) {
            do {
                if (lastKnownDeliveryCursor.getKey().getSourceID() <= sourceID) {
                    final SparseSourceTemporalFieldValue<String> lastKnownDelivery = lastKnownDeliveryCursor.getValue();
                    if (lastKnownDelivery.getToSourceID() == null || lastKnownDelivery.getToSourceID() > sourceID) {

                    }
                }
            } while (lastKnownDeliveryCursor.moveNext());
        }
        */

        // Kill off any product that we expected to be in this delivery but was not
        if (lastKnownDeliveryCursor.moveFirst()) {
            do {
                final String idBBGlobal = lastKnownDeliveryCursor.getKey();
                if (seenIdBBGlobals.contains(idBBGlobal)) continue;

                lastKnownDeliveryCursor.put(null);
                itemLastKnownDateCursor.moveTo(idBBGlobal);
                itemLastKnownDateCursor.put(deliveryOldLastKnownDate);
            } while (lastKnownDeliveryCursor.moveNext());
        }

        long duration = System.nanoTime() - startTime;
        System.out.println("Loaded " + items + " in " + duration + "ns (" + (duration / items) + "ns/item)");
    }
}
