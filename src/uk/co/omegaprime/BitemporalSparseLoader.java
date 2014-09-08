package uk.co.omegaprime;

import au.com.bytecode.opencsv.CSVReader;
import uk.co.omegaprime.thunder.*;
import uk.co.omegaprime.thunder.schema.*;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
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

    // TODO: the interface of this is inconsistent with SparseTemporalCursor. With STC you setPosition
    // but don't learn about whether you are positioned or not until you actually do an operation,
    // with this one you moveTo and then immediately learn whether you are positioned or not.
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
            subcursor.setPosition(idBBGlobal);

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
                    subcursor.setPosition(cursor.getKey().getIDBBGlobal());
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
        public Integer getToSourceID() { return toSourceID; } // Invariant: must not be greater than the maximum source ID
        public V getValue() { return value; }

        public SparseTemporalFieldValue<V> setToDate(LocalDate toDate) { return new SparseTemporalFieldValue<>(toDate, toSourceID, value); }

        // NB: assumes that LKD is a monotonically increasing function of sourceID
        //
        // NB: this is an estimate in that it may return a maximum that is higher than the true value: it is guaranteed
        // to never return one lower than the true value. (This can happen when toSourceID < the maximum source ID, in
        // which case our maxAchievableLKD may be higher than what is achievable in reality since we only work with the
        // penultimateSourceLKD here rather than the full mapping from source ID to LKD.)
        public LocalDate estimateMaximumToDate(LocalDate penultimateSourceLKD, LocalDate maxSourceLKD) {
            if (maxSourceLKD == null) throw new IllegalArgumentException("maxSourceLKD argument must not be null, though penultimateSourceLKD may be");

            final LocalDate maxAchievableLKD = (toSourceID == null || penultimateSourceLKD == null) ? maxSourceLKD : penultimateSourceLKD;
            return (toDate == null || maxAchievableLKD.isBefore(toDate)) ? maxAchievableLKD : toDate;
        }
    }

    public static class SparseTemporalCursor<V> {
        private final Cursor<TemporalFieldKey, SparseTemporalFieldValue<V>> cursor;
        private final int currentSourceID;
        private final SubcursorView<String, Pair<LocalDate, Integer>, SparseTemporalFieldValue<V>> subcursor;
        private final Cursorlike<Pair<LocalDate, Integer>, SparseTemporalFieldValue<V>> subcursorForCurrentSourceID;

        private LocalDate lastKnownDate;
        private LocalDate priorLastKnownDate;

        public SparseTemporalCursor(Cursor<TemporalFieldKey, SparseTemporalFieldValue<V>> cursor, int currentSourceID) {
            this.cursor                      = cursor;
            this.currentSourceID             = currentSourceID;
            this.subcursor                   = new SubcursorView<>(cursor, ID_BB_GLOBAL_SCHEMA, Schema.zip(LocalDateSchema.INSTANCE, SOURCE_ID_SCHEMA));
            this.subcursorForCurrentSourceID = new FilteredView<>(subcursor, this::isValidAtCurrentSource);
        }

        private boolean isValidAtCurrentSource(Pair<LocalDate, Integer> key, SparseTemporalFieldValue<V> value) {
            return key.v <= currentSourceID && (value.toSourceID == null || value.toSourceID > currentSourceID);
        }

        public void setPosition(String idBBGlobal, LocalDate priorLastKnownDate, LocalDate lastKnownDate) {
            this.subcursor.setPosition(idBBGlobal);
            this.priorLastKnownDate = priorLastKnownDate;
            this.lastKnownDate = lastKnownDate;
        }

        public boolean moveTo(LocalDate date) {
            if (!this.subcursorForCurrentSourceID.moveFloor(new Pair<>(date, currentSourceID))) {
                return false;
            } else {
                final LocalDate toDate = this.subcursorForCurrentSourceID.getValue().getToDate();
                return toDate == null || toDate.isAfter(date);
            }
        }

        public void put(LocalDate date, V value) {
            putDelete(date, Maybe.of(value));
        }

        public void delete(LocalDate date) {
            putDelete(date, Maybe.empty());
        }

        // TODO: currently this assumes that the currentSourceID is the maximum one. Should document that this is the only supported mode. (get maybe could support more though?)
        private void putDelete(LocalDate date, Maybe<V> value) {
            if (subcursor.moveFloor(new Pair<>(date, currentSourceID))) {
                boolean found = false;
                Pair<LocalDate, Integer> existingFieldKey;
                SparseTemporalFieldValue<V> existingFieldValue = null;
                do {
                    existingFieldKey = subcursor.getKey();
                    if (subcursor.getKey().k.isAfter(date)) {
                        break;
                    }

                    existingFieldValue = cursor.getValue();
                    if (existingFieldValue.getToDate() == null || existingFieldValue.getToDate().isAfter(date)) {
                        if (existingFieldValue.getToSourceID() == null) {
                            found = true;
                            break;
                        }
                    }
                } while (subcursor.moveNext());

                if (found) {
                    if (value.isPresent() && existingFieldValue.getValue().equals(value.get())) {
                        return;
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
                    //  a) Any tuple may be omitted if FromDate >= min(ToDate, max(LKBD( [FromSource, ISNULL(ToSource, S)) ))
                    //  b) The D+1 ToDate for tuple 3 may be set to null if D+1 is > max(LKBD( [FromSource, S) ))
                    //  c) Tuple 3 may be omitted if S_f >= S
                    //  d) The D+1 ToDate for tuple 4 may be set to null if D+1 is > the LKBD for S
                    {
                        // Tuple 1
                        final SparseTemporalFieldValue<V> proposedValue = new SparseTemporalFieldValue<>(date, null, existingFieldValue.getValue());
                        if (existingFieldKey.k.isBefore(proposedValue.estimateMaximumToDate(priorLastKnownDate, lastKnownDate))) {
                            subcursor.put(proposedValue);
                        } else {
                            subcursor.delete();
                        }
                    }
                    // Tuple 2
                    if (date.plusDays(1).isBefore(existingFieldValue.estimateMaximumToDate(priorLastKnownDate, lastKnownDate))) {
                        subcursor.put(new Pair<>(date.plusDays(1), existingFieldKey.v),
                                      existingFieldValue);
                    }
                    // Tuple 3
                    if (existingFieldKey.v < currentSourceID) {
                        final LocalDate proposedToDate = date.isBefore(priorLastKnownDate) ? date.plusDays(1) : null;
                        subcursor.put(new Pair<>(date, existingFieldKey.v),
                                      new SparseTemporalFieldValue<V>(proposedToDate, currentSourceID, existingFieldValue.getValue()));
                    }
                }
            }

            // Tuple 4
            if (value.isPresent()) {
                final LocalDate proposedToDate = date.isBefore(lastKnownDate) ? date.plusDays(1) : null;
                subcursor.put(new Pair<>(date, currentSourceID),
                              new SparseTemporalFieldValue<V>(proposedToDate, null, value.get()));
            }
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
        final LocalDate deliveryPriorLastKnownDate = deliveryLastKnownDateCursor.put(date);

        int idBBGlobalIx = -1;
        final SparseTemporalCursor<String>[] cursors = (SparseTemporalCursor<String>[]) new SparseTemporalCursor[headers.length];
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equals("ID_BB_GLOBAL")) {
                idBBGlobalIx = i;
            } else {
                final String indexName = headers[i].replace(" ", "");
                final Index<TemporalFieldKey, SparseTemporalFieldValue<String>> index = db.createIndex(tx, indexName, TemporalFieldKey.SCHEMA, SparseTemporalFieldValue.schema(new Latin1StringSchema(64)));
                cursors[i] = new SparseTemporalCursor<>(index.createCursor(tx), sourceID);
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
            final LocalDate oldDeliveryPriorLastKnownDate
                = delivery.equals(oldDelivery)                                             ? deliveryPriorLastKnownDate
                : (oldDelivery != null && deliveryLastKnownDateCursor.moveTo(oldDelivery)) ? deliveryLastKnownDateCursor.getValue()
                : null;
            final LocalDate priorLastKnownDate = maxDate(oldDeliveryPriorLastKnownDate, itemLastKnownDate);
            final LocalDate lastKnownDate      = maxDate(priorLastKnownDate, date);
            if (oldDelivery != null && !delivery.equals(oldDelivery) && priorLastKnownDate != null) {
                itemLastKnownDateCursor.put(priorLastKnownDate);
            }

            for (int i = 0; i < headers.length; i++) {
                if (i == idBBGlobalIx) continue;

                final SparseTemporalCursor<String> cursor = cursors[i];
                final String value = line[i].trim();

                items++;

                cursor.setPosition(idBBGlobal, priorLastKnownDate, lastKnownDate);
                if (value.length() != 0) {
                    cursor.put(date, value);
                } else {
                    cursor.delete(date);
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
                itemLastKnownDateCursor.put(deliveryPriorLastKnownDate);
            } while (lastKnownDeliveryCursor.moveNext());
        }

        long duration = System.nanoTime() - startTime;
        System.out.println("Loaded " + items + " in " + duration + "ns (" + (duration / items) + "ns/item)");
    }
}
