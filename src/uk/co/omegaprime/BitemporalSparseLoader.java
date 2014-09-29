package uk.co.omegaprime;

import au.com.bytecode.opencsv.CSVReader;
import uk.co.omegaprime.thunder.*;
import uk.co.omegaprime.thunder.schema.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.*;

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

        // NB: this only works properly if the current source ID is the maximum one
        public boolean delete() {
            if (positioned) {
                truncateExistingSubcursorRange(getValue());
                positioned = false;
                return true;
            } else {
                return false;
            }
        }

        // NB: this only works properly if the current source ID is the maximum one
        public V put(V value) {
            final V existingValue;
            if (positioned) {
                existingValue = getValue();
                if (Objects.equals(existingValue, value)) {
                    return value;
                }
                truncateExistingSubcursorRange(existingValue);
            } else {
                existingValue = null;
            }

            subcursor.put(currentSourceID, new SparseSourceTemporalFieldValue<>(null, value));
            return existingValue;
        }

        private void truncateExistingSubcursorRange(V existingValue) {
            if (subcursor.getKey() == currentSourceID) {
                subcursor.delete();
            } else {
                subcursor.put(new SparseSourceTemporalFieldValue<V>(currentSourceID, existingValue));
            }
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
            if (toSourceID != null && toDate == null) {
                // In fact, the invariant is more complex than this:
                //  1. If toSourceID == null, then we expect that toDate == null || !toDate.isAfter(lastKnownDate(maxSourceID))
                //  2. If toSourceID != null, then we expect that toDate != null && !toDate.isAfter(lastKnownDate(toSourceID-1).plusDays(1))
                //
                // Condition 1 is necessary for two reasons:
                //  a) It makes semantic sense: we don't know anything about the future past lastKnownDate, so it doesn't make sense
                //     to say that this row definitely terminates in the future
                //  b) It makes the denotation of a sparse source temporal row simpler, becaues it means that a row sets
                //     value V on the last known business date then if we just pad forward then V will certainly be padded
                //     forward to LKBD+1. If we allow toDate >= LKBD when toSourceID == null then padding forward
                //     by incrementing the LKBD and doing nothing else would *not* pad this data item if e.g. toDate == LKBD+1.
                //
                // Condition 2 is not strictly necessary (i.e. we could simply insist that toDate == null || !toDate.isAfter(lastKnownDate(toSourceID-1))
                // in this case) but we insist on this stronger invariant because it means that in order to interpret the DB we need only know
                // the LKBD of each ID BB global as of the *maximum source* because the only missing toDates will be those associated with rows
                // having toSourceID == null. This makes code a little bit simpler and more efficient in a few places.
                throw new IllegalArgumentException("In order to make interpreting rows in the database simpler and faster " +
                                                   "we maintain the invariant that toDate is only null if toSourceID is null.");
            }

            this.toDate = toDate;
            this.toSourceID = toSourceID;
            this.value = value;
        }

        public LocalDate getToDate() { return toDate; }
        public Integer getToSourceID() { return toSourceID; } // Invariant: must not be greater than the maximum source ID
        public V getValue() { return value; }

        // NB: relies on the assumption that LKD is a monotonically increasing function of sourceID
        //
        // This is an estimate in that it may return a maximum that is higher than the true value: it is guaranteed
        // to never return one lower than the true value. This can *only* happen when toSourceID < the maximum source ID,
        // in which case our maxAchievableLKD may be higher than what is achievable in reality since we only work with the
        // penultimateSourceLKD here rather than the full mapping from source ID to LKD.
        //
        // In the case that toSourceID is null this returns a precise answer.
        public LocalDate estimateMaximumToDate(LocalDate penultimateSourceLKD, LocalDate ultimateSourceLKD) {
            if (ultimateSourceLKD == null) throw new IllegalArgumentException("ultimateSourceLKD argument must not be null, though penultimateSourceLKD may be");

            final LocalDate maxAchievableLKD = (toSourceID == null || penultimateSourceLKD == null) ? ultimateSourceLKD : penultimateSourceLKD;
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
                return (toDate == null ? lastKnownDate.plusDays(1) : toDate).isAfter(date);
            }
        }

        // NB: this only works if currentSourceID is the maximum one
        public void put(LocalDate date, V value) {
            putDelete(date, Maybe.of(value));
        }

        // NB: this only works if currentSourceID is the maximum one
        public void delete(LocalDate date) {
            putDelete(date, Maybe.empty());
        }

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
                    if (existingFieldValue.getToSourceID() == null &&
                        (existingFieldValue.getToDate() == null ? lastKnownDate.plusDays(1) : existingFieldValue.getToDate()).isAfter(date)) {
                        found = true;
                        break;
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
                    //  b) Tuple 3 may be omitted if S_f >= S
                    //  c) The D+1 ToDate for tuple 4 may be set to null if D+1 is > the LKBD for S
                    //
                    // NB: tuples 2 and 3 might create tuples that are useful for only part of their source range. For example
                    // if the last known date as of S_f is less than D then both tuple 2 and 3 will start after the last known date
                    // and so encode absolutely no information for S_f (and possibly some successor sources as well). But that's fine!
                    {
                        // Tuple 1. NB: proposedValue.toSourceID is null so estimateMaximumToDate returns an exact value
                        final SparseTemporalFieldValue<V> proposedValue = new SparseTemporalFieldValue<>(date, null, existingFieldValue.getValue());
                        if (existingFieldKey.k.isBefore(proposedValue.estimateMaximumToDate(priorLastKnownDate, lastKnownDate))) {
                            subcursor.put(proposedValue);
                        } else {
                            subcursor.delete();
                        }
                    }
                    // Tuple 2. NB: existingFieldValue.toSourceID is null so estimateMaximumToDate returns an exact value
                    if (date.plusDays(1).isBefore(existingFieldValue.estimateMaximumToDate(priorLastKnownDate, lastKnownDate))) {
                        subcursor.put(new Pair<>(date.plusDays(1), existingFieldKey.v),
                                      existingFieldValue);
                    }
                    // Tuple 3
                    {
                        final LocalDate proposedToDate = minDate(priorLastKnownDate, date).plusDays(1);
                        if (existingFieldKey.v < currentSourceID && date.isBefore(proposedToDate)) {
                            subcursor.put(new Pair<>(date, existingFieldKey.v),
                                          new SparseTemporalFieldValue<>(proposedToDate, currentSourceID, existingFieldValue.getValue()));
                        }
                    }
                }
            }

            // Tuple 4
            if (value.isPresent()) {
                final LocalDate proposedToDate = date.isBefore(lastKnownDate) ? date.plusDays(1) : null;
                subcursor.put(new Pair<>(date, currentSourceID),
                              new SparseTemporalFieldValue<>(proposedToDate, null, value.get()));
            }
        }
    }

    private static class LKDCursors implements AutoCloseable {
        final SparseSourceTemporalCursor<String>    lastKnownDeliveryCursor;
        final SparseSourceTemporalCursor<LocalDate> itemLastKnownDateCursor;
        final SparseSourceTemporalCursor<LocalDate> deliveryLastKnownDateCursor;

        public LKDCursors(Database db, Transaction tx, int sourceID) {
            this.lastKnownDeliveryCursor     = new SparseSourceTemporalCursor<>(db.createIndex(tx, "LastKnownDelivery",     SourceTemporalFieldKey.SCHEMA, SparseSourceTemporalFieldValue.schema(new Latin1StringSchema(10))).createCursor(tx), sourceID);
            this.itemLastKnownDateCursor     = new SparseSourceTemporalCursor<>(db.createIndex(tx, "ItemLastKnownDate",     SourceTemporalFieldKey.SCHEMA, SparseSourceTemporalFieldValue.schema(LocalDateSchema.INSTANCE)  ).createCursor(tx), sourceID);
            this.deliveryLastKnownDateCursor = new SparseSourceTemporalCursor<>(db.createIndex(tx, "DeliveryLastKnownDate", SourceTemporalFieldKey.SCHEMA, SparseSourceTemporalFieldValue.schema(LocalDateSchema.INSTANCE)  ).createCursor(tx), sourceID); // Actually keyed by delivery, not ID_BB_GLOBAL
        }

        public LocalDate lastKnownDate(String idBBGlobal) {
            return maxDate(itemLastKnownDateCursor.moveTo(idBBGlobal) ? itemLastKnownDateCursor.getValue() : null,
                           lastKnownDeliveryCursor.moveTo(idBBGlobal) && deliveryLastKnownDateCursor.moveTo(lastKnownDeliveryCursor.getValue()) ? deliveryLastKnownDateCursor.getValue() : null);
        }

        public void killAllProductsInDeliveryExcept(String delivery, LocalDate deliveryPriorLastKnownDate, HashSet<String> seenIdBBGlobals) {
            if (lastKnownDeliveryCursor.moveFirst()) {
                do {
                    final String idBBGlobal = lastKnownDeliveryCursor.getKey();
                    lastKnownDeliveryCursor.moveTo(idBBGlobal); // TODO: yuck
                    if (!lastKnownDeliveryCursor.getValue().equals(delivery) || seenIdBBGlobals.contains(idBBGlobal)) continue;

                    final LocalDate itemLKD = itemLastKnownDateCursor.moveTo(idBBGlobal) ? itemLastKnownDateCursor.getValue() : null;
                    lastKnownDeliveryCursor.delete();
                    itemLastKnownDateCursor.put(maxDate(deliveryPriorLastKnownDate, itemLKD));
                } while (lastKnownDeliveryCursor.moveNext());
            }
        }

        public void close() {
            lastKnownDeliveryCursor.cursor.close();
            itemLastKnownDateCursor.cursor.close();
            deliveryLastKnownDateCursor.cursor.close();
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

    private static LocalDate maxDate(LocalDate a, LocalDate b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.isAfter(b) ? a : b;
    }

    private static LocalDate minDate(LocalDate a, LocalDate b) {
        if (a == null) return b;
        if (b == null) return a;
        return a.isAfter(b) ? b : a;
    }

    public static int load(Database db, Transaction tx, LocalDate date, String delivery, boolean deliveryIsExhaustive, InputStream is) throws IOException {
        final CSVReader reader = new CSVReader(new InputStreamReader(is), '|');
        String[] headers = reader.readNext();
        if (headers == null || headers.length == 1) {
            throw new IllegalStateException("Empty file");
        }

        try (final Cursor<Integer, Source> sourceCursor = createSourcesCursor(db, tx);
             final Cursor<String, Void> fieldsCursor = createFieldsCursor(db, tx)) {
            final int sourceID = sourceCursor.moveLast() ? sourceCursor.getKey() + 1 : 0;
            sourceCursor.put(sourceID, new Source(delivery, date));

            try (final LKDCursors lkdCursors = new LKDCursors(db, tx, sourceID)) {

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

                lkdCursors.deliveryLastKnownDateCursor.moveTo(delivery);
                final LocalDate deliveryPriorLastKnownDate = lkdCursors.deliveryLastKnownDateCursor.put(date);

                int idBBGlobalIx = -1;
                final SparseTemporalCursor<String>[] cursors = (SparseTemporalCursor<String>[]) new SparseTemporalCursor[headers.length];
                for (int i = 0; i < headers.length; i++) {
                    if (headers[i].equals("ID_BB_GLOBAL")) {
                        idBBGlobalIx = i;
                    } else {
                        final String indexName = headers[i].replace(" ", "");
                        if (!fieldsCursor.moveTo(indexName)) fieldsCursor.put(indexName, null);
                        cursors[i] = new SparseTemporalCursor<>(createFieldCursor(db, tx, indexName), sourceID);
                    }
                }

                if (idBBGlobalIx < 0) {
                    throw new IllegalArgumentException("No ID_BB_GLOBAL field");
                }

                int items = 0;
                long startTime = System.nanoTime();

                final HashSet<String> seenIdBBGlobals = deliveryIsExhaustive ? new HashSet<>() : null;
                String[] line;
                while ((line = reader.readNext()) != null) {
                    if (line.length < headers.length) {
                        continue;
                    }

                    final String idBBGlobal = line[idBBGlobalIx];
                    if (seenIdBBGlobals != null) seenIdBBGlobals.add(idBBGlobal);

                    final LocalDate itemLastKnownDate = lkdCursors.itemLastKnownDateCursor.moveTo(idBBGlobal) ? lkdCursors.itemLastKnownDateCursor.getValue() : null;

                    lkdCursors.lastKnownDeliveryCursor.moveTo(idBBGlobal);
                    final String oldDelivery = lkdCursors.lastKnownDeliveryCursor.put(delivery);
                    final LocalDate oldDeliveryPriorLastKnownDate
                        = delivery.equals(oldDelivery)                                                        ? deliveryPriorLastKnownDate
                        : (oldDelivery != null && lkdCursors.deliveryLastKnownDateCursor.moveTo(oldDelivery)) ? lkdCursors.deliveryLastKnownDateCursor.getValue()
                        : null;
                    final LocalDate priorLastKnownDate = maxDate(oldDeliveryPriorLastKnownDate, itemLastKnownDate);
                    final LocalDate lastKnownDate      = maxDate(priorLastKnownDate, date);
                    if (oldDelivery != null && !delivery.equals(oldDelivery) && priorLastKnownDate != null) {
                        lkdCursors.itemLastKnownDateCursor.put(priorLastKnownDate);
                    } else if (oldDelivery == null && itemLastKnownDate != null) {
                        // We have just resurrected a dead item. This leads to a problem. For any field for this quote
                        // we may already have a (FromSourceID, ToSourceID=NULL, FromDate, ToDate=NULL) tuple in the DB.
                        // By the act of bumping the quote into this delivery we have changed the implicit ToSourceID
                        // on all of these rows, but we (probably) don't really want to pad all those old values -- because
                        // the quote was deleted for that time period we should just record missing values for those dates.
                        //
                        // To avoid this we go back and rewrite these tuples to (FromSourceID, ToSourceID=NULL, FromDate, ToDate=priorLastKnownDate+1)
                        if (fieldsCursor.moveFirst()) {
                            do {
                                final String field = fieldsCursor.getKey();
                                try (final Cursor<TemporalFieldKey, SparseTemporalFieldValue<String>> fieldCursor = createFieldCursor(db, tx, field)) {
                                    final SubcursorView<String, Pair<LocalDate, Integer>, SparseTemporalFieldValue<String>> cursor =  new SubcursorView<>(fieldCursor, ID_BB_GLOBAL_SCHEMA, Schema.zip(LocalDateSchema.INSTANCE, SOURCE_ID_SCHEMA));
                                    cursor.setPosition(idBBGlobal);
                                    if (cursor.moveFirst()) {
                                        do {
                                            final SparseTemporalFieldValue<String> fieldValue = cursor.getValue();
                                            if (fieldValue.getToSourceID() == null && fieldValue.getToDate() == null) {
                                                cursor.put(new SparseTemporalFieldValue<>(priorLastKnownDate.plusDays(1), null, fieldValue.getValue()));
                                            }
                                        } while (cursor.moveNext());
                                    }
                                }
                            } while (fieldsCursor.moveNext());
                        }
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

                if (seenIdBBGlobals != null) {
                    // Kill off any product that we expected to be in this delivery but was not
                    lkdCursors.killAllProductsInDeliveryExcept(delivery, deliveryPriorLastKnownDate, seenIdBBGlobals);
                }

                long duration = System.nanoTime() - startTime;
                System.out.println("Loaded " + items + " in " + duration + "ns (" + (duration / items) + "ns/item) to source ID " + sourceID);
            }

            return sourceID;
        }
    }

    private static Cursor<TemporalFieldKey, SparseTemporalFieldValue<String>> createFieldCursor(Database db, Transaction tx, String indexName) {
        return db.createIndex(tx, indexName, TemporalFieldKey.SCHEMA, SparseTemporalFieldValue.schema(new Latin1StringSchema(64))).createCursor(tx);
    }

    private static Cursor<String, Void> createFieldsCursor(Database db, Transaction tx) {
        return db.createIndex(tx, "Fields", Latin1StringSchema.INSTANCE, VoidSchema.INSTANCE).createCursor(tx);
    }

    private static Cursor<Integer, Source> createSourcesCursor(Database db, Transaction tx) {
        return db.createIndex(tx, "Source", IntegerSchema.INSTANCE, Source.SCHEMA).createCursor(tx);
    }

    public static Map<String, Map<String, SortedMap<LocalDate, String>>> currentSourceToJava(Database db, Transaction tx) {
        return sourceToJava(db, tx, null);
    }

    public static Map<String, Map<String, SortedMap<LocalDate, String>>> sourceToJava(Database db, Transaction tx, Integer sourceID) {
        final HashMap<String, Map<String, SortedMap<LocalDate, String>>> valuesByField = new HashMap<>();

        try (final Cursor<Integer, Source> sourcesCursor = createSourcesCursor(db, tx);
             final Cursor<String, Void> fieldsCursor = createFieldsCursor(db, tx)) {
            final int maxSourceId = sourcesCursor.moveLast() ? sourcesCursor.getKey() : -1;
            int currentSourceId = sourceID == null ? maxSourceId : sourceID;
            try (final LKDCursors lkdCursors = new LKDCursors(db, tx, currentSourceId)) {
                if (fieldsCursor.moveFirst()) {
                    do {
                        final String field = fieldsCursor.getKey();
                        final Map<String, SortedMap<LocalDate, String>> valuesByIDBBGlobal = new HashMap<>();
                        valuesByField.put(field, valuesByIDBBGlobal);

                        try (final Cursor<TemporalFieldKey, SparseTemporalFieldValue<String>> fieldCursor = createFieldCursor(db, tx, field)) {
                            final FilteredView<TemporalFieldKey, SparseTemporalFieldValue<String>> nowFieldCursor = new FilteredView<>(fieldCursor, (TemporalFieldKey k, SparseTemporalFieldValue<String> v) -> k.getSourceID() <= currentSourceId && (v.getToSourceID() == null ? maxSourceId + 1 : v.getToSourceID()) > currentSourceId);
                            if (nowFieldCursor.moveFirst()) {
                                do {
                                    // FIXME: exploit the fact that we are grouped by id bb unique to reduce LKD lookups
                                    // (could even merge join with the underlying per-item LKD tables)
                                    final TemporalFieldKey fieldKey = nowFieldCursor.getKey();
                                    final String idBBGlobal = fieldKey.getIDBBGlobal();
                                    SortedMap<LocalDate, String> valuesByDate = valuesByIDBBGlobal.get(idBBGlobal);
                                    if (valuesByDate == null) {
                                        valuesByDate = new TreeMap<>();
                                        valuesByIDBBGlobal.put(idBBGlobal, valuesByDate);
                                    }

                                    final LocalDate lkd = lkdCursors.lastKnownDate(idBBGlobal);
                                    final SparseTemporalFieldValue<String> fieldValue = nowFieldCursor.getValue();
                                    final LocalDate padToDate = minDate(fieldValue.getToDate(), lkd.plusDays(1));

                                    LocalDate date = fieldKey.getDate();
                                    while (date.isBefore(padToDate)) {
                                        if (valuesByDate.put(date, fieldValue.getValue()) != null) {
                                            throw new IllegalStateException("Integrity check failure: date " + date + " was covered twice in the DB for " + field + " and " + idBBGlobal);
                                        }
                                        date = date.plusDays(1);
                                    }
                                } while (nowFieldCursor.moveNext());
                            }
                        }
                    } while (fieldsCursor.moveNext());
                }

                return valuesByField;
            }
        }
    }
}
