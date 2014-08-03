package uk.co.omegaprime;

import org.junit.Test;

import java.io.File;
import java.time.LocalDate;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.co.omegaprime.Loader.*;
import static uk.co.omegaprime.Bits.*;

public class LoaderTest {
    static final File dbDirectory = new File("/Users/mbolingbroke/example.lmdb");

    private Database createDatabase() {
        if (dbDirectory.exists()) {
            for (File f : dbDirectory.listFiles()) {
                f.delete();
            }
            if (!dbDirectory.delete()) {
                throw new IllegalStateException("Failed to delete target directory " + dbDirectory);
            }
        }
        dbDirectory.mkdir();

        return openDatabase();
    }

    private Database openDatabase() {
        return new Database(dbDirectory, new DatabaseOptions().maxIndexes(40).mapSize(1_073_741_824));
    }

    @Test
    public void canCursorAroundPositiveFloatsAndNaNs() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<Float, String> index = db.createIndex(tx, "Test", FloatSchema.INSTANCE, StringSchema.INSTANCE);

                index.put(tx, 1.0f, "One");
                index.put(tx, 1234567890123.0f, "Biggish");
                index.put(tx, Float.NaN, "Not a number");
                index.put(tx, Float.POSITIVE_INFINITY, "Infinity");

                assertEquals("One", index.get(tx, 1.0f));
                assertEquals("Biggish", index.get(tx, 1234567890123.0f));
                assertEquals("Not a number", index.get(tx, Float.NaN));
                assertEquals("Infinity", index.get(tx, Float.POSITIVE_INFINITY));

                try (Cursor<Float, String> cursor = index.createCursor(tx)) {
                    assertTrue(cursor.moveCeiling(0.5f));
                    assertEquals("One", cursor.getValue());

                    assertFalse(cursor.movePrevious());

                    assertTrue(cursor.moveCeiling(100f));
                    assertEquals("Biggish", cursor.getValue());

                    assertTrue(cursor.movePrevious());
                    assertEquals("One", cursor.getValue());

                    assertTrue(cursor.moveCeiling(12345678901234.0f));
                    assertEquals("Infinity", cursor.getValue());

                    assertTrue(cursor.moveNext());
                    assertEquals("Not a number", cursor.getValue());

                    assertTrue(cursor.moveFloor(Float.NaN));
                    assertEquals("Not a number", cursor.getValue());
                }

                tx.commit();
            }
        }
    }

    @Test
    public void canStoreLocalDates() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<LocalDate, String> index = db.createIndex(tx, "Test", LocalDateSchema.INSTANCE, StringSchema.INSTANCE);

                index.put(tx, LocalDate.of(1999, 1, 1), "One");
                index.put(tx, LocalDate.of(1999, 1, 3), "Two");
                index.put(tx, LocalDate.of(1999, 1, 5), "Three");

                try (Cursor<LocalDate, String> cursor = index.createCursor(tx)) {
                    assertTrue(cursor.moveCeiling(LocalDate.of(1998, 1, 1)));
                    assertEquals(LocalDate.of(1999, 1, 1), cursor.getKey());
                    assertEquals("One", cursor.getValue());

                    assertTrue(cursor.moveCeiling(LocalDate.of(1999, 1, 2)));
                    assertEquals(LocalDate.of(1999, 1, 3), cursor.getKey());
                    assertEquals("Two", cursor.getValue());

                    assertTrue(cursor.moveLast());
                    assertEquals(LocalDate.of(1999, 1, 5), cursor.getKey());
                    assertEquals("Three", cursor.getValue());
                }

                tx.commit();
            }
        }
    }

    @Test
    public void canRemove() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<Integer, Integer> index = db.createIndex(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                index.put(tx, 1, 100);
                index.put(tx, 2, 200);
                index.put(tx, 3, 300);

                assertEquals(Arrays.asList(1, 2, 3), iteratorToList(index.keys(tx)));

                assertTrue(index.remove(tx, 2));
                assertEquals(Arrays.asList(1, 3), iteratorToList(index.keys(tx)));

                assertFalse(index.remove(tx, 2));
                assertEquals(Arrays.asList(1, 3), iteratorToList(index.keys(tx)));

                assertTrue(index.remove(tx, 3));
                assertEquals(Arrays.asList(1), iteratorToList(index.keys(tx)));
            }
        }
    }

    @Test
    public void canGetValues() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<Integer, Integer> index = db.createIndex(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                index.put(tx, 1, 100);
                index.put(tx, 2, 200);
                index.put(tx, 3, 100);

                assertEquals(Arrays.asList(100, 200, 100), iteratorToList(index.values(tx)));
            }
        }
    }

    @Test
    public void canGetKeyValues() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<Integer, Integer> index = db.createIndex(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                index.put(tx, 1, 100);
                index.put(tx, 2, 200);
                index.put(tx, 3, 100);

                assertEquals(Arrays.asList(new Loader.Pair<>(1, 100), new Loader.Pair<>(2, 200), new Loader.Pair<>(3, 100)),
                             iteratorToList(index.keyValues(tx)));
            }
        }
    }

    @Test
    public void canCursorAroundIndexWithDuplicateKeys() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final IndexWithDuplicateKeys<Integer, Integer> index = db.createIndexWithDuplicateKeys(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                index.put(tx, 1, 100);
                index.put(tx, 2, 200);
                index.put(tx, 2, 200);
                index.put(tx, 2, 300);
                index.put(tx, 2, 200);
                index.put(tx, 4, 400);

                try (CursorWithDuplicateKeys<Integer, Integer> cursor = index.createCursor(tx)) {
                    assertTrue(cursor.moveFirst());
                    assertEquals(1, cursor.getKey().intValue());
                    assertEquals(100, cursor.getValue().intValue());
                    assertEquals(1, cursor.keyItemCount());

                    assertFalse(cursor.moveNextOfKey());
                    assertFalse(cursor.movePreviousOfKey());

                    assertTrue(cursor.moveNext());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(200, cursor.getValue().intValue());
                    assertEquals(2, cursor.keyItemCount());

                    assertTrue(cursor.moveNext());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(300, cursor.getValue().intValue());

                    assertTrue(cursor.moveNext());
                    assertEquals(4, cursor.getKey().intValue());
                    assertEquals(400, cursor.getValue().intValue());

                    assertTrue(cursor.moveLastOfPreviousKey());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(300, cursor.getValue().intValue());

                    assertTrue(cursor.moveFirst());
                    assertTrue(cursor.moveFirstOfNextKey());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(200, cursor.getValue().intValue());

                    assertTrue(cursor.moveLastOfKey());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(300, cursor.getValue().intValue());

                    assertFalse(cursor.moveNextOfKey());
                    assertTrue(cursor.movePreviousOfKey());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(200, cursor.getValue().intValue());

                    assertFalse(cursor.movePreviousOfKey());
                    assertTrue(cursor.moveNextOfKey());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(300, cursor.getValue().intValue());

                    assertTrue(cursor.moveFirstOfKey());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(200, cursor.getValue().intValue());

                    // This exposed a bug in LMDB: the *{First,Last}OfKey methods didn't work if you were pointing to an entry without duplicates
                    assertTrue(cursor.moveFirst());
                    assertTrue(cursor.moveLastOfKey());
                    assertEquals(1, cursor.getKey().intValue());
                    assertEquals(100, cursor.getValue().intValue());
                }
            }
        }
    }

    @Test
    public void canCursorSeekIntoIndexWithDuplicateKeys() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final IndexWithDuplicateKeys<Integer, Integer> index = db.createIndexWithDuplicateKeys(tx, "Test", UnsignedIntegerSchema.INSTANCE, UnsignedIntegerSchema.INSTANCE);

                index.put(tx, 1, 100);
                index.put(tx, 2, 200);
                index.put(tx, 2, 300);
                index.put(tx, 4, 400);

                try (CursorWithDuplicateKeys<Integer, Integer> cursor = index.createCursor(tx)) {
                    assertTrue(cursor.moveFloor(3));
                    assertEquals(300, cursor.getValue().intValue());

                    assertTrue(cursor.moveCeiling(3));
                    assertEquals(400, cursor.getValue().intValue());

                    // The two-arg versions of moveFloorOfKey/moveCeilingOfKey should never move us to a different K value than the one we asked for
                    assertFalse(cursor.moveFloorOfKey(3, 350));
                    assertFalse(cursor.moveCeilingOfKey(3, 350));

                    assertFalse(cursor.moveFloorOfKey(2, 150));
                    assertTrue(cursor.moveFloorOfKey(2, 250));
                    assertEquals(200, cursor.getValue().intValue());

                    assertTrue(cursor.moveFloorOfKey(2, 350));
                    assertEquals(300, cursor.getValue().intValue());

                    assertTrue(cursor.moveCeilingOfKey(2, 250));
                    assertEquals(300, cursor.getValue().intValue());

                    assertTrue(cursor.moveCeilingOfKey(2, 150));
                    assertEquals(200, cursor.getValue().intValue());

                    assertFalse(cursor.moveCeilingOfKey(2, 350));
                    assertFalse(cursor.moveTo(1, 101));
                    assertFalse(cursor.moveTo(2, 201));
                    assertFalse(cursor.moveTo(2, 199));
                    assertTrue(cursor.moveTo(2, 200));

                    cursor.delete();
                    assertFalse(cursor.moveTo(2, 200));
                    assertTrue(cursor.moveTo(2, 300));
                }
            }
        }
    }

    @Test
    public void canPutWithJustAValueIntoCursorWithDuplicateKeys() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final IndexWithDuplicateKeys<Integer, Integer> index = db.createIndexWithDuplicateKeys(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                index.put(tx, 2, 200);
                index.put(tx, 2, 200);
                index.put(tx, 2, 300);
                index.put(tx, 2, 300);

                try (CursorWithDuplicateKeys<Integer, Integer> cursor = index.createCursor(tx)) {
                    assertTrue(cursor.moveTo(2));
                    assertEquals(2, cursor.keyItemCount());

                    cursor.put(200);
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(200, cursor.getValue().intValue());
                    assertEquals(2, cursor.keyItemCount());

                    cursor.put(250);
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(250, cursor.getValue().intValue());
                    assertEquals(3, cursor.keyItemCount());
                }
            }
        }
    }

    @Test
    public void indexLevelOperationsWorkOnIndexWithDuplicateKeys() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final IndexWithDuplicateKeys<Integer, Integer> index = db.createIndexWithDuplicateKeys(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                index.put(tx, 1, 100);
                index.put(tx, 2, 200);
                index.put(tx, 2, 200);
                index.put(tx, 2, 300);
                index.put(tx, 2, 200);
                index.put(tx, 4, 400);

                assertTrue(index.contains(tx, 1, 100));
                assertFalse(index.contains(tx, 1, 150));
                assertTrue(index.contains(tx, 2, 200));
                assertFalse(index.contains(tx, 2, 250));
                assertTrue(index.contains(tx, 2, 300));

                assertTrue(index.remove(tx, 2, 200));
                assertFalse(index.contains(tx, 2, 200));
                assertTrue(index.contains(tx, 2, 300));

                assertFalse(index.remove(tx, 2, 250));

                assertTrue(index.remove(tx, 1, 100));
                assertFalse(index.contains(tx, 1, 100));

                index.put(tx, 2, 200);
                assertTrue(index.remove(tx, 2));
                assertFalse(index.contains(tx, 2, 200));
                assertFalse(index.contains(tx, 2, 300));

                assertFalse(index.remove(tx, 2));
            }
        }
    }

    @Test
    public void putIfAbsentShouldWorkOnNormalIndex() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<Integer, Integer> index = db.createIndex(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                assertEquals(null, index.putIfAbsent(tx, 1, 100));
                assertEquals(Integer.valueOf(100), index.putIfAbsent(tx, 1, 100));
                assertEquals(Integer.valueOf(100), index.putIfAbsent(tx, 1, 200));
                assertEquals(Integer.valueOf(100), index.putIfAbsent(tx, 1, 100));
                assertEquals(null, index.putIfAbsent(tx, 2, 100));

                try (Cursor<Integer, Integer> cursor = index.createCursor(tx)) {
                    assertEquals(Integer.valueOf(100), cursor.putIfAbsent(2, 200));
                    assertEquals(100, cursor.getValue().intValue());
                    assertEquals(null, cursor.putIfAbsent(3, 100));
                    assertEquals(100, cursor.getValue().intValue());
                }
            }
        }
    }

    @Test
    public void putIfAbsentShouldWorkOnIndexWithDuplicates() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final IndexWithDuplicateKeys<Integer, Integer> index = db.createIndexWithDuplicateKeys(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                assertEquals(null, index.putIfAbsent(tx, 1, 100));
                assertEquals(Integer.valueOf(100), index.putIfAbsent(tx, 1, 100));
                assertEquals(null, index.putIfAbsent(tx, 1, 200));
                assertEquals(Integer.valueOf(100), index.putIfAbsent(tx, 1, 100));
                assertEquals(null, index.putIfAbsent(tx, 2, 100));

                try (CursorWithDuplicateKeys<Integer, Integer> cursor = index.createCursor(tx)) {
                    assertEquals(Integer.valueOf(100), cursor.putIfAbsent(2, 100));
                    assertEquals(100, cursor.getValue().intValue());
                    assertEquals(null, cursor.putIfAbsent(2, 200));
                    assertEquals(200, cursor.getValue().intValue());
                    assertEquals(Integer.valueOf(200), cursor.putIfAbsent(2, 200));
                    assertEquals(200, cursor.getValue().intValue());
                    assertEquals(null, cursor.putIfAbsent(3, 100));
                    assertEquals(100, cursor.getValue().intValue());
                }
            }
        }
    }

    @Test
    public void deleteAllOfKeyWorks() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final IndexWithDuplicateKeys<Integer, Integer> index = db.createIndexWithDuplicateKeys(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                index.put(tx, 1, 100);
                index.put(tx, 1, 200);
                index.put(tx, 2, 100);
                index.put(tx, 3, 200);

                try (CursorWithDuplicateKeys<Integer, Integer> cursor = index.createCursor(tx)) {
                    assertTrue(cursor.moveFirst());
                    assertEquals(2, cursor.keyItemCount());

                    cursor.deleteAllOfKey();
                    assertTrue(cursor.moveFirst());
                    assertEquals(Integer.valueOf(2), cursor.getKey());
                    assertEquals(1, cursor.keyItemCount());

                    cursor.deleteAllOfKey();
                    assertTrue(cursor.moveFirst());
                    assertEquals(Integer.valueOf(3), cursor.getKey());
                }
            }
        }
    }

    private static <T> List<T> iteratorToList(Iterator<T> it) {
        final ArrayList<T> result = new ArrayList<>();
        while (it.hasNext()) {
            result.add(it.next());
        }
        return result;
    }

    @Test
    public void canStoreNullFreeStrings() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<String, Long> index = db.createIndex(tx, "Test", NullFreeStringSchema.INSTANCE, LongSchema.INSTANCE);

                index.put(tx, "Foo", 1l);
                index.put(tx, "Fooa", 2l);
                index.put(tx, "Fo", 3l);
                index.put(tx, "Foa", 4l);

                assertEquals(Arrays.asList("Fo", "Foa", "Foo", "Fooa"), iteratorToList(index.keys(tx)));

                tx.commit();
            }
        }
    }

    @Test
    public void canStoreStrings() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<String, Long> index = db.createIndex(tx, "Test", StringSchema.INSTANCE, LongSchema.INSTANCE);

                index.put(tx, "Foo", 1l);
                index.put(tx, "Fooa", 2l);
                index.put(tx, "Fo", 3l);
                index.put(tx, "Foa", 4l);

                assertEquals(Arrays.asList("Fo", "Foa", "Foo", "Fooa"), iteratorToList(index.keys(tx)));

                tx.commit();
            }
        }
    }

    @Test
    public void canStoreCompositeStringKey() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<String, Integer> index = db.createIndex(tx, "Test", Schema.zipWith(StringSchema.INSTANCE, (String x) -> x.split("/", 2)[0],
                                                                                               StringSchema.INSTANCE, (String x) -> x.split("/", 2)[1],
                                                                                               (String x, String y) -> x + "/" + y),
                                                                                IntegerSchema.INSTANCE);

                index.put(tx, "Food/Bean", 10);
                index.put(tx, "Air/Bean", 11);
                index.put(tx, "Apple/Bean", 12);
                index.put(tx, "Apple/Beans", 13);
                index.put(tx, "Apple/Carrot", 14);
                index.put(tx, "Airpie/Bean", 15);

                assertEquals(Arrays.asList("Air/Bean", "Airpie/Bean", "Apple/Bean", "Apple/Beans", "Apple/Carrot", "Food/Bean"),
                             iteratorToList(index.keys(tx)));

                tx.commit();
            }
        }
    }

    @Test
    public void canStoreLongs() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<Long, String> index = db.createIndex(tx, "Test", LongSchema.INSTANCE, StringSchema.INSTANCE);

                index.put(tx, -100000000l, "Neg Big");
                index.put(tx, -1l, "Neg One");
                index.put(tx, 1l, "One");
                index.put(tx, 100000000l, "Big");

                assertEquals(Arrays.asList(-100000000l, -1l, 1l, 100000000l), iteratorToList(index.keys(tx)));

                tx.commit();
            }
        }
    }

    @Test
    public void canStoreFloats() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<Float, String> index = db.createIndex(tx, "Test", FloatSchema.INSTANCE, StringSchema.INSTANCE);

                index.put(tx, Float.NEGATIVE_INFINITY, "Neg Inf");
                index.put(tx, -100000000.0f, "Neg Big");
                index.put(tx, -1.0f, "Neg One");
                index.put(tx, 1.0f, "One");
                index.put(tx, 100000000.0f, "Big");
                index.put(tx, Float.POSITIVE_INFINITY, "Neg Inf");

                assertEquals(Arrays.asList(Float.NEGATIVE_INFINITY, -100000000.0f, -1.0f, 1.0f, 100000000.0f, Float.POSITIVE_INFINITY), iteratorToList(index.keys(tx)));

                tx.commit();
            }
        }
    }

    @Test
    public void canStoreDoubles() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<Double, String> index = db.createIndex(tx, "Test", DoubleSchema.INSTANCE, StringSchema.INSTANCE);

                index.put(tx, Double.NEGATIVE_INFINITY, "Neg Inf");
                index.put(tx, -100000000.0, "Neg Big");
                index.put(tx, -1.0, "Neg One");
                index.put(tx, 1.0, "One");
                index.put(tx, 100000000.0, "Big");
                index.put(tx, Double.POSITIVE_INFINITY, "Neg Inf");

                assertEquals(Arrays.asList(Double.NEGATIVE_INFINITY, -100000000.0, -1.0, 1.0, 100000000.0, Double.POSITIVE_INFINITY), iteratorToList(index.keys(tx)));

                tx.commit();
            }
        }
    }

    @Test
    public void unsignedIntegerSchemaStoresCorrectOrdering() {
        final long ptr = unsafe.allocateMemory(Integer.BYTES);
        try {
            UnsignedIntegerSchema.INSTANCE.write(new BitStream3(ptr, Integer.BYTES), 0xCAFEBABE);
            assertEquals((byte)0xCA, unsafe.getByte(ptr + 0));
            assertEquals((byte)0xFE, unsafe.getByte(ptr + 1));
            assertEquals((byte)0xBE, unsafe.getByte(ptr + Integer.BYTES - 1));
        } finally {
            unsafe.freeMemory(ptr);
        }
    }

    @Test
    public void integerSchemaStoresCorrectOrdering() {
        final long ptr = unsafe.allocateMemory(Integer.BYTES);
        try {
            IntegerSchema.INSTANCE.write(new BitStream3(ptr, Integer.BYTES), 0xCAFEBABE);
            // 0xC = 12 = 1010b ==> 0010b = 0x4
            assertEquals((byte)0x4A, unsafe.getByte(ptr + 0));
            assertEquals((byte)0xFE, unsafe.getByte(ptr + 1));
            assertEquals((byte)0xBE, unsafe.getByte(ptr + Integer.BYTES - 1));
        } finally {
            unsafe.freeMemory(ptr);
        }
    }

    @Test
    public void unsignedLongSchemaStoresCorrectOrdering() {
        final long ptr = unsafe.allocateMemory(Long.BYTES);
        try {
            UnsignedLongSchema.INSTANCE.write(new BitStream3(ptr, Long.BYTES), 0xCAFEBABEDEADBEEFl);
            assertEquals((byte)0xCA, unsafe.getByte(ptr + 0));
            assertEquals((byte)0xFE, unsafe.getByte(ptr + 1));
            assertEquals((byte)0xEF, unsafe.getByte(ptr + Long.BYTES - 1));
        } finally {
            unsafe.freeMemory(ptr);
        }
    }

    @Test
    public void longSchemaStoresCorrectOrdering() {
        final long ptr = unsafe.allocateMemory(Long.BYTES);
        try {
            LongSchema.INSTANCE.write(new BitStream3(ptr, Long.BYTES), 0xCAFEBABEDEADBEEFl);
            // 0xC = 12 = 1010b ==> 0010b = 0x4
            assertEquals((byte)0x4A, unsafe.getByte(ptr + 0));
            assertEquals((byte)0xFE, unsafe.getByte(ptr + 1));
            assertEquals((byte)0xEF, unsafe.getByte(ptr + Long.BYTES - 1));
        } finally {
            unsafe.freeMemory(ptr);
        }
    }

    @Test
    public void singleTransaction() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<Integer, String> index = db.createIndex(tx, "Test", IntegerSchema.INSTANCE, StringSchema.INSTANCE);

                index.put(tx, 1, "Hello");
                index.put(tx, 2, "World");
                index.put(tx, 3, "!");

                assertEquals("World", index.get(tx, 2));

                tx.commit();
            }
        }
    }

    @Test
    public void doubleTransaction() {
        try (final Database db = createDatabase()) {
            final Index<Integer, String> index;
            try (final Transaction tx = db.transaction(false)) {
                index = db.createIndex(tx, "Test", IntegerSchema.INSTANCE, StringSchema.INSTANCE);

                index.put(tx, 1, "Hello");
                index.put(tx, 2, "World");
                index.put(tx, 3, "!");

                tx.commit();
            }

            try (final Transaction tx = db.transaction(true)) {
                assertEquals("World", index.get(tx, 2));
            }
        }
    }

    @Test
    public void doubleDatabase() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<Integer, String> index = db.createIndex(tx, "Test", IntegerSchema.INSTANCE, StringSchema.INSTANCE);

                index.put(tx, 1, "Hello");
                index.put(tx, 2, "World");
                index.put(tx, 3, "!");

                tx.commit();
            }
        }

        try (final Database db = openDatabase()) {
            try (final Transaction tx = db.transaction(true)) {
                final Index<Integer, String> index = db.createIndex(tx, "Test", IntegerSchema.INSTANCE, StringSchema.INSTANCE);

                assertEquals("World", index.get(tx, 2));
            }
        }
    }

    @Test
    public void singleCursoredTransaction() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<Integer, String> index = db.createIndex(tx, "Test", IntegerSchema.INSTANCE, StringSchema.INSTANCE);
                try (final Cursor<Integer, String> cursor = index.createCursor(tx)) {
                    cursor.put(1, "Hello");
                    cursor.put(2, "World");
                    cursor.put(3, "!");

                    assertTrue(cursor.moveTo(2));
                    assertEquals("World", cursor.getValue());
                    assertEquals(2, cursor.getKey().longValue());
                }

                tx.commit();
            }
        }
    }

    @Test
    public void doubleCursoredTransaction() {
        try (final Database db = createDatabase()) {
            final Index<Integer, String> index;
            try (final Transaction tx = db.transaction(false)) {
                index = db.createIndex(tx, "Test", IntegerSchema.INSTANCE, StringSchema.INSTANCE);
                try (final Cursor<Integer, String> cursor = index.createCursor(tx)) {
                    cursor.put(1, "Hello");
                    cursor.put(2, "World");
                    cursor.put(3, "!");
                }

                tx.commit();
            }

            try (final Transaction tx = db.transaction(true)) {
                try (final Cursor<Integer, String> cursor = index.createCursor(tx)) {
                    assertTrue(cursor.moveTo(2));
                    assertEquals("World", cursor.getValue());
                    assertEquals(2, cursor.getKey().longValue());
                }
            }
        }
    }

    @Test
    public void boundedSizeKeyValues() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<String, String> index = db.createIndex(tx, "Test", new Latin1StringSchema(10), new Latin1StringSchema(10));

                index.put(tx, "Hello", "World");
                index.put(tx, "Goodbye", "Hades");

                assertEquals("World", index.get(tx, "Hello"));

                tx.commit();
            }
        }
    }

    @Test
    public void moveCursor() {
        try (final Database db = createDatabase()) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<Integer, String> index = db.createIndex(tx, "Test", IntegerSchema.INSTANCE, new Latin1StringSchema(10));

                index.put(tx, 1, "World");
                index.put(tx, 3, "Heaven");
                index.put(tx, 5, "Hades");

                try (final Cursor<Integer, String> cursor = index.createCursor(tx)) {

                    // moveFirst/moveNext/moveLast

                    assertTrue(cursor.moveFirst());
                    assertEquals(1, cursor.getKey().intValue());
                    assertEquals("World", cursor.getValue());

                    assertFalse(cursor.movePrevious());

                    assertTrue(cursor.moveNext());
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());

                    assertTrue(cursor.moveLast());
                    assertEquals(5, cursor.getKey().intValue());
                    assertEquals("Hades", cursor.getValue());

                    assertFalse(cursor.moveNext());


                    // movePrevious when you are at the start doesn't do anything to your current position:
                    assertTrue(cursor.moveFirst());
                    assertTrue(cursor.moveNext());
                    assertEquals(3, cursor.getKey().intValue());

                    // moveNext when you are at the end doesn't do anything to your current position:
                    assertTrue(cursor.moveLast());
                    assertTrue(cursor.movePrevious());
                    assertEquals(3, cursor.getKey().intValue());


                    // moveTo

                    assertTrue(cursor.moveTo(3));
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());

                    assertTrue(cursor.moveTo(1));
                    assertEquals(1, cursor.getKey().intValue());
                    assertEquals("World", cursor.getValue());

                    assertTrue(cursor.moveTo(5));
                    assertEquals(5, cursor.getKey().intValue());
                    assertEquals("Hades", cursor.getValue());

                    assertFalse(cursor.moveTo(4));


                    // moveCeilingOfKey

                    assertTrue(cursor.moveCeiling(2));
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());

                    assertTrue(cursor.moveCeiling(3));
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());

                    assertTrue(cursor.moveCeiling(0));
                    assertEquals(1, cursor.getKey().intValue());
                    assertEquals("World", cursor.getValue());

                    assertFalse(cursor.moveCeiling(6));

                    // At this point the cursor is "off the end" so going back 1 will take us to the last item
                    assertTrue(cursor.movePrevious());
                    assertEquals(5, cursor.getKey().intValue());
                    assertEquals("Hades", cursor.getValue());


                    // moveFloorOfKey

                    assertTrue(cursor.moveFloor(4));
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());

                    assertTrue(cursor.moveFloor(3));
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());

                    assertTrue(cursor.moveFloor(6));
                    assertEquals(5, cursor.getKey().intValue());
                    assertEquals("Hades", cursor.getValue());

                    assertFalse(cursor.moveFloor(0));

                    // At this point the cursor is (a bit inconsistently..) actually pointing to the first item.
                    // There doesn't seem be any way to get it to go "off the beginning" such that moveNext()
                    // takes you to the first item, otherwise I'd probably arrange for moveFloorOfKey() to do that.
                    // (I tried going to the first item then doing movePrevious() but that just leaves you in the same place.)
                    assertTrue(cursor.moveNext());
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());
                }

                tx.commit();
            }
        }
    }
}
