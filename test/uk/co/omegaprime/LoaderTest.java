package uk.co.omegaprime;

import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.omegaprime.Loader.*;

public class LoaderTest {
    static final File dbDirectory = new File("/Users/mbolingbroke/example.lmdb");

    private Database createDatabase() {
        if (dbDirectory.exists()) {
            for (File f : dbDirectory.listFiles()) {
                f.delete();
            }
            dbDirectory.delete();
        }
        dbDirectory.mkdir();

        return openDatabase();
    }

    private Database openDatabase() {
        return new Database(dbDirectory, new DatabaseOptions().maxIndexes(40).mapSize(1_073_741_824));
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
                final Cursor<Integer, String> cursor = index.createCursor(tx);

                cursor.put(1, "Hello");
                cursor.put(2, "World");
                cursor.put(3, "!");

                assertTrue(cursor.moveTo(2));
                assertEquals("World", cursor.getValue());
                assertEquals(2, cursor.getKey().longValue());

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
                final Cursor<Integer, String> cursor = index.createCursor(tx);

                cursor.put(1, "Hello");
                cursor.put(2, "World");
                cursor.put(3, "!");

                tx.commit();
            }

            try (final Transaction tx = db.transaction(true)) {
                final Cursor<Integer, String> cursor = index.createCursor(tx);

                assertTrue(cursor.moveTo(2));
                assertEquals("World", cursor.getValue());
                assertEquals(2, cursor.getKey().longValue());
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
}
