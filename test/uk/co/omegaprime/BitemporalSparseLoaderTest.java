package uk.co.omegaprime;

import org.junit.Test;
import uk.co.omegaprime.thunder.Database;
import uk.co.omegaprime.thunder.DatabaseOptions;
import uk.co.omegaprime.thunder.Pair;
import uk.co.omegaprime.thunder.Transaction;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public class BitemporalSparseLoaderTest {
    private static Supplier<Database> prepareDatabase() {
        final File dbDirectory;
        try {
            dbDirectory = Files.createTempDirectory("DatabaseTest").toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final File[] files = dbDirectory.listFiles();
        if (files != null) {
            for (File f : files) {
                f.delete();
            }
            if (!dbDirectory.delete()) {
                throw new IllegalStateException("Failed to delete target directory " + dbDirectory);
            }
        }
        dbDirectory.mkdir();
        dbDirectory.deleteOnExit();

        return () -> new Database(dbDirectory, new DatabaseOptions().maxIndexes(40).mapSize(1024 * 1024));
    }

    private static Database createDatabase() {
        return prepareDatabase().get();
    }

    private static <K, V> V getOrElseUpdate(Map<K, V> mp, K k, Supplier<V> supplier) {
        V v = mp.get(k);
        if (v == null) {
            v = supplier.get();
            mp.put(k, v);
        }
        return v;
    }

    public void randomTest(boolean sourcesAreExhaustive) throws IOException {
        final Random random = new Random();
        final long seed = random.nextLong();
        random.setSeed(seed);
        System.out.println(seed);

        for (int i = 0; i < 100; i++) {
            try (Database db = createDatabase();
                 Transaction tx = db.transaction(false)) {

                final Map<Integer, Map<String, Map<String, SortedMap<LocalDate, String>>>> actualBySourceID = new HashMap<>();

                final Map<String, Map<String, SortedMap<LocalDate, String>>> expected = new HashMap<>();
                final Map<String, SortedMap<LocalDate, String>> expectedName  = new HashMap<>(); expected.put("NAME",  expectedName);
                final Map<String, SortedMap<LocalDate, String>> expectedPrice = new HashMap<>(); expected.put("PRICE", expectedPrice);
                final Map<String, Pair<String, LocalDate>> lastSeenByID = new HashMap<>();
                for (int j = 0; j < 6; j++) {
                    final LocalDate date = LocalDate.of(2014, 1, 1).plusDays(j);
                    for (String delivery : random.nextBoolean() ? new String[] { "foo" } : new String[] { "foo", "bar" }) {
                        final StringBuilder csv = new StringBuilder();
                        csv.append("ID_BB_GLOBAL|NAME|PRICE\n");
                        for (String id : new String[] { "TERRY", "MIKE" }) {
                            final boolean inThisDelivery = id.equals("TERRY") || random.nextBoolean();

                            final Pair<String, LocalDate> lastSeen = lastSeenByID.get(id);
                            if (lastSeen != null && (inThisDelivery || (!sourcesAreExhaustive && lastSeen.k.equals(delivery)))) {
                                final LocalDate padDate = lastSeen.v;
                                final String padName  = expectedName .getOrDefault(id, new TreeMap<>()).get(padDate);
                                final String padPrice = expectedPrice.getOrDefault(id, new TreeMap<>()).get(padDate);
                                {
                                    LocalDate paddingDate = padDate.plusDays(1);
                                    while (!paddingDate.isAfter(date)) {
                                        if (padName  != null) getOrElseUpdate(expectedName,  id, TreeMap::new).put(paddingDate, padName);
                                        if (padPrice != null) getOrElseUpdate(expectedPrice, id, TreeMap::new).put(paddingDate, padPrice);
                                        paddingDate = paddingDate.plusDays(1);
                                    }
                                }
                            }

                            if (inThisDelivery) {
                                lastSeenByID.put(id, new Pair<>(delivery, date));

                                final String name = id + random.nextBoolean();
                                final String price = random.nextBoolean() ? null : Integer.toString(10 * random.nextInt(4));
                                csv.append(id).append('|')
                                        .append(name).append('|')
                                        .append(price == null ? "" : price).append('\n');

                                getOrElseUpdate(expectedName, id, TreeMap::new).put(date, name);
                                final SortedMap<LocalDate, String> expectedPriceForId = getOrElseUpdate(expectedPrice, id, TreeMap::new);
                                if (price != null) {
                                    expectedPriceForId.put(date, price);
                                } else {
                                    expectedPriceForId.remove(date);
                                    if (expectedPriceForId.isEmpty()) {
                                        expectedPrice.remove(id);
                                    }
                                }
                            }
                        }

                        if (sourcesAreExhaustive) {
                            for (Map.Entry<String, Pair<String, LocalDate>> lastSeenEntry : new HashSet<>(lastSeenByID.entrySet())) {
                                if (lastSeenEntry.getValue().k.equals(delivery) && lastSeenEntry.getValue().v.isBefore(date)) {
                                    // Expected in this delivery but didn't show up: it must have been deleted. Don't pad anything forward.
                                    lastSeenByID.remove(lastSeenEntry.getKey());
                                }
                            }
                        }

                        System.out.println(delivery + ": " + date);
                        System.out.println(csv.toString());
                        final int sourceID = BitemporalSparseLoader.load(db, tx, date, delivery, sourcesAreExhaustive, new ByteArrayInputStream(csv.toString().getBytes()));
                        actualBySourceID.put(sourceID, BitemporalSparseLoader.currentSourceToJava(db, tx));

                        BitemporalSparseLoader.checkInvariants(db, tx); // Just in case!
                    }

                    final Map<String, Map<String, SortedMap<LocalDate, String>>> actual = BitemporalSparseLoader.currentSourceToJava(db, tx);
                    assertEquals(expected, actual);
                }

                // Test the bitemporal aspect of the DB: ensure that we can reconstruct previously seen sources
                for (int sourceID : actualBySourceID.keySet()) {
                    assertEquals(Integer.toString(sourceID), actualBySourceID.get(sourceID), BitemporalSparseLoader.sourceToJava(db, tx, sourceID));
                }
            }
        }
    }

    @Test
    public void testRandomExhaustiveLoad() throws IOException {
        randomTest(true);
    }

    @Test
    public void testRandomNonExhaustiveLoad() throws IOException {
        randomTest(false);
    }

    @Test
    public void testCanRollBackToSource() throws IOException {
        throw new IllegalStateException("FIXME: test this");
    }
}
