package uk.co.omegaprime;

import au.com.bytecode.opencsv.CSVReader;
import uk.co.omegaprime.thunder.*;
import uk.co.omegaprime.thunder.schema.*;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Loader {
    private static class Source {
        private final Instant instant;

        public Source(Instant instant) {
            this.instant = instant;
        }

        public Instant getInstant() {
            return instant;
        }
    }

    private static Iterator<File> availableFiles(File directory) {
        return Stream.of(directory.listFiles()).filter((File f) -> FILENAME_REGEX.matcher(f.getName()).matches()).iterator();
    }

    private static final Field addressField;
    private static final Field capacityField;

    static {
        try {
            addressField = Buffer.class.getDeclaredField("address");
            capacityField = Buffer.class.getDeclaredField("capacity");

            addressField.setAccessible(true);
            capacityField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Failed to get fields to construct naughty DirectByteBuffer", e);
        }
    }


    // NB: caller is still responsible for deallocating the supplied memory if necessary
    private static ByteBuffer wrapPointer(long ptr, int sz) {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(0).order(ByteOrder.nativeOrder());

        try {
            addressField.setLong(buffer, ptr);
            capacityField.setInt(buffer, sz);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to construct naughty DirectByteBuffer", e);
        }

        buffer.limit(sz);

        return buffer;
    }

    /*
    private static class NativeBuffer<T> {
        final long sz;
        final long data;

        public NativeBuffer(long mdbValPtr) {
            this.sz = unsafe.getAddress(mdbValPtr);
            this.data = unsafe.getAddress(mdbValPtr + Unsafe.ADDRESS_SIZE);
        }

        public byte[] getBytes() {
            if (sz < 0 || sz > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Value of size " + sz + " is larger than is representable in a Java array");
            }

            final byte[] bs = new byte[(int)sz];
            for (int i = 0; i < bs.length; i++) {
                bs[i] = unsafe.getByte(data + i);
            }
            return bs;
        }
    }
    */

    public static void main(String[] args) throws IOException {
        final File dbDirectory = new File("/Users/mbolingbroke/example.lmdb");
        if (dbDirectory.exists()) {
            for (File f : dbDirectory.listFiles()) {
                f.delete();
            }
            dbDirectory.delete();
        }
        dbDirectory.mkdir();

        try (final Database db = new Database(dbDirectory, new DatabaseOptions().maxIndexes(40).mapSize(1024*1024*1024))) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<File, Source> sourcesIndex = db.<File, Source>createIndex(tx, "Sources", StringSchema.INSTANCE.map(File::getAbsolutePath, File::new),
                                                                                                     InstantSchema.INSTANCE_SECOND_RESOLUTION.map(Source::getInstant, Source::new));
                final Iterator<File> it = availableFiles(new File("/Users/mbolingbroke/Downloads"));
                while (it.hasNext()) {
                    final File file = it.next();
                    if (!sourcesIndex.contains(tx, file)) {
                        loadZip(db, tx, file);
                        sourcesIndex.put(tx, file, new Source(Instant.now()));
                    }
                }

                tx.commit();
            }
        }
    }

    private static class FieldKey {
        private final String idBBGlobal;
        private final LocalDate date;

        public FieldKey(String idBBGlobal, LocalDate date) {
            this.idBBGlobal = idBBGlobal;
            this.date = date;
        }

        public String getIDBBGlobal() { return idBBGlobal; }
        public LocalDate getDate() { return date; }
    }

    private static class FieldKeySchema {
        public static Schema<FieldKey> INSTANCE = Schema.zipWith(new Latin1StringSchema(20), FieldKey::getIDBBGlobal,
                                                                 LocalDateSchema.INSTANCE,   FieldKey::getDate,
                                                                 FieldKey::new);
    }

    private static class FieldValue {
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

    private static class FieldValueSchema {
        public static Schema<FieldValue> INSTANCE = Schema.zipWith(new Latin1StringSchema(64),                FieldValue::getValue,
                                                                   Schema.nullable(LocalDateSchema.INSTANCE), FieldValue::getToDate,
                                                                   FieldValue::new);
    }

    final static Pattern FILENAME_REGEX = Pattern.compile("Equity_Common_Stock_([0-9]+)[.]txt[.]zip");
    final static DateTimeFormatter FILENAME_DTF = DateTimeFormatter.ofPattern("yyyyMMdd");

    public static void loadZip(Database db, Transaction tx, File file) throws IOException {
        final Matcher m = FILENAME_REGEX.matcher(file.getName());
        if (!m.matches()) throw new IllegalStateException("Supplied file name " + file.getName() + " did not match expected pattern");

        final String dateString = m.group(1);
        final LocalDate date = LocalDate.parse(dateString, FILENAME_DTF);

        try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                System.out.println("Loading " + file + ":" + entry);
                // Dense:
                //loadZippedFile(db, tx, date, zis);
                // Range-based:
                loadZippedFile2(db, tx, date, zis);
            }
        }
    }

    private static void loadZippedFile(Database db, Transaction tx, LocalDate date, ZipInputStream zis) throws IOException {
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
                final String indexName = headers[i].replace(" ", "");
                final Index<FieldKey, String> index = db.createIndex(tx, indexName, FieldKeySchema.INSTANCE, new Latin1StringSchema(64));
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
        System.out.println("Loaded " + items + " in " + duration + "ns (" + (duration / items) + "ns/item)");
    }

    private static void loadZippedFile2(Database db, Transaction tx, LocalDate date, ZipInputStream zis) throws IOException {
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
                final Index<FieldKey, FieldValue> index = db.createIndex(tx, indexName, FieldKeySchema.INSTANCE, FieldValueSchema.INSTANCE);
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
                    if (cursor.moveFloor(fieldKey) && cursor.getKey().idBBGlobal.equals(fieldKey.idBBGlobal)) {
                        // There is a range in the map starting before the date of interest: we might have to truncate it
                        final FieldValue fieldValue = cursor.getValue();
                        if (fieldValue.getToDate() == null || fieldValue.getToDate().isAfter(date)) {
                            cursor.put(fieldValue.setToDate(date));
                        }
                    }
                } else {
                    final boolean mustCreate;
                    if (cursor.moveFloor(fieldKey) && cursor.getKey().idBBGlobal.equals(fieldKey.idBBGlobal)) {
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
