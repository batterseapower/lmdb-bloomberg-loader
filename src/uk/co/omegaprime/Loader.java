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
                //TemporalDenseLoader.load(db, tx, date, zis);
                // Range-based:
                TemporalSparseLoader.load(db, tx, date, zis);
            }
        }
    }
}
