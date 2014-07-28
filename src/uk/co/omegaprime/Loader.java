package uk.co.omegaprime;

import au.com.bytecode.opencsv.CSVReader;
import org.fusesource.lmdbjni.JNI;
import org.fusesource.lmdbjni.Util;
import sun.misc.Unsafe;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static uk.co.omegaprime.Bits.*;

public class Loader {
    private static final Unsafe unsafe = Bits.unsafe;

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

    static class DatabaseOptions {
        int createPermissions = 0644;
        long mapSizeBytes = 10_485_760;
        long maxIndexes   = 1;
        long maxReaders   = 126;
        int flags         = JNI.MDB_WRITEMAP;

        public DatabaseOptions createPermissions(int perms) { this.createPermissions = perms; return this; }
        public DatabaseOptions mapSize(long bytes)          { this.mapSizeBytes = bytes; return this; }
        public DatabaseOptions maxIndexes(long indexes)     { this.maxIndexes = indexes; return this; }
        public DatabaseOptions maxReaders(long readers)     { this.maxReaders = readers; return this; }

        private DatabaseOptions flag(int flag, boolean set) { this.flags = set ? flags | flag : flags & ~flag; return this; }
        public DatabaseOptions writeMap(boolean set)       { return flag(JNI.MDB_WRITEMAP,   set); }
        public DatabaseOptions noSubDirectory(boolean set) { return flag(JNI.MDB_NOSUBDIR,   set); }
        public DatabaseOptions readOnly(boolean set)       { return flag(JNI.MDB_RDONLY,     set); }
        public DatabaseOptions noTLS(boolean set)          { return flag(JNI.MDB_NOTLS,      set); }
    }

    static class Database implements AutoCloseable {
        final long env;

        public Database(File file) {
            this(file, new DatabaseOptions());
        }

        public Database(File file, DatabaseOptions options) {
            final long[] envPtr = new long[1];
            Util.checkErrorCode(JNI.mdb_env_create(envPtr));
            env = envPtr[0];

            Util.checkErrorCode(JNI.mdb_env_set_maxdbs    (env, options.maxIndexes));
            Util.checkErrorCode(JNI.mdb_env_set_mapsize   (env, options.mapSizeBytes));
            Util.checkErrorCode(JNI.mdb_env_set_maxreaders(env, options.maxReaders));

            Util.checkErrorCode(JNI.mdb_env_open(env, file.getAbsolutePath(), options.flags, options.createPermissions));
        }

        public void setMetaSync(boolean enabled) { Util.checkErrorCode(JNI.mdb_env_set_flags(env, JNI.MDB_NOMETASYNC, enabled ? 0 : 1)); }
        public void setSync    (boolean enabled) { Util.checkErrorCode(JNI.mdb_env_set_flags(env, JNI.MDB_NOSYNC,     enabled ? 0 : 1)); }
        public void setMapSync (boolean enabled) { Util.checkErrorCode(JNI.mdb_env_set_flags(env, JNI.MDB_MAPASYNC,   enabled ? 0 : 1)); }

        public void sync(boolean force) { Util.checkErrorCode(JNI.mdb_env_sync(env, force ? 1 : 0)); }

        public <K, V> Index<K, V> index(Transaction tx, String name, Schema<K> kSchema, Schema<V> vSchema) {
            return index(tx, name, kSchema, vSchema, false);
        }
        public <K, V> Index<K, V> createIndex(Transaction tx, String name, Schema<K> kSchema, Schema<V> vSchema) {
            return index(tx, name, kSchema, vSchema, true);
        }

        public <K, V> Index<K, V> index(Transaction tx, String name, Schema<K> kSchema, Schema<V> vSchema, boolean allowCreation) {
            final long[] dbiPtr = new long[1];
            Util.checkErrorCode(JNI.mdb_dbi_open(tx.txn, name, allowCreation ? JNI.MDB_CREATE : 0, dbiPtr));
            return new Index<>(this, dbiPtr[0], kSchema, vSchema);
        }

        // Quoth the docs:
        //   A transaction and its cursors must only be used by a single
	    //   thread, and a thread may only have a single transaction at a time.
	    //   If #MDB_NOTLS is in use, this does not apply to read-only transactions.
        public Transaction transaction(boolean isReadOnly) {
            final long[] txnPtr = new long[1];
            Util.checkErrorCode(JNI.mdb_txn_begin(env, 0, isReadOnly ? JNI.MDB_RDONLY : 0, txnPtr));
            return new Transaction(txnPtr[0]);
        }

        public void close() {
            JNI.mdb_env_close(env);
        }
    }

    static class Transaction implements AutoCloseable {
        final long txn;
        boolean handleFreed = false;

        Transaction(long txn) {
            this.txn = txn;
        }

        public void abort() {
            handleFreed = true;
            JNI.mdb_txn_abort(txn);
        }

        public void commit() {
            handleFreed = true;
            Util.checkErrorCode(JNI.mdb_txn_commit(txn));
        }

        public void close() {
            if (!handleFreed) {
                // Get a very scary JVM crash if we call this after already calling commit()
                abort();
            }
        }
    }

    interface Schema<T> {
        public static <T, U, V> Schema<V> zipWith(Schema<T> leftSchema, Function<V, T> leftProj, Schema<U> rightSchema, Function<V, U> rightProj, BiFunction<T, U, V> f) {
            return new Schema<V>() {
                @Override
                public V read(BitStream3 bs) {
                    return f.apply(leftSchema.read(bs), rightSchema.read(bs));
                }

                @Override
                public int maximumSizeBits() {
                    return (leftSchema.maximumSizeBits() >= 0 && rightSchema.maximumSizeBits() >= 0) ? Math.max(leftSchema.maximumSizeBits(), rightSchema.maximumSizeBits()) : -1;
                }

                @Override
                public int sizeBits(V x) {
                    return leftSchema.sizeBits(leftProj.apply(x)) + rightSchema.sizeBits(rightProj.apply(x));
                }

                @Override
                public void write(BitStream3 bs, V x) {
                    leftSchema.write (bs, leftProj .apply(x));
                    rightSchema.write(bs, rightProj.apply(x));
                }
            };
        }

        public static <T> Schema<T> nullable(Schema<T> schema) {
            return new Schema<T>() {
                @Override
                public T read(BitStream3 bs) {
                    if (!bs.getBoolean()) {
                        return null;
                    } else {
                        return schema.read(bs);
                    }
                }

                @Override
                public int maximumSizeBits() {
                    return 2 + schema.maximumSizeBits();
                }

                @Override
                public int sizeBits(T x) {
                    return 1 + (x == null ? 0 : schema.sizeBits(x));
                }

                @Override
                public void write(BitStream3 bs, T x) {
                    if (x == null) {
                        bs.putBoolean(false);
                    } else {
                        bs.putBoolean(true);
                        schema.write(bs, x);
                    }
                }
            };
        }

        T read(BitStream3 bs);
        int maximumSizeBits();
        int sizeBits(T x);
        void write(BitStream3 bs, T x);

        default <U> Schema<U> map(Function<U, T> f, Function<T, U> g) {
            final Schema<T> parent = this;
            return new Schema<U>() {
                public U read(BitStream3 bs) {
                    return g.apply(parent.read(bs));
                }

                public int maximumSizeBits() {
                    return parent.maximumSizeBits();
                }

                public int sizeBits(U x) {
                    return parent.sizeBits(f.apply(x));
                }

                public void write(BitStream3 bs, U x) {
                    parent.write(bs, f.apply(x));
                }
            };
        }
    }

    static class VoidSchema implements Schema<Void> {
        public static VoidSchema INSTANCE = new VoidSchema();

        public Void read(BitStream3 bs) { return null; }
        public int maximumSizeBits() { return 0; }
        public int sizeBits(Void x) { return maximumSizeBits(); }
        public void write(BitStream3 bs, Void x) { }
    }

    static class IntegerSchema implements Schema<Integer> {
        public static IntegerSchema INSTANCE = new IntegerSchema();

        public Integer read(BitStream3 bs) { return swapSign(bs.getInt()); }
        public int maximumSizeBits() { return Integer.BYTES * 8; }
        public int sizeBits(Integer x) { return maximumSizeBits(); }
        public void write(BitStream3 bs, Integer x) { bs.putInt(swapSign(x)); }
    }

    static class UnsignedIntegerSchema implements Schema<Integer> {
        public static UnsignedIntegerSchema INSTANCE = new UnsignedIntegerSchema();

        public Integer read(BitStream3 bs) { return bs.getInt(); }
        public int maximumSizeBits() { return Integer.BYTES * 8; }
        public int sizeBits(Integer x) { return maximumSizeBits(); }
        public void write(BitStream3 bs, Integer x) { bs.putInt(x); }
    }

    static class LongSchema implements Schema<Long> {
        public static LongSchema INSTANCE = new LongSchema();

        public Long read(BitStream3 bs) { return swapSign(bs.getLong()); }
        public int maximumSizeBits() { return Long.BYTES * 8; }
        public int sizeBits(Long x) { return maximumSizeBits(); }
        public void write(BitStream3 bs, Long x) { bs.putLong(swapSign(x)); }
    }

    static class UnsignedLongSchema implements Schema<Long> {
        public static UnsignedLongSchema INSTANCE = new UnsignedLongSchema();

        public Long read(BitStream3 bs) { return bs.getLong(); }
        public int maximumSizeBits() { return Long.BYTES * 8; }
        public int sizeBits(Long x) { return maximumSizeBits(); }
        public void write(BitStream3 bs, Long x) { bs.putLong(x); }
    }

    static class FloatSchema implements Schema<Float> {
        public static FloatSchema INSTANCE = new FloatSchema();

        // This sign-swapping magic is due to HBase's OrderedBytes class (and from Orderly before that)
        private int toDB(int l) {
            return l ^ ((l >> Integer.SIZE - 1) | Integer.MIN_VALUE);
        }

        private int fromDB(int l) {
            return l ^ ((~l >> Integer.SIZE - 1) | Integer.MIN_VALUE);
        }

        public Float read(BitStream3 bs) { return Float.intBitsToFloat(fromDB(bs.getInt())); }
        public int maximumSizeBits() { return Float.BYTES * 8; }
        public int sizeBits(Float x) { return maximumSizeBits(); }
        public void write(BitStream3 bs, Float x) { bs.putInt(toDB(Float.floatToRawIntBits(x))); }
    }

    static class DoubleSchema implements Schema<Double> {
        public static DoubleSchema INSTANCE = new DoubleSchema();

        // This sign-swapping magic is due to HBase's OrderedBytes class (and from Orderly before that)
        private long toDB(long l) {
            return l ^ ((l >> Long.SIZE - 1) | Long.MIN_VALUE);
        }

        private long fromDB(long l) {
            return l ^ ((~l >> Long.SIZE - 1) | Long.MIN_VALUE);
        }

        public Double read(BitStream3 bs) { return Double.longBitsToDouble(fromDB(bs.getLong())); }
        public int maximumSizeBits() { return Double.BYTES * 8; }
        public int sizeBits(Double x) { return maximumSizeBits(); }
        public void write(BitStream3 bs, Double x) { bs.putLong(toDB(Double.doubleToRawLongBits(x))); }
    }

    static class Latin1StringSchema implements Schema<String> {
        public static Latin1StringSchema INSTANCE = new Latin1StringSchema();

        private final int maximumLength;

        public Latin1StringSchema() { this(-1); }
        public Latin1StringSchema(int maximumLength) { this.maximumLength = maximumLength; }

        @Override
        public String read(BitStream3 bs) {
            final long mark = bs.mark();
            int count = 0;
            while (bs.getBoolean()) {
                count++;
                bs.getByte();
            }
            bs.reset(mark);

            final char[] cs = new char[count];
            for (int i = 0; i < cs.length; i++) {
                if (!bs.getBoolean()) throw new IllegalStateException("Latin1StringSchema.read: impossible");
                cs[i] = (char)bs.getByte();
            }
            if (bs.getBoolean()) throw new IllegalStateException("Latin1StringSchema.read: impossible");
            return new String(cs);
        }

        @Override
        public int maximumSizeBits() {
            return maximumLength * 9 + 1;
        }

        @Override
        public int sizeBits(String x) {
            return x.length() * 9 + 1;
        }

        @Override
        public void write(BitStream3 bs, String x) {
            if (maximumLength >= 0 && x.length() > maximumLength) {
                throw new IllegalArgumentException("Supplied string " + x + " would be truncated to maximum size of " + maximumLength + " chars");
            }

            for (int i = 0; i < x.length(); i++) {
                final char c = x.charAt(i);
                bs.putBoolean(true);
                bs.putByte((byte)((int)c < 255 ? c : '?'));
            }
            bs.putBoolean(false);
        }

    }

    static class StringSchema {
        private static final Charset UTF8 = Charset.forName("UTF-8");

        // TODO: due to the structure of UTF-8 it's actually possible to have less overhead than this
        public static Schema<String> INSTANCE = ByteArraySchema.INSTANCE.map((String x) -> x.getBytes(UTF8), (byte[] xs) -> new String(xs, UTF8));
    }

    // TODO: should this be the default?
    static class NullFreeStringSchema implements Schema<String> {
        public static NullFreeStringSchema INSTANCE = new NullFreeStringSchema();

        private static final Charset UTF8 = Charset.forName("UTF-8");

        @Override
        public String read(BitStream3 bs) {
            final long mark = bs.mark();
            int count = 0;
            do {
                byte b = bs.getByte();
                if (b == 0) {
                    break;
                } else if ((b & 0xF0) == 0xF0) {
                    bs.getByte();
                    bs.getByte();
                    bs.getByte();
                } else if ((b & 0xE0) == 0xE0) {
                    bs.getByte();
                    bs.getByte();
                } else if ((b & 0xC0) == 0xC0) {
                    bs.getByte();
                }
                count++;
            } while (true);
            bs.reset(mark);

            final byte[] bytes = new byte[count];
            for (int i = 0; i < bytes.length; i++) {
                bytes[i] = bs.getByte();
            }
            if (bs.getByte() != 0) throw new IllegalStateException("NullFreeStringSchema.read(): impossible");

            return new String(bytes, UTF8);
        }

        @Override
        public int maximumSizeBits() {
            return -1;
        }

        @Override
        public int sizeBits(String x) {
            return x.getBytes(UTF8).length * 8 + 8;
        }

        @Override
        public void write(BitStream3 bs, String x) {
            final byte[] bytes = x.getBytes(UTF8);
            for (int i = 0; i < bytes.length; i++) {
                if (bytes[i] == 0) {
                    throw new IllegalArgumentException("Input string " + x + " contained a null byte");
                }
                bs.putByte(bytes[i]);
            }
            bs.putByte((byte)0);
        }
    }

    static class InstantSchema {
        public static Schema<Instant> INSTANCE_SECOND_RESOLUTION = LongSchema.INSTANCE.map(Instant::getEpochSecond, Instant::ofEpochSecond);
    }

    static class LocalDateSchema {
        public static Schema<LocalDate> INSTANCE = LongSchema.INSTANCE.map(LocalDate::toEpochDay, LocalDate::ofEpochDay);
    }

    static class ByteArraySchema implements Schema<byte[]> {
        public static Schema<byte[]> INSTANCE = new ByteArraySchema();

        public byte[] read(BitStream3 bs) {
            final long mark = bs.mark();
            int count = 0;
            while (bs.getBoolean()) {
                count++;
                bs.getByte();
            }
            bs.reset(mark);

            final byte[] xs = new byte[count];
            for (int i = 0; i < xs.length; i++) {
                if (!bs.getBoolean()) throw new IllegalStateException("ByteArraySchema.read: impossible");
                xs[i] = bs.getByte();
            }
            if (bs.getBoolean()) throw new IllegalStateException("ByteArraySchema.read: impossible");
            return xs;
        }

        public int maximumSizeBits() { return -1; }
        public int sizeBits(byte[] x) { return x.length * 9 + 1; }

        public void write(BitStream3 bs, byte[] xs) {
            for (byte x : xs) {
                bs.putBoolean(true);
                bs.putByte(x);
            }
            bs.putBoolean(false);
        }
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

    static class Index<K, V> implements AutoCloseable {
        final Database db;
        final long dbi;
        final Schema<K> kSchema;
        final Schema<V> vSchema;

        // Used for temporary scratch storage within the context of a single method only, basically
        // just to save some calls to the allocator. The sole reason why Index is not thread safe.
        final long kBufferPtr, vBufferPtr;
        final BitStream3 bs = new BitStream3();

        Index(Database db, long dbi, Schema<K> kSchema, Schema<V> vSchema) {
            this.db = db;
            this.dbi = dbi;
            this.kSchema = kSchema;
            this.vSchema = vSchema;

            this.kBufferPtr = allocateSharedBufferPointer(kSchema);
            this.vBufferPtr = allocateSharedBufferPointer(vSchema);
        }

        private static <T> long allocateSharedBufferPointer(Schema<T> schema) {
            if (schema.maximumSizeBits() < 0) {
                // TODO: speculatively allocate a reasonable amount of memory that most allocations of interest might fit into?
                return 0;
            } else {
                return allocateBufferPointer(0, bitsToBytes(schema.maximumSizeBits()));
            }
        }

        private static void freeSharedBufferPointer(long bufferPtr) {
            if (bufferPtr != 0) {
                unsafe.freeMemory(bufferPtr);
            }
        }

        // INVARIANT: sz == schema.size(x)
        private <T> void fillBufferPointerFromSchema(Schema<T> schema, long bufferPtr, int sz, T x) {
            unsafe.putAddress(bufferPtr, sz);
            unsafe.putAddress(bufferPtr + Unsafe.ADDRESS_SIZE, bufferPtr + 2 * Unsafe.ADDRESS_SIZE);
            bs.initialize(bufferPtr + 2 * Unsafe.ADDRESS_SIZE, sz);
            schema.write(bs, x);
            bs.zeroFill(); // Very important to do this for keys but we skip it for values
        }

        private static long allocateBufferPointer(long bufferPtr, int sz) {
            if (bufferPtr != 0) {
                return bufferPtr;
            } else {
                return unsafe.allocateMemory(2 * Unsafe.ADDRESS_SIZE + sz);
            }
        }

        private static void freeBufferPointer(long bufferPtr, long bufferPtrNow) {
            if (bufferPtr == 0) {
                unsafe.freeMemory(bufferPtrNow);
            }
        }

        public Cursor<K, V> createCursor(Transaction tx) {
            final long[] cursorPtr = new long[1];
            Util.checkErrorCode(JNI.mdb_cursor_open(tx.txn, dbi, cursorPtr));
            return new Cursor<>(this, cursorPtr[0]);
        }

        public void close() {
            freeSharedBufferPointer(kBufferPtr);
            freeSharedBufferPointer(vBufferPtr);
            JNI.mdb_dbi_close(db.env, dbi);
        }

        public void put(Transaction tx, K k, V v) {
            final int kSz = bitsToBytes(kSchema.sizeBits(k));
            final int vSz = bitsToBytes(vSchema.sizeBits(v));

            final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
            fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
            final long vBufferPtrNow = allocateBufferPointer(vBufferPtr, vSz);
            unsafe.putAddress(vBufferPtrNow, vSz);
            try {
                Util.checkErrorCode(JNI.mdb_put_raw(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow, JNI.MDB_RESERVE));
                assert(unsafe.getAddress(vBufferPtrNow) == vSz);
                bs.initialize(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), vSz);
                vSchema.write(bs, v);
                bs.zeroFill();
            } finally {
                freeBufferPointer(vBufferPtr, vBufferPtrNow);
                freeBufferPointer(kBufferPtr, kBufferPtrNow);
            }
        }

        public boolean remove(Transaction tx, K k) {
            final int kSz = bitsToBytes(kSchema.sizeBits(k));

            final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
            fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
            try {
                int rc = JNI.mdb_del_raw(tx.txn, dbi, kBufferPtrNow, 0);
                if (rc == JNI.MDB_NOTFOUND) {
                    return false;
                } else {
                    Util.checkErrorCode(rc);
                    return true;
                }
            } finally {
                freeBufferPointer(kBufferPtr, kBufferPtrNow);
            }
        }

        public V get(Transaction tx, K k) {
            final int kSz = bitsToBytes(kSchema.sizeBits(k));

            final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
            fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
            final long vBufferPtrNow = allocateBufferPointer(vBufferPtr, 0);
            try {
                int rc = JNI.mdb_get_raw(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow);
                if (rc == JNI.MDB_NOTFOUND) {
                    return null;
                } else {
                    Util.checkErrorCode(rc);
                    bs.initialize(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(vBufferPtrNow));
                    return vSchema.read(bs);
                }
            } finally {
                freeBufferPointer(vBufferPtr, vBufferPtrNow);
                freeBufferPointer(kBufferPtr, kBufferPtrNow);
            }
        }

        public boolean contains(Transaction tx, K k) {
            final int kSz = bitsToBytes(kSchema.sizeBits(k));

            final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
            fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
            final long vBufferPtrNow = allocateBufferPointer(vBufferPtr, 0);
            try {
                int rc = JNI.mdb_get_raw(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow);
                if (rc == JNI.MDB_NOTFOUND) {
                    return false;
                } else {
                    Util.checkErrorCode(rc);
                    return true;
                }
            } finally {
                freeBufferPointer(vBufferPtr, vBufferPtrNow);
                freeBufferPointer(kBufferPtr, kBufferPtrNow);
            }
        }

        public Iterator<K> keys(Transaction tx) {
            final Cursor<K, V> cursor = createCursor(tx);
            final boolean initialHasNext = cursor.moveFirst();
            return new Iterator<K>() {
                boolean hasNext = initialHasNext;

                public boolean hasNext() {
                    return hasNext;
                }

                @Override
                public K next() {
                    if (!hasNext) throw new IllegalStateException("No more elements");

                    final K key = cursor.getKey();
                    hasNext = cursor.moveNext();
                    if (!hasNext) {
                        cursor.close();
                    }
                    return key;
                }
            };
        }

        public Iterator<Pair<K, V>> keyValues(Transaction tx) {
            final Cursor<K, V> cursor = createCursor(tx);
            final boolean initialHasNext = cursor.moveFirst();
            return new Iterator<Pair<K, V>>() {
                boolean hasNext = initialHasNext;

                public boolean hasNext() {
                    return hasNext;
                }

                @Override
                public Pair<K, V> next() {
                    if (!hasNext) throw new IllegalStateException("No more elements");

                    final Pair<K, V> pair = new Pair<>(cursor.getKey(), cursor.getValue());
                    hasNext = cursor.moveNext();
                    if (!hasNext) {
                        cursor.close();
                    }
                    return pair;
                }
            };
        }
    }

    private static class Pair<K, V> {
        final K k;
        final V v;

        public Pair(K k, V v) {
            this.k = k;
            this.v = v;
        }
    }

    // TODO: type specialisation for true 0-allocation? But we might hope that escape analysis would save us because our boxes are intermediate only.
    // TODO: duplicate item support
    static class Cursor<K, V> implements AutoCloseable {
        final Index<K, V> index;
        final long cursor;

        // Unlike the bufferPtrs in Index, it is important the the state of this var persists across calls:
        // it basically holds info about what the cursor is currently pointing to.
        //
        // If bufferPtrStale is true then the contents of this buffer aren't actually right, and you
        // will have to call move(JNI.MDB_GET_CURRENT) to correct this situation. An alternative to
        // having the bufferPtrStale flag would be to just call this eagerly whenever the buffer goes
        // stale, but I kind of like the idea of avoiding the JNI call (though TBH it doesn't seem to
        final long bufferPtr;
        boolean bufferPtrStale;

        public Cursor(Index<K, V> index, long cursor) {
            this.index = index;
            this.cursor = cursor;

            this.bufferPtr = unsafe.allocateMemory(4 * Unsafe.ADDRESS_SIZE);
        }

        private boolean isFound(int rc) {
            if (rc == JNI.MDB_NOTFOUND) {
                return false;
            } else {
                Util.checkErrorCode(rc);
                return true;
            }
        }

        private boolean move(int op) {
            boolean result = isFound(JNI.mdb_cursor_get_so_raw_it_hurts(cursor, bufferPtr, bufferPtr + 2 * Unsafe.ADDRESS_SIZE, op));
            bufferPtrStale = false;
            return result;
        }

        public boolean moveFirst()    { return move(JNI.MDB_FIRST); }
        public boolean moveLast()     { return move(JNI.MDB_LAST); }
        public boolean moveNext()     { return move(JNI.MDB_NEXT); }
        public boolean movePrevious() { return move(JNI.MDB_PREV); }

        boolean refresh() { return move(JNI.MDB_GET_CURRENT); }

        private boolean move(K k, int op) {
            final int kSz = bitsToBytes(index.kSchema.sizeBits(k));

            final long kBufferPtrNow = Index.allocateBufferPointer(index.kBufferPtr, kSz);
            index.fillBufferPointerFromSchema(index.kSchema, kBufferPtrNow, kSz, k);
            try {
                return isFound(JNI.mdb_cursor_get_so_raw_it_hurts(cursor, kBufferPtrNow, bufferPtr + 2 * Unsafe.ADDRESS_SIZE, op));
            } finally {
                // Need to copy the MDB_val from the temp structure to the permanent one, in case someone does getKey() now (they should get back k)
                unsafe.putAddress(bufferPtr,                       unsafe.getAddress(kBufferPtrNow));
                unsafe.putAddress(bufferPtr + Unsafe.ADDRESS_SIZE, unsafe.getAddress(kBufferPtrNow + Unsafe.ADDRESS_SIZE));
                bufferPtrStale = false;
                Index.freeBufferPointer(index.kBufferPtr, kBufferPtrNow);
            }
        }

        public boolean moveTo(K k)      { return move(k, JNI.MDB_SET_KEY); }
        public boolean moveCeiling(K k) { return move(k, JNI.MDB_SET_RANGE); }

        public boolean moveFloor(K k) {
            return (moveCeiling(k) && keyEquals(k)) || movePrevious();
        }

        private boolean keyEquals(K k) {
            if (bufferPtrStale) { refresh(); }

            final int kSz = bitsToBytes(index.kSchema.sizeBits(k));
            if (kSz != unsafe.getAddress(bufferPtr)) {
                return false;
            }

            final long kBufferPtrNow = Index.allocateBufferPointer(index.kBufferPtr, kSz);
            index.fillBufferPointerFromSchema(index.kSchema, kBufferPtrNow, kSz, k);
            try {
                final long ourKeyPtr   = unsafe.getAddress(bufferPtr + Unsafe.ADDRESS_SIZE);
                final long theirKeyPtr = kBufferPtrNow + 2 * Unsafe.ADDRESS_SIZE;
                for (int i = 0; i < kSz; i++) {
                    if (unsafe.getByte(ourKeyPtr + i) != unsafe.getByte(theirKeyPtr + i)) {
                        return false;
                    }
                }

                return true;
            } finally {
                Index.freeBufferPointer(index.kBufferPtr, kBufferPtrNow);
            }
        }

        public K getKey() {
            if (bufferPtrStale) { refresh(); }
            index.bs.initialize(unsafe.getAddress(bufferPtr + Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(bufferPtr));
            return index.kSchema.read(index.bs);
        }

        public V getValue() {
            if (bufferPtrStale) { refresh(); }
            index.bs.initialize(unsafe.getAddress(bufferPtr + 3 * Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(bufferPtr + 2 * Unsafe.ADDRESS_SIZE));
            return index.vSchema.read(index.bs);
        }

        public void put(V v) {
            if (bufferPtrStale) { refresh(); }

            final int vSz = bitsToBytes(index.vSchema.sizeBits(v));

            unsafe.putAddress(bufferPtr + 2 * Unsafe.ADDRESS_SIZE, vSz);
            Util.checkErrorCode(JNI.mdb_cursor_put_raw(cursor, bufferPtr, bufferPtr + 2 * Unsafe.ADDRESS_SIZE, JNI.MDB_CURRENT | JNI.MDB_RESERVE));
            index.bs.initialize(unsafe.getAddress(bufferPtr + 3 * Unsafe.ADDRESS_SIZE), vSz);
            index.vSchema.write(index.bs, v);
            index.bs.zeroFill();

            bufferPtrStale = false;
        }

        // This method has a lot in common with Index.put. LMDB actually just implements mdb_put using mdb_cursor_put, so this makes sense!
        public void put(K k, V v) {
            final int kSz = bitsToBytes(index.kSchema.sizeBits(k));
            final int vSz = bitsToBytes(index.vSchema.sizeBits(v));

            final long kBufferPtrNow = Index.allocateBufferPointer(index.kBufferPtr, kSz);
            index.fillBufferPointerFromSchema(index.kSchema, kBufferPtrNow, kSz, k);
            final long vBufferPtrNow = Index.allocateBufferPointer(index.vBufferPtr, vSz);
            unsafe.putAddress(vBufferPtrNow, vSz);
            try {
                Util.checkErrorCode(JNI.mdb_cursor_put_raw(cursor, kBufferPtrNow, vBufferPtrNow, JNI.MDB_RESERVE));
                index.bs.initialize(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), vSz);
                index.vSchema.write(index.bs, v);
            } finally {
                Index.freeBufferPointer(index.vBufferPtr, vBufferPtrNow);
                Index.freeBufferPointer(index.kBufferPtr, kBufferPtrNow);
            }

            bufferPtrStale = true;
        }

        public void delete() {
            Util.checkErrorCode(JNI.mdb_cursor_del(cursor, 0));

            bufferPtrStale = true;
        }

        public void close() {
            unsafe.freeMemory(bufferPtr);
            JNI.mdb_cursor_close(cursor);
        }
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
        public static Schema<FieldValue> INSTANCE = Schema.zipWith(new Latin1StringSchema(64), FieldValue::getValue,
                                                                   LocalDateSchema.INSTANCE,   FieldValue::getToDate,
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
                        if (fieldValue.getToDate().isAfter(date)) {
                            cursor.put(fieldValue.setToDate(date));
                        }
                    }
                } else {
                    final boolean mustCreate;
                    if (cursor.moveFloor(fieldKey) && cursor.getKey().idBBGlobal.equals(fieldKey.idBBGlobal)) {
                        final FieldValue fieldValue = cursor.getValue();
                        if (fieldValue.getToDate().isAfter(date)) {
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
                        cursor.put(fieldKey, new FieldValue(value, LocalDate.of(2999, 1, 1))); // TODO: nulls or ADTs instead of a dummy value?
                    }
                }
            }
        }

        long duration = System.nanoTime() - startTime;
        System.out.println("Loaded " + items + " in " + duration + "ns (" + (duration / items) + "ns/item)");
    }
}
