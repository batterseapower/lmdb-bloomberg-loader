package uk.co.omegaprime;

import au.com.bytecode.opencsv.CSVReader;
import org.fusesource.lmdbjni.Constants;
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

public class Loader {
    protected static final Unsafe unsafe = getUnsafe();

    @SuppressWarnings("restriction")
    private static Unsafe getUnsafe() {
        try {

            Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
            singleoneInstanceField.setAccessible(true);
            return (Unsafe) singleoneInstanceField.get(null);

        } catch (IllegalArgumentException | SecurityException | NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

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
        int flags         = 0; // JNI.MDB_WRITEMAP;

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
        public void setSync    (boolean enabled) { Util.checkErrorCode(JNI.mdb_env_set_flags(env, JNI.MDB_NOSYNC, enabled ? 0 : 1)); }
        public void setMapSync (boolean enabled) { Util.checkErrorCode(JNI.mdb_env_set_flags(env, JNI.MDB_MAPASYNC, enabled ? 0 : 1)); }

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
        static <T> T readFramed(Schema<T> schema, long ptr, int[] sizeOut) {
            if (schema.fixedSize() >= 0) {
                sizeOut[0] = schema.fixedSize();
                return schema.read(ptr, schema.fixedSize());
            } else {
                int sz = unsafe.getInt(ptr);
                sizeOut[0] = Integer.BYTES + sz;
                return schema.read(ptr + Integer.BYTES, sz);
            }
        }

        static <T> int writeFramed(Schema<T> schema, long ptr, T x) {
            if (schema.fixedSize() >= 0) {
                schema.write(ptr, schema.fixedSize(), x);
                return schema.fixedSize();
            } else {
                final int sz = schema.size(x);
                unsafe.putInt(ptr, sz);
                schema.write(ptr + Integer.BYTES, sz, x);
                return Integer.BYTES + sz;
            }
        }

        public static <T, U, V> Schema<V> zipWith(Schema<T> leftSchema, Function<V, T> leftProj, Schema<U> rightSchema, Function<V, U> rightProj, BiFunction<T, U, V> f) {
            return new Schema<V>() {
                @Override
                public V read(long ptr, int sz) {
                    int[] sizeOut = new int[] { 0 };
                    final T left = readFramed(leftSchema, ptr, sizeOut);
                    ptr += sizeOut[0];
                    final U right = rightSchema.read(ptr, sz - sizeOut[0]);
                    return f.apply(left, right);
                }

                @Override
                public int fixedSize() {
                    if (leftSchema.fixedSize() >= 0 && rightSchema.fixedSize() >= 0) {
                        return leftSchema.fixedSize() + rightSchema.fixedSize();
                    } else {
                        return -1;
                    }
                }

                @Override
                public int maximumSize() {
                    if (leftSchema.maximumSize() >= 0 && rightSchema.maximumSize() >= 0) {
                        return (leftSchema.fixedSize() >= 0 ? 0 : Integer.BYTES) + leftSchema.maximumSize() + rightSchema.maximumSize();
                    } else {
                        return -1;
                    }
                }

                @Override
                public int size(V x) {
                    return (leftSchema.fixedSize() >= 0 ? 0 : Integer.BYTES) + leftSchema.size(leftProj.apply(x)) + rightSchema.size(rightProj.apply(x));
                }

                @Override
                public void write(long ptr, int sz, V x) {
                    final int leftSize  = writeFramed(leftSchema, ptr, leftProj.apply(x));
                    rightSchema.write(ptr + leftSize, sz - leftSize, rightProj.apply(x));
                }
            };
        }

        T read(long ptr, int sz);
        int fixedSize();
        int maximumSize();
        int size(T x);
        void write(long ptr, int sz, T x);

        default <U> Schema<U> map(Function<U, T> f, Function<T, U> g) {
            final Schema<T> parent = this;
            return new Schema<U>() {
                public U read(long ptr, int sz) {
                    return g.apply(parent.read(ptr, sz));
                }

                public int fixedSize() {
                    return parent.fixedSize();
                }

                public int maximumSize() {
                    return parent.maximumSize();
                }

                public int size(U x) {
                    return parent.size(f.apply(x));
                }

                public void write(long ptr, int sz, U x) {
                    parent.write(ptr, sz, f.apply(x));
                }
            };
        }
    }

    static class IntegerSchema implements Schema<Integer> {
        public static IntegerSchema INSTANCE = new IntegerSchema();

        public Integer read(long ptr, int sz) { return unsafe.getInt(ptr); }
        public int fixedSize() { return Integer.BYTES; }
        public int maximumSize() { return fixedSize(); }
        public int size(Integer x) { return fixedSize(); }
        public void write(long ptr, int sz, Integer x) { unsafe.putInt(ptr, x); }
    }

    static class LongSchema implements Schema<Long> {
        public static LongSchema INSTANCE = new LongSchema();

        public Long read(long ptr, int sz) { return unsafe.getLong(ptr); }
        public int fixedSize() { return Long.BYTES; }
        public int maximumSize() { return fixedSize(); }
        public int size(Long x) { return fixedSize(); }
        public void write(long ptr, int sz, Long x) { unsafe.putLong(ptr, x); }
    }

    static class Latin1StringSchema implements Schema<String> {
        public static Latin1StringSchema INSTANCE = new Latin1StringSchema();

        private final int maximumLength;

        public Latin1StringSchema() { this(-1); }
        public Latin1StringSchema(int maximumLength) { this.maximumLength = maximumLength; }

        @Override
        public String read(long ptr, int sz) {
            final char[] cs = new char[sz];
            for (int i = 0; i < cs.length; i++) {
                cs[i] = (char)unsafe.getByte(ptr + sz);
            }
            return new String(cs);
        }

        @Override
        public int fixedSize() {
            return -1;
        }

        @Override
        public int maximumSize() {
            return maximumLength;
        }

        @Override
        public int size(String x) {
            return x.length();
        }

        @Override
        public void write(long ptr, int sz, String x) {
            if (maximumLength >= 0 && x.length() > maximumLength) {
                throw new IllegalArgumentException("Supplied string " + x + " would be truncated to maximum size of " + maximumLength + " chars");
            }

            for (int i = 0; i < x.length(); i++) {
                final char c = x.charAt(i);
                unsafe.putByte(ptr + i, (byte)((int)c > 255 ? c : '?'));
            }
        }

    }

    static class StringSchema implements Schema<String> {
        private static final Charset UTF8 = Charset.forName("UTF-8");

        public static StringSchema INSTANCE = new StringSchema();

        private final Charset charset;
        private final int maximumLength;

        public StringSchema() { this(UTF8); }
        public StringSchema(int maximumLength) { this(UTF8, maximumLength); }
        public StringSchema(Charset charset) { this(charset, -1); }
        public StringSchema(Charset charset, int maximumLength) {
            this.charset = charset;
            this.maximumLength = maximumLength;
        }

        public String read(long ptr, int sz) { return charset.decode(wrapPointer(ptr, sz)).toString(); }
        public int fixedSize() { return -1; }
        public int maximumSize() { return 4 * maximumLength; }
        public int size(String x) { return charset.encode(x).remaining(); }
        public void write(long ptr, int sz, String x) {
            if (maximumLength >= 0 && x.length() > maximumLength) {
                throw new IllegalArgumentException("Supplied string " + x + " would be truncated to maximum size of " + maximumLength + " chars");
            }
            wrapPointer(ptr, sz).put(charset.encode(x));
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

        public byte[] read(long ptr, int sz) {
            final byte[] bs = new byte[sz];
            for (int i = 0; i < sz; i++) bs[i] = unsafe.getByte(ptr + i);
            return bs;
        }

        public int fixedSize() { return -1; }
        public int maximumSize() { return -1; }
        public int size(byte[] x) { return x.length; }

        public void write(long ptr, int sz, byte[] x) {
            for (int i = 0; i < x.length; i++) {
                unsafe.putByte(ptr + i, x[i]);
            }
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

        Index(Database db, long dbi, Schema<K> kSchema, Schema<V> vSchema) {
            this.db = db;
            this.dbi = dbi;
            this.kSchema = kSchema;
            this.vSchema = vSchema;

            this.kBufferPtr = allocateSharedBufferPointer(kSchema);
            this.vBufferPtr = allocateSharedBufferPointer(vSchema);
        }

        private static <T> long allocateSharedBufferPointer(Schema<T> schema) {
            if (schema.maximumSize() < 0) {
                // TODO: speculatively allocate a reasonable amount of memory that most allocations of interest might fit into?
                return 0;
            } else {
                long bufferPtr = allocateBufferPointer(0, schema.maximumSize());
                if (schema.fixedSize() >= 0) {
                    unsafe.putAddress(bufferPtr, schema.fixedSize());
                }
                return bufferPtr;
            }
        }

        private static void freeSharedBufferPointer(long bufferPtr) {
            if (bufferPtr != 0) {
                unsafe.freeMemory(bufferPtr);
            }
        }

        private static <T> void fillBufferPointerSizeFromSchema(Schema<T> schema, long bufferPtr, int sz) {
            if (schema.fixedSize() < 0) {
                unsafe.putAddress(bufferPtr, sz);
            }
        }

        // INVARIANT: sz == schema.size(x)
        private static <T> void fillBufferPointerFromSchema(Schema<T> schema, long bufferPtr, int sz, T x) {
            fillBufferPointerSizeFromSchema(schema, bufferPtr, sz);
            schema.write(bufferPtr + 2 * Unsafe.ADDRESS_SIZE, sz, x);
        }

        private static long allocateBufferPointer(long bufferPtr, int sz) {
            if (bufferPtr != 0) {
                return bufferPtr;
            } else {
                final long bufferPtrNow = unsafe.allocateMemory(2 * Unsafe.ADDRESS_SIZE + sz);
                unsafe.putAddress(bufferPtrNow + Unsafe.ADDRESS_SIZE, bufferPtrNow + 2 * Unsafe.ADDRESS_SIZE);
                return bufferPtrNow;
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
            return new Cursor<K, V>(this, cursorPtr[0]);
        }

        public void close() {
            freeSharedBufferPointer(kBufferPtr);
            freeSharedBufferPointer(vBufferPtr);
            JNI.mdb_dbi_close(db.env, dbi);
        }

        public void put(Transaction tx, K k, V v) {
            final int kSz = kSchema.size(k);
            final int vSz = vSchema.size(v);

            final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
            fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
            final long vBufferPtrNow = allocateBufferPointer(vBufferPtr, vSz);
            fillBufferPointerSizeFromSchema(vSchema, vBufferPtrNow, vSz);
            try {
                Util.checkErrorCode(JNI.mdb_put_raw(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow, JNI.MDB_RESERVE));
                vSchema.write(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), vSz, v);
            } finally {
                freeBufferPointer(vBufferPtr, vBufferPtrNow);
                freeBufferPointer(kBufferPtr, kBufferPtrNow);
            }
        }

        public boolean remove(Transaction tx, K k) {
            final int kSz = kSchema.size(k);

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
            final int kSz = kSchema.size(k);

            final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
            fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
            final long vBufferPtrNow = allocateBufferPointer(vBufferPtr, 0);
            try {
                int rc = JNI.mdb_get_raw(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow);
                if (rc == JNI.MDB_NOTFOUND) {
                    return null;
                } else {
                    Util.checkErrorCode(rc);
                    return vSchema.read(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(vBufferPtrNow));
                }
            } finally {
                freeBufferPointer(vBufferPtr, vBufferPtrNow);
                freeBufferPointer(kBufferPtr, kBufferPtrNow);
            }
        }

        public boolean contains(Transaction tx, K k) {
            final int kSz = kSchema.size(k);

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

        public Iterator<Pair<K, V>> keyValues(Transaction tx) {
            final Cursor<K, V> cursor = createCursor(tx);
            boolean initialAtEnd = cursor.moveFirst();
            return new Iterator<Pair<K, V>>() {
                boolean atEnd = initialAtEnd;

                public boolean hasNext() {
                    return !atEnd;
                }

                @Override
                public Pair<K, V> next() {
                    if (atEnd) throw new IllegalStateException("No more elements");

                    final Pair<K, V> pair = new Pair<>(cursor.getKey(), cursor.getValue());
                    atEnd = cursor.moveNext();
                    if (atEnd) {
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

    // TODO: duplicate item support
    static class Cursor<K, V> {
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

        private boolean move(K k, int op) {
            final int kSz = index.kSchema.size(k);

            final long kBufferPtrNow = Index.allocateBufferPointer(index.kBufferPtr, kSz);
            Index.fillBufferPointerFromSchema(index.kSchema, kBufferPtrNow, kSz, k);
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

        public K getKey() {
            if (bufferPtrStale) { move(JNI.MDB_GET_CURRENT); }
            return index.kSchema.read(unsafe.getAddress(bufferPtr + Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(bufferPtr));
        }

        public V getValue() {
            if (bufferPtrStale) { move(JNI.MDB_GET_CURRENT); }
            return index.vSchema.read(unsafe.getAddress(bufferPtr + 3 * Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(bufferPtr + 2 * Unsafe.ADDRESS_SIZE));
        }

        public void put(V v) {
            final int vSz = index.vSchema.size(v);

            unsafe.putAddress(bufferPtr + 2 * Unsafe.ADDRESS_SIZE, vSz);
            Util.checkErrorCode(JNI.mdb_cursor_put_raw(cursor, 0, bufferPtr + 2 * Unsafe.ADDRESS_SIZE, JNI.MDB_CURRENT | JNI.MDB_RESERVE));
            index.vSchema.write(unsafe.getAddress(bufferPtr + 3 * Unsafe.ADDRESS_SIZE), vSz, v);

            bufferPtrStale = false;
        }

        // This method has a lot in common with Index.put. LMDB actually just implements mdb_put using mdb_cursor_put, so this makes sense!
        public void put(K k, V v) {
            final int kSz = index.kSchema.size(k);
            final int vSz = index.vSchema.size(v);

            final long kBufferPtrNow = Index.allocateBufferPointer(index.kBufferPtr, kSz);
            Index.fillBufferPointerFromSchema(index.kSchema, kBufferPtrNow, kSz, k);
            final long vBufferPtrNow = Index.allocateBufferPointer(index.vBufferPtr, vSz);
            Index.fillBufferPointerSizeFromSchema(index.vSchema, vBufferPtrNow, vSz);
            try {
                Util.checkErrorCode(JNI.mdb_cursor_put_raw(cursor, kBufferPtrNow, vBufferPtrNow, JNI.MDB_RESERVE));
                index.vSchema.write(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), vSz, v);
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



        try (final Database db = new Database(dbDirectory, new DatabaseOptions().maxIndexes(40).mapSize(1_073_741_824))) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<Integer, String> index = db.createIndex(tx, "Test", IntegerSchema.INSTANCE, StringSchema.INSTANCE);

                index.put(tx, 1, "Hello");
                index.put(tx, 2, "World");
                index.put(tx, 3, "!");

                String got = index.get(tx, 2);
                if (!"World".equals(got)) {
                    throw new IllegalStateException("WTF? '" + got + "'");
                }

                tx.commit();
            }
        }

        System.exit(1);

        try (final Database db = new Database(dbDirectory, new DatabaseOptions().maxIndexes(40).mapSize(1_073_741_824))) {
            try (final Transaction tx = db.transaction(false)) {
                final Index<File, Source> sourcesIndex = db.<File, Source>createIndex(tx, "Sources", StringSchema.INSTANCE.map(File::getAbsolutePath, File::new),
                                                                                                     InstantSchema.INSTANCE_SECOND_RESOLUTION.map(Source::getInstant, Source::new));
                final Iterator<File> it = availableFiles(new File("/Users/mbolingbroke/Downloads"));
                while (it.hasNext()) {
                    final File file = it.next();
                    if (!sourcesIndex.contains(tx, file)) {
                        load(db, tx, file);
                        sourcesIndex.put(tx, file, new Source(Instant.now()));
                    }
                }

                tx.commit();
            }
            db.sync(true);
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

    final static Pattern FILENAME_REGEX = Pattern.compile("Equity_Common_Stock_([0-9]+)[.]txt[.]zip");
    final static DateTimeFormatter FILENAME_DTF = DateTimeFormatter.ofPattern("yyyyMMdd");

    public static void load(Database db, Transaction tx, File file) throws IOException {
        final Matcher m = FILENAME_REGEX.matcher(file.getName());
        if (!m.matches()) throw new IllegalStateException("Supplied file name " + file.getName() + " did not match expected pattern");

        final String dateString = m.group(1);
        final LocalDate date = LocalDate.parse(dateString, FILENAME_DTF);

        try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                System.out.println("Loading " + file + ":" + entry);
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
                    throw new IllegalArgumentException("File " + file + " does not have ID_BB_GLOBAL field");
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

                        //final Index<FieldKey, String> index = indexes[i];
                        final Cursor<FieldKey, String> cursor = cursors[i];
                        final String value = line[i].trim();

                        items++;

                        if (value.length() == 0) {
                            //index.remove(tx, key);
                            if (cursor.moveTo(key)) {
                                cursor.delete();
                            }
                        } else {
                            //index.put(tx, key, value);
                            cursor.put(key, value);
                        }
                    }
                }

                long duration = System.nanoTime() - startTime;
                System.out.println("Loaded " + items + " in " + duration + "ns (" + (duration / items) + "ns/item)");
            }
        }
    }
}
