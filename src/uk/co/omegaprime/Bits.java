package uk.co.omegaprime;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

public class Bits {
    public static final Unsafe unsafe = getUnsafe();

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

    public static int   bigEndian(int x)   { return (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) ? x : Integer.reverseBytes(x); }
    public static short bigEndian(short x) { return (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) ? x : Short.reverseBytes(x); }
    public static long  bigEndian(long x)  { return (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) ? x : Long.reverseBytes(x); }

    public static int swapSign(int x) { return (x & 0x7FFFFFFF) | (~x & 0x80000000); }
    public static long swapSign(long x) { return (x & 0x7FFFFFFFFFFFFFFFl) | (~x & 0x8000000000000000l); }

    public static int bitsToBytes(int bits) {
        return bits / 8 + (bits % 8 != 0 ? 1 : 0);
    }
}
