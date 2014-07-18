package uk.co.omegaprime;

import static uk.co.omegaprime.Bits.bigEndian;
import static uk.co.omegaprime.Bits.unsafe;

public class BitStream2 {
    private long ptr;
    private long endPtr;
    private byte bitOffset = 0;
    private int depth = 0;

    public BitStream2(long ptr, int sz) {
        this.ptr = ptr;
        this.endPtr = ptr + sz;
    }

    public void deeper() {
        if (depth == 0) {
            depth = 1;
            advance(0);
        } else {
            depth += 1;
        }
    }

    public boolean tryGetEnd() {
        if (depth == 0) {
            // After writing a stream we do not necessarily end up on a byte boundary, even if we started on one.
            // This method still returns true iff we at the end of the stream, so long as we don't write to the
            // bitstream in units of less than 8 bits.
            return bitOffset == 0 ? ptr == endPtr : ptr + 1 == endPtr;
        } else {
            final boolean isEnd;
            if (bitOffset == 0) {
                isEnd = (Bits.unsafe.getByte(ptr - 1) & 1) == 0;
            } else {
                isEnd = ((Bits.unsafe.getByte(ptr) << (bitOffset - 1)) & 0x80) == 0;
            }

            if (isEnd) {
                depth -= 1;
                advance(0);
            }
            return isEnd;
        }
    }

    // This can be implemented by the clients in the terms of other methods, but it is universally handy
    public int bytesToEnd() {
        if (depth == 0) {
            return (int)(endPtr - ptr - (bitOffset == 0 ? 0 : 1));
        } else {
            int count = -1;
            boolean isEnd;
            long ptrPrime = ptr;
            byte bitOffsetPrime = bitOffset;
            do {
                count++;

                if (bitOffsetPrime == 0) {
                    isEnd = (Bits.unsafe.getByte(ptrPrime - 1) & 1) == 0;
                } else {
                    isEnd = ((Bits.unsafe.getByte(ptrPrime) << (bitOffsetPrime - 1)) & 0x80) == 0;
                }

                if (bitOffsetPrime == 7) {
                    bitOffsetPrime = 0;
                    ptrPrime += 2;
                } else {
                    bitOffsetPrime++;
                    ptrPrime++;
                }
            } while (!isEnd);

            return count;
        }
    }

    public byte getByte() {
        short x = bigEndian(Bits.unsafe.getShort(ptr));
        byte result = (byte)((x << bitOffset) >> 8);
        advance(1);
        return result;
    }

    public int getInt() {
        long x = bigEndian(Bits.unsafe.getLong(ptr));
        int result = (int)((x << bitOffset) >> 32);
        advance(4);
        return result;
    }

    public void writeWaste(boolean continues) {
        if (depth == 0) {
            return;
        }

        if (bitOffset == 0) {
            Bits.unsafe.putByte(ptr - 1, continues ? (byte)(Bits.unsafe.getByte(ptr - 1) | 0x01)
                                                   : (byte)(Bits.unsafe.getByte(ptr - 1) & 0xFE));
        } else {
            Bits.unsafe.putByte(ptr, continues ? (byte)(Bits.unsafe.getByte(ptr) |  (1 << 8 - bitOffset))
                                               : (byte)(Bits.unsafe.getByte(ptr) & ~(1 << 8 - bitOffset)));
        }
    }

    public void putEnd() {
        writeWaste(false);
        depth -= 1;
        advance(0);
    }

    public void putByte(byte x) {
        writeWaste(true);
        final int mask = 0xFF << (8 - bitOffset);
        int cleared = bigEndian(Bits.unsafe.getShort(ptr)) & ~mask;
        Bits.unsafe.putShort(ptr, bigEndian((short)(cleared | (x << (8 - bitOffset)))));
        advance(1);
    }

    public void putInt(int x) {
        writeWaste(true);
        final long mask = 0xFFFFFFFFl << (32 - bitOffset);
        long cleared = bigEndian(Bits.unsafe.getLong(ptr)) & ~mask;
        Bits.unsafe.putLong(ptr, bigEndian(cleared | ((long)x << (32 - bitOffset))));
        advance(4);
    }

    public void advance(int nBytes) {
        int newBitOffset = bitOffset + nBytes * 8 + (depth > 0 ? 1 : 0);
        ptr += newBitOffset / 8;
        bitOffset = (byte)(newBitOffset % 8);
    }
}
