package uk.co.omegaprime;

import static uk.co.omegaprime.Bits.bigEndian;

public class BitStream2 {
    private long ptr;
    private long endPtr;
    private byte bitOffset;
    private int depth;

    public BitStream2() {
        this(0, 0);
    }

    public BitStream2(long ptr, int sz) {
        initialize(ptr, sz);
    }

    public void initialize(long ptr, int sz) {
        this.ptr = ptr;
        this.endPtr = ptr + sz;
        this.bitOffset = 0;
        this.depth = 0;
    }

    // Supply the number of bytes that will be written to the stream after the corresponding putEnd
    // or will be read from the stream after the corresponding tryGetEnd.
    public void deeper(int trailingDataBytes) {
        if (trailingDataBytes >= 0) {
            if (depth > 0) {
                throw new IllegalStateException("We were told that there were " + trailingDataBytes + " bytes guaranteed after this variable length block, but the nested section we are in was not.. something is amiss");
            }
        } else if (depth == 0) {
            depth = 1;
            advance(0);
        } else {
            depth += 1;
        }
    }

    // Supply the number of bytes that will be read from the stream after this point.
    public boolean tryGetEnd(int trailingDataBytes) {
        if (depth == 0) {
            final long blockEndPtr = trailingDataBytes >= 0 ? endPtr - trailingDataBytes : endPtr;
            // After writing a stream we do not necessarily end up on a byte boundary, even if we started on one.
            // This method still returns true iff we at the end of the stream, so long as we don't write to the
            // bitstream in units of less than 8 bits.
            return bitOffset == 0 ? ptr == blockEndPtr : ptr + 1 == blockEndPtr;
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
    // Supply the number of bytes that will be read from the stream after the corresponding tryGetEnd.
    public int bytesToEnd(int trailingDataBytes) {
        if (depth == 0) {
            final long blockEndPtr = trailingDataBytes >= 0 ? endPtr - trailingDataBytes : endPtr;
            return (int)(blockEndPtr - ptr - (bitOffset == 0 ? 0 : 1));
        } else {
            int count = -1;
            boolean isEnd;
            long ptrNew = ptr;
            byte bitOffsetNew = bitOffset;
            do {
                count++;

                if (bitOffsetNew == 0) {
                    isEnd = (Bits.unsafe.getByte(ptrNew - 1) & 1) == 0;
                } else {
                    isEnd = ((Bits.unsafe.getByte(ptrNew) << (bitOffsetNew - 1)) & 0x80) == 0;
                }

                if (bitOffsetNew == 7) {
                    bitOffsetNew = 0;
                    ptrNew += 2;
                } else {
                    bitOffsetNew++;
                    ptrNew++;
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

    public long getLong() {
        long x0 = bigEndian(Bits.unsafe.getLong(ptr));
        long result0 = ((x0 << bitOffset) >>> 32);
        long x1 = bigEndian(Bits.unsafe.getLong(ptr + 4));
        long result1 = ((x1 << bitOffset) >>> 32);
        advance(8);
        return (result0 << 32) | result1;
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

    // Supply the number of bytes that will be put to the stream after this point (or negative if unknown)
    public void putEnd(int trailingDataBytes) {
        if (trailingDataBytes < 0) {
            writeWaste(false);
            depth -= 1;
            advance(0);
        }
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
        Bits.unsafe.putLong(ptr, bigEndian(cleared | ((x & 0xFFFFFFFFl) << (32 - bitOffset))));
        advance(4);
    }

    public void putLong(long x) {
        writeWaste(true);
        // Fake it by doing two 32-bit writes:
        final long mask = 0xFFFFFFFFl << (32 - bitOffset);
        {
            long cleared = bigEndian(Bits.unsafe.getLong(ptr)) & ~mask;
            Bits.unsafe.putLong(ptr, bigEndian(cleared | ((x >>> 32) << (32 - bitOffset))));
        }
        {
            long cleared = bigEndian(Bits.unsafe.getLong(ptr + 4)) & ~mask;
            Bits.unsafe.putLong(ptr + 4, bigEndian(cleared | ((x & 0xFFFFFFFFl) << (32 - bitOffset))));
        }
        advance(8);
    }

    public void advance(int nBytes) {
        int newBitOffset = bitOffset + nBytes * 8 + (depth > 0 ? 1 : 0);
        ptr += newBitOffset / 8;
        bitOffset = (byte)(newBitOffset % 8);
    }
}
