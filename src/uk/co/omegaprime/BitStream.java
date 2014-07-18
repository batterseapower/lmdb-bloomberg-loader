package uk.co.omegaprime;

import static uk.co.omegaprime.Bits.*;

// Unfortunately this is a stupid thing to do. Consider a composite primary key of three
// elements: A, B and C. Now consider the cost of encoding them as ((A, B), C) and
// (A, (B, C)) using this library:
//
// len(A) | len(B) | len(C) | Cost (bits), left nested  | Cost (bits), right nested
// 0        0        N                2 +         1 + N           1 +         1 + N
// 0        1        N                2 + 1 + 8 + 1 + N           1 + 1 + 8 + 1 + N
// 1        0        N        2 + 8 + 2 +         1 + N   1 + 8 + 1 +         1 + N
// 1        1        N        2 + 8 + 2 + 1 + 8 + 1 + N   1 + 8 + 1 + 1 + 8 + 1 + N
//
// Right nested is at least no worse than left nested, so the whole variable depth idea
// of this class is actually harmful.
//
// For this reason I moved to BitStream2 where there is at most 1 bit of waste per byte.
public class BitStream {
    // INVARIANT: ptr <= endPtr
    private long ptr;
    private long endPtr;
    // bitOffset points to the first bit *after* the waste that comes before the next byte (if any)
    // INVARIANT: 0 <= bitOffset < 8
    private byte bitOffset = 0;
    // INVARIANT: 0 <= wasteBitCount
    private byte wasteBitCount = 0;

    public BitStream(long ptr, long bytes) {
        this.ptr    = ptr;
        this.endPtr = ptr + bytes;
    }

    public void deeper() {
        wasteBitCount += 1;
        if (bitOffset < 7) {
            bitOffset++;
        } else {
            bitOffset = 0;
            ptr += 1;
        }
    }

    // May only advance the read pointer if it returns true: if this returns false then the read pointer is guaranteed in the same place as before
    public boolean tryGetShallower() {
        if (wasteBitCount == 0) {
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
                wasteBitCount -= 1;
                advance(0);
                return true;
            } else {
                return false;
            }
        }
    }

    public void putShallower() {
        // TODO: support going shallower in multiple levels simultaneously, as on optimisation?
        writeWaste(false);
        wasteBitCount -= 1;
        advance(0);
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
        if (wasteBitCount == 0) {
            return;
        }

        // Fill the partial byte at the start of the waste (if any) with 1s. Note that for the purposes of this block
        // the bits we want to write in the partial byte must directly abut the end of the byte.
        if ((bitOffset - wasteBitCount) % 8 < 0) {
            int bitCount = (wasteBitCount - bitOffset) % 8;
            int byteOff = (bitOffset - wasteBitCount) / 8;
            // INVARIANT: byteOff > 0
            int mask = ~(0xFF << bitCount);
            Bits.unsafe.putByte(ptr - byteOff, (byte)(Bits.unsafe.getByte(ptr - byteOff) | mask));
        }

        // Fill the partial byte at the end of the waste with 1s. Unlike the previous case, in this block we are
        // prepared to deal with writing bit ranges in the middle of a byte.
        if (bitOffset > 0) {
            int mask = ((1 << wasteBitCount) - 1) << 8 - bitOffset;
            Bits.unsafe.putByte(ptr, (byte)(Bits.unsafe.getByte(ptr) | mask));
        }

        // Fill the full bytes in between the start and the end with 1s.
        // FIXME: I strongly suspect this is wrong
        int middleByteCount = (wasteBitCount - bitOffset) / 8;
        unsafe.setMemory(ptr - middleByteCount, middleByteCount, (byte) 0xFF);

        if (!continues) {
            // Flip the final bit in the waste
            if (bitOffset == 0) {
                Bits.unsafe.putByte(ptr - 1, (byte)(Bits.unsafe.getByte(ptr - 1) & 0xFE));
            } else {
                Bits.unsafe.putByte(ptr, (byte)(Bits.unsafe.getByte(ptr) & ~(1 << 8 - bitOffset)));
            }
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
        Bits.unsafe.putLong(ptr, bigEndian(cleared | ((long)x << (32 - bitOffset))));
        advance(4);
    }

    public void advance(int nBytes) {
        int newBitOffset = bitOffset + nBytes * 8 + wasteBitCount;
        ptr += newBitOffset / 8;
        bitOffset = (byte)(newBitOffset % 8);
    }
}
