package uk.co.omegaprime;

import org.junit.Test;

import static org.junit.Assert.*;

public class BitStreamTest {
    @Test
    public void muchTestWow() {
        final long ptr = Bits.unsafe.allocateMemory(32); // 256 bit budget

        {
            final BitStream bs = new BitStream(ptr, 32);
            bs.putByte((byte)1); // 8 bits
            bs.putInt(1337);     // 40
            bs.deeper();         // 40
            bs.putInt(128);      // 73  (waste 1)
            bs.putByte((byte)7); // 82  (waste 2)
            bs.putShallower();   // 83  (waste 3)
            bs.putInt(42);       // 115 (waste 3)
            bs.deeper();         // 115 (waste 3)
            bs.deeper();         // 115 (waste 3)
            bs.deeper();         // 115 (waste 3)
            bs.putByte((byte)2); // 126 (waste 6)
            bs.putShallower();   // 129 (waste 9)
            bs.putShallower();   // 131 (waste 11)
            bs.putShallower();   // 132 (waste 12)
        }

        {
            final BitStream bs = new BitStream(ptr, 17);
            assertEquals(1, bs.getByte());
            assertEquals(1337, bs.getInt());
            bs.deeper();
            assertEquals(128, bs.getInt());
            assertEquals(7, bs.getByte());
            assertTrue(bs.tryGetShallower());
            assertEquals(42, bs.getInt());
            bs.deeper();
            bs.deeper();
            bs.deeper();
            assertEquals(2, bs.getByte());
            assertTrue(bs.tryGetShallower());
            assertTrue(bs.tryGetShallower());
            assertTrue(bs.tryGetShallower());

            // The end
            assertTrue(bs.tryGetShallower());
        }

        Bits.unsafe.freeMemory(ptr);
    }

    @Test
    public void deepTest() {
        final long ptr = Bits.unsafe.allocateMemory(13); // 104 bit budget

        {
            final BitStream bs = new BitStream(ptr, 13);
            for (int i = 0; i < 10; i++) {
                bs.deeper();
            }
            // Pay 10 + 32 bits for this:
            bs.putInt(100);
            // Pay 10 + 9 + 8 + ... + 2 + 1 bits for this loop:
            for (int i = 0; i < 10; i++) {
                bs.putShallower();
            }

            // Total cost == 97 bits, so we are now pointing somewhere in the 13th byte
            assertTrue(bs.tryGetShallower());
        }

        {
            final BitStream bs = new BitStream(ptr, 13);
            for (int i = 0; i < 10; i++) {
                bs.deeper();
            }
            assertEquals(100, bs.getInt());
            for (int i = 0; i < 10; i++) {
                assertTrue(bs.tryGetShallower());
            }

            assertTrue(bs.tryGetShallower());
        }

        Bits.unsafe.freeMemory(ptr);
    }
}
