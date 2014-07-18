package uk.co.omegaprime;

import org.junit.Test;

import static org.junit.Assert.*;

public class BitStream2Test {
    @Test
    public void muchTestWow() {
        final long ptr = Bits.unsafe.allocateMemory(16); // 128 bit budget

        {
            final BitStream2 bs = new BitStream2(ptr, 16);
            bs.putByte((byte)1); // 8 bits
            bs.putInt(1337);     // 40
            bs.deeper();         // 41
            bs.putInt(128);      // 74
            bs.putByte((byte)7); // 83
            bs.putEnd();         // 83
            bs.putInt(42);       // 115
            bs.deeper();         // 116
            bs.deeper();         // 116
            bs.deeper();         // 116
            bs.putByte((byte)2); // 125
            bs.putEnd();         // 126
            bs.putEnd();         // 127
            bs.putEnd();         // 127
        }

        {
            final BitStream2 bs = new BitStream2(ptr, 16);
            assertEquals(1, bs.getByte());
            assertEquals(1337, bs.getInt());
            bs.deeper();
            assertEquals(128, bs.getInt());
            assertEquals(7, bs.getByte());
            assertTrue(bs.tryGetEnd());
            assertEquals(42, bs.getInt());
            bs.deeper();
            bs.deeper();
            bs.deeper();
            assertEquals(2, bs.getByte());
            assertTrue(bs.tryGetEnd());
            assertTrue(bs.tryGetEnd());
            assertTrue(bs.tryGetEnd());

            // The end
            assertTrue(bs.tryGetEnd());
        }

        Bits.unsafe.freeMemory(ptr);
    }

    @Test
    public void deepTest() {
        final long ptr = Bits.unsafe.allocateMemory(6); // 48 bit budget

        {
            final BitStream2 bs = new BitStream2(ptr, 6);
            // Consumes 1 bit:
            for (int i = 0; i < 10; i++) {
                bs.deeper();
            }
            // Costs 33 bits:
            bs.putInt(100);
            // Consumes 9 bits:
            for (int i = 0; i < 10; i++) {
                bs.putEnd();
            }

            // Total consumption: 43 bits
            assertTrue(bs.tryGetEnd());
        }

        {
            final BitStream2 bs = new BitStream2(ptr, 6);
            for (int i = 0; i < 10; i++) {
                bs.deeper();
            }
            assertEquals(100, bs.getInt());
            for (int i = 0; i < 10; i++) {
                assertTrue(bs.tryGetEnd());
            }

            assertTrue(bs.tryGetEnd());
        }

        Bits.unsafe.freeMemory(ptr);
    }

    @Test
    public void testLongs() {
        final long ptr = Bits.unsafe.allocateMemory(33);

        {
            final BitStream2 bs = new BitStream2(ptr, 33);
            bs.putLong(1337);
            bs.putLong(-1337);
            bs.deeper();
            bs.putLong(100);
            bs.putLong(-100);
            bs.putEnd();
        }

        {
            final BitStream2 bs = new BitStream2(ptr, 33);
            assertEquals(1337, bs.getLong());
            assertEquals(-1337, bs.getLong());
            bs.deeper();
            assertEquals(100, bs.getLong());
            assertEquals(-100, bs.getLong());
            assertTrue(bs.tryGetEnd());
        }

        Bits.unsafe.freeMemory(ptr);
    }
}
