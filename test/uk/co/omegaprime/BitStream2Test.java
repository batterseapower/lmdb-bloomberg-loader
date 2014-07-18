package uk.co.omegaprime;

import org.junit.Test;

import static org.junit.Assert.*;

public class BitStream2Test {
    @Test
    public void muchTestWow() {
        final long ptr = Bits.unsafe.allocateMemory(32); // 256 bit budget

        {
            final BitStream2 bs = new BitStream2(ptr, 32);
            bs.putByte((byte)1);
            bs.putInt(1337);
            bs.putInt(128);
            bs.putByte((byte)7);
            bs.putEnd();
            bs.putInt(42);
            bs.putByte((byte)2);
            bs.putEnd();
            bs.putEnd();
            bs.putEnd();
        }

        {
            final BitStream2 bs = new BitStream2(ptr, 17);
            assertEquals(1, bs.getByte());
            assertEquals(1337, bs.getInt());
            assertEquals(128, bs.getInt());
            assertEquals(7, bs.getByte());
            assertTrue(bs.tryGetEnd());
            assertEquals(42, bs.getInt());
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
        final long ptr = Bits.unsafe.allocateMemory(13); // 104 bit budget

        {
            final BitStream2 bs = new BitStream2(ptr, 13);
            bs.putInt(100);
            for (int i = 0; i < 10; i++) {
                bs.putEnd();
            }

            assertTrue(bs.tryGetEnd());
        }

        {
            final BitStream2 bs = new BitStream2(ptr, 13);
            assertEquals(100, bs.getInt());
            for (int i = 0; i < 10; i++) {
                assertTrue(bs.tryGetEnd());
            }

            assertTrue(bs.tryGetEnd());
        }

        Bits.unsafe.freeMemory(ptr);
    }
}
