package uk.co.omegaprime;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BitsTest {
    @Test
    public void canSwapSign() {
        assertEquals(0x4AFEBABE,          Bits.swapSign(0xCAFEBABE));
        assertEquals(0x4AFEBABEDEADBEEFl, Bits.swapSign(0xCAFEBABEDEADBEEFl));
    }
}
