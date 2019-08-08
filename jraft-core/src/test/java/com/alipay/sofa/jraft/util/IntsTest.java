package com.alipay.sofa.jraft.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IntsTest {

  @Test
  public void testCheckedCast() {
    assertEquals(7, Ints.checkedCast(7L));
  }

  @Test
  public void testSaturatedCast() {
    assertEquals(3, Ints.saturatedCast(3L));
    assertEquals(2_147_483_647, Ints.saturatedCast(144_115_190_223_339_520L));
    assertEquals(-2_147_483_648, Ints.saturatedCast(-9_079_256_846_631_436_288L));
  }

  @Test
  public void testFindNextPositivePowerOfTwo() {
    assertEquals(4, Ints.findNextPositivePowerOfTwo(3));
    assertEquals(1, Ints.findNextPositivePowerOfTwo(-99));
    assertEquals(1_073_741_824, Ints.findNextPositivePowerOfTwo(1_107_296_256));
  }

  @Test
  public void testRoundToPowerOfTwo() {
    assertEquals(1_048_576, Ints.roundToPowerOfTwo(524_289));
  }

  @Test
  public void testIsPowerOfTwo() {
    assertTrue(Ints.isPowerOfTwo(2));

    assertFalse(Ints.isPowerOfTwo(3));
  }
}
