package com.linkedin.thirdeye.dataframe;

import org.testng.Assert;
import org.testng.annotations.Test;


public class JoinMapTest {

  @Test
  public void testLog2() {
    Assert.assertEquals(JoinHashMap.log2(0), 0);
    Assert.assertEquals(JoinHashMap.log2(1), 0);
    Assert.assertEquals(JoinHashMap.log2(2), 1);
    Assert.assertEquals(JoinHashMap.log2(3), 1);
    Assert.assertEquals(JoinHashMap.log2(4), 2);
    Assert.assertEquals(JoinHashMap.log2(5), 2);
    Assert.assertEquals(JoinHashMap.log2(6), 2);
    Assert.assertEquals(JoinHashMap.log2(7), 2);
    Assert.assertEquals(JoinHashMap.log2(8), 3);
  }

  @Test
  public void testPow2() {
    Assert.assertEquals(JoinHashMap.pow2(0), 1);
    Assert.assertEquals(JoinHashMap.pow2(1), 2);
    Assert.assertEquals(JoinHashMap.pow2(2), 4);
    Assert.assertEquals(JoinHashMap.pow2(3), 8);
    Assert.assertEquals(JoinHashMap.pow2(4), 16);
  }

  @Test
  public void testSizing() {
    Assert.assertEquals(new JoinHashMap(0).size(), 2);
    Assert.assertEquals(new JoinHashMap(1).size(), 2);
    Assert.assertEquals(new JoinHashMap(2).size(), 4);
    Assert.assertEquals(new JoinHashMap(3).size(), 4);
    Assert.assertEquals(new JoinHashMap(4).size(), 8);
    Assert.assertEquals(new JoinHashMap(5).size(), 8);
    Assert.assertEquals(new JoinHashMap(6).size(), 8);
    Assert.assertEquals(new JoinHashMap(7).size(), 16); // scaling factor
    Assert.assertEquals(new JoinHashMap(8).size(), 16);
    Assert.assertEquals(new JoinHashMap(9).size(), 16);
  }

  @Test
  public void testTupleDecomposition() {
    final long tuple = 0x1234567899990000L;
    Assert.assertEquals(JoinHashMap.tuple2key(tuple), 0x12345678);
    Assert.assertEquals(JoinHashMap.tuple2val(tuple), 0x99990000);
  }

  @Test
  public void testTupleComposition() {
    Assert.assertEquals(JoinHashMap.tuple(0x12345678, 0x79999999), 0x1234567879999999L);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testValueUpperBound() {
    final JoinHashMap m = new JoinHashMap(0);
    m.put(0xFFFFFFFF, 0x80000000);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testValueLowerBound() {
    final JoinHashMap m = new JoinHashMap(0);
    m.put(0xFFFFFFFF, -1);
  }

  @Test
  public void testBasic() {
    JoinHashMap m = new JoinHashMap(4);
    m.put(12345, 54321);
    Assert.assertEquals(m.get(12345, 0), 54321);
  }

  @Test
  public void testMultiple() {
    JoinHashMap m = new JoinHashMap(4);
    m.put(0, 99);
    m.put(1, 98);
    m.put(2, 97);
    m.put(3, 96);

    Assert.assertEquals(m.get(0, 0), 99);
    Assert.assertEquals(m.get(1, 0), 98);
    Assert.assertEquals(m.get(2, 0), 97);
    Assert.assertEquals(m.get(3, 0), 96);
  }

  @Test
  public void testMultiSet() {
    JoinHashMap m = new JoinHashMap(8);
    m.put(0, 99);
    m.put(1, 98);
    m.put(3, 97);
    m.put(1, 96);
    m.put(0, 95);
    m.put(0, 94);
    m.put(3, 93);
    m.put(2, 92);

    Assert.assertEquals(m.get(0, 0), 99);
    Assert.assertEquals(m.get(0, 1), 95);
    Assert.assertEquals(m.get(0, 2), 94);
    Assert.assertEquals(m.get(0, 3), -1);
    Assert.assertEquals(m.get(1, 0), 98);
    Assert.assertEquals(m.get(1, 1), 96);
    Assert.assertEquals(m.get(1, 2), -1);
    Assert.assertEquals(m.get(2, 0), 92);
    Assert.assertEquals(m.get(2, 1), -1);
    Assert.assertEquals(m.get(3, 0), 97);
    Assert.assertEquals(m.get(3, 1), 93);
    Assert.assertEquals(m.get(3, 2), -1);
  }
}
