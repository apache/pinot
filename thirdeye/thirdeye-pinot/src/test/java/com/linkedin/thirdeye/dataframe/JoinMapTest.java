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
    Assert.assertEquals(new JoinHashMap(1).size(), 4);
    Assert.assertEquals(new JoinHashMap(2).size(), 8);
    Assert.assertEquals(new JoinHashMap(3).size(), 8);
    Assert.assertEquals(new JoinHashMap(4).size(), 16);
    Assert.assertEquals(new JoinHashMap(5).size(), 16);
    Assert.assertEquals(new JoinHashMap(6).size(), 16);
    Assert.assertEquals(new JoinHashMap(7).size(), 16);
    Assert.assertEquals(new JoinHashMap(8).size(), 32);
    Assert.assertEquals(new JoinHashMap(9).size(), 32);
  }

  @Test
  public void testTuple() {
    Assert.assertEquals(JoinHashMap.tuple(0xFFFFFFFF, 0xFFFFFFFF), 0xFFFFFFFFFFFFFFFFL);
    Assert.assertEquals(JoinHashMap.tuple(0xFFFFFFFF, 0x00000000), 0xFFFFFFFF00000000L);
    Assert.assertEquals(JoinHashMap.tuple(0x00000000, 0xFFFFFFFF), 0x00000000FFFFFFFFL);
    Assert.assertEquals(JoinHashMap.tuple(0x00000000, 0x00000000), 0x0000000000000000L);
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

  @Test
  public void testValuesUpperBound() {
    final JoinHashMap m = new JoinHashMap(2);
    final int KEY_HI = 0xFFFFFFFF;
    final int KEY_LO = 0x00000000;
    final int VAL_HI = 0xFFFFFFFE;

    m.put(KEY_HI, VAL_HI);
    m.put(KEY_LO, VAL_HI);

    Assert.assertEquals(m.get(KEY_HI, 0), VAL_HI);
    Assert.assertEquals(m.get(KEY_LO, 0), VAL_HI);
  }

  @Test
  public void testValuesLowerBound() {
    final JoinHashMap m = new JoinHashMap(2);
    final int KEY_HI = 0xFFFFFFFF;
    final int KEY_LO = 0x00000000;
    final int VAL_LO = 0x00000000;

    m.put(KEY_HI, VAL_LO);
    m.put(KEY_LO, VAL_LO);

    Assert.assertEquals(m.get(KEY_HI, 0), VAL_LO);
    Assert.assertEquals(m.get(KEY_LO, 0), VAL_LO);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testValuesReserved() {
    new JoinHashMap(0).put(0, JoinHashMap.RESERVED_VALUE);
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

  @Test
  public void testMultiset() {
    JoinHashMap m = new JoinHashMap(10000);
    for(int i=0; i<1000; i++) {
      for(int j=0; j<10; j++) {
        m.put(i, j);
      }
    }

    for(int i=0; i<1000; i++) {
      for(int j=0; j<10; j++) {
        Assert.assertEquals(m.get(i, j), j);
      }
      Assert.assertEquals(m.get(i, 10), -1);
    }
  }

  @Test
  public void testMultisetIterator() {
    JoinHashMap m = new JoinHashMap(10000);
    for(int i=0; i<1000; i++) {
      for(int j=0; j<10; j++) {
        m.put(i, j);
      }
    }

    for(int i=0; i<1000; i++) {
      Assert.assertEquals(m.get(i, 0), 0);
      for(int j=1; j<10; j++) {
        Assert.assertEquals(m.getNext(), j);
      }
      Assert.assertEquals(m.getNext(), -1);
    }
  }
}
