/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.dataframe;

import org.testng.Assert;
import org.testng.annotations.Test;


public class PrimitiveMultimapTest {

  @Test
  public void testLog2() {
    Assert.assertEquals(PrimitiveMultimap.log2(0), 0);
    Assert.assertEquals(PrimitiveMultimap.log2(1), 0);
    Assert.assertEquals(PrimitiveMultimap.log2(2), 1);
    Assert.assertEquals(PrimitiveMultimap.log2(3), 1);
    Assert.assertEquals(PrimitiveMultimap.log2(4), 2);
    Assert.assertEquals(PrimitiveMultimap.log2(5), 2);
    Assert.assertEquals(PrimitiveMultimap.log2(6), 2);
    Assert.assertEquals(PrimitiveMultimap.log2(7), 2);
    Assert.assertEquals(PrimitiveMultimap.log2(8), 3);
  }

  @Test
  public void testPow2() {
    Assert.assertEquals(PrimitiveMultimap.pow2(0), 1);
    Assert.assertEquals(PrimitiveMultimap.pow2(1), 2);
    Assert.assertEquals(PrimitiveMultimap.pow2(2), 4);
    Assert.assertEquals(PrimitiveMultimap.pow2(3), 8);
    Assert.assertEquals(PrimitiveMultimap.pow2(4), 16);
  }

  @Test
  public void testCapacity() {
    Assert.assertEquals(new PrimitiveMultimap(0).capacity(), 0);
    Assert.assertEquals(new PrimitiveMultimap(1).capacity(), 1);
    Assert.assertEquals(new PrimitiveMultimap(2).capacity(), 2);
    Assert.assertEquals(new PrimitiveMultimap(3).capacity(), 3);
    Assert.assertEquals(new PrimitiveMultimap(4).capacity(), 4);
  }

  @Test
  public void testCapacityEffective() {
    Assert.assertEquals(new PrimitiveMultimap(0).capacityEffective(), 2);
    Assert.assertEquals(new PrimitiveMultimap(1).capacityEffective(), (int) (2 * PrimitiveMultimap.SCALING_FACTOR));
    Assert.assertEquals(new PrimitiveMultimap(2).capacityEffective(), (int) (4 * PrimitiveMultimap.SCALING_FACTOR));
    Assert.assertEquals(new PrimitiveMultimap(3).capacityEffective(), (int) (4 * PrimitiveMultimap.SCALING_FACTOR));
    Assert.assertEquals(new PrimitiveMultimap(4).capacityEffective(), (int) (8 * PrimitiveMultimap.SCALING_FACTOR));
    Assert.assertEquals(new PrimitiveMultimap(5).capacityEffective(), (int) (8 * PrimitiveMultimap.SCALING_FACTOR));
    Assert.assertEquals(new PrimitiveMultimap(6).capacityEffective(), (int) (8 * PrimitiveMultimap.SCALING_FACTOR));
    Assert.assertEquals(new PrimitiveMultimap(7).capacityEffective(), (int) (8 * PrimitiveMultimap.SCALING_FACTOR));
    Assert.assertEquals(new PrimitiveMultimap(8).capacityEffective(), (int) (16 * PrimitiveMultimap.SCALING_FACTOR));
    Assert.assertEquals(new PrimitiveMultimap(9).capacityEffective(), (int) (16 * PrimitiveMultimap.SCALING_FACTOR));
  }

  @Test
  public void testTuple() {
    Assert.assertEquals(PrimitiveMultimap.tuple(0xFFFFFFFF, 0xFFFFFFFF), 0xFFFFFFFFFFFFFFFFL);
    Assert.assertEquals(PrimitiveMultimap.tuple(0xFFFFFFFF, 0x00000000), 0xFFFFFFFF00000000L);
    Assert.assertEquals(PrimitiveMultimap.tuple(0x00000000, 0xFFFFFFFF), 0x00000000FFFFFFFFL);
    Assert.assertEquals(PrimitiveMultimap.tuple(0x00000000, 0x00000000), 0x0000000000000000L);
  }

  @Test
  public void testTupleDecomposition() {
    final long tuple = 0x1234567899990000L;
    Assert.assertEquals(PrimitiveMultimap.tuple2key(tuple), 0x12345678);
    Assert.assertEquals(PrimitiveMultimap.tuple2val(tuple), 0x99990000);
  }

  @Test
  public void testTupleComposition() {
    Assert.assertEquals(PrimitiveMultimap.tuple(0x12345678, 0x79999999), 0x1234567879999999L);
  }

  @Test
  public void testValuesUpperBound() {
    final PrimitiveMultimap m = new PrimitiveMultimap(2);
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
    final PrimitiveMultimap m = new PrimitiveMultimap(2);
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
    new PrimitiveMultimap(0).put(0, PrimitiveMultimap.RESERVED_VALUE);
  }

  @Test
  public void testBasic() {
    PrimitiveMultimap m = new PrimitiveMultimap(4);
    m.put(12345, 54321);
    Assert.assertEquals(m.get(12345, 0), 54321);
  }

  @Test
  public void testMultiple() {
    PrimitiveMultimap m = new PrimitiveMultimap(4);
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
    PrimitiveMultimap m = new PrimitiveMultimap(8);
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
  public void testMultisetSequential() {
    PrimitiveMultimap m = new PrimitiveMultimap(10000);
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
  public void testMultisetRoundRobin() {
    PrimitiveMultimap m = new PrimitiveMultimap(10000);
    for(int i=0; i<10000; i++) {
      m.put(i % 10, i);
    }

    for(int i=0; i<10000; i++) {
      Assert.assertEquals(m.get(i % 10, i / 10), i);
    }
  }

  @Test
  public void testMultisetIterator() {
    PrimitiveMultimap m = new PrimitiveMultimap(10000);
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

  @Test
  public void testMultisetSingleKey() {
    PrimitiveMultimap m = new PrimitiveMultimap(1000);
    for(int i=0; i<1000; i++) {
      m.put(12345, i);
    }

    int cntr = 0;
    int val = m.get(12345);
    while(val != -1) {
      Assert.assertEquals(val, cntr++);
      val = m.getNext();
    }
  }
}
