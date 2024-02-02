/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.utils;

import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import java.util.Objects;
import java.util.Random;
import org.apache.pinot.spi.utils.FALFInterner;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FALFInternerTest {
  @Test
  public void testInterningByteBuffers() {
    Random random = new Random(1);

    int nUniqueObjs = 1024;
    int nTotalObjs = 8 * nUniqueObjs;

    String[] allObjs = new String[nTotalObjs];

    // Create an array of objects where each object should have ~8 copies
    for (int i = 0; i < nTotalObjs; i++) {
      int next = random.nextInt(nUniqueObjs);
      allObjs[i] = Integer.toString(next);
    }

    Interner<String> exactInterner = Interners.newStrongInterner();
    Interner<String> falfInterner = new FALFInterner(nUniqueObjs);
    Interner<String> falfInternerCustomHash = new FALFInterner(nUniqueObjs, s -> hashCode((String) s), Objects::equals);

    // Go over all objects and intern them using both exact and FALF interners
    int nHits1 = runInterning(allObjs, exactInterner, true);
    int nHits2 = runInterning(allObjs, falfInterner, true);
    int nHits3 = runInterning(allObjs, falfInternerCustomHash, true);

    System.out.println(nHits1);
    System.out.println(nHits2);
    System.out.println(nHits3);

    // For the exact interner, we should get a hit for each object except the
    // first nUniqueObjs.
    Assert.assertEquals(nHits1, nTotalObjs - nUniqueObjs);

    // For the FALF interner, due to its fixed size and thus almost inevitable hash
    // collisions, the number of hits is smaller. Let's verify that it's not too small, though.
    Assert.assertTrue(nHits2 > (nTotalObjs - nUniqueObjs) * 0.4);

    // With the better hash function, FALF interner should have more hits
    Assert.assertTrue(nHits3 > (nTotalObjs - nUniqueObjs) * 0.6);

    // Ad hoc benchmarking code. Disabled to avoid test slowdown.
    // In one run the MacBook laptop, FALFInterner below performs nearly twice faster
    // (1217 ms vs 2230 ms) With custom hash function, FALFInterner's speed is about the
    // same as the Guava interner.
//    for (int j = 0; j < 3; j++) {
//      long time0 = System.currentTimeMillis();
//      long totNHits = 0;
//      for (int i = 0; i < 10000; i++) {
//        totNHits += runInterning(allObjs, exactInterner, false);
//      }
//      long time1 = System.currentTimeMillis();
//      System.out.println("Guava interner. totNHits = " + totNHits + ", time = " + (time1 - time0));
//
//      time0 = System.currentTimeMillis();
//      totNHits = 0;
//      for (int i = 0; i < 10000; i++) {
//        totNHits += runInterning(allObjs, falfInterner, false);
//      }
//      time1 = System.currentTimeMillis();
//      System.out.println("FALF interner. totNHits = " + totNHits + ", time = " + (time1 - time0));
//
//      time0 = System.currentTimeMillis();
//      totNHits = 0;
//      for (int i = 0; i < 10000; i++) {
//        totNHits += runInterning(allObjs, falfInternerCustomHash, false);
//      }
//      time1 = System.currentTimeMillis();
//      System.out.println("FALF interner Custom Hash. totNHits = " + totNHits + ", time = " + (time1 - time0));
//    }
  }

  private int runInterning(String[] objs, Interner<String> interner, boolean performAssert) {
    int nHits = 0;
    for (String origObj : objs) {
      String internedObj = interner.intern(origObj);
      if (performAssert) {
        Assert.assertEquals(origObj, internedObj);
      }
      if (origObj != internedObj) {
        nHits++;
      }
    }
    return nHits;
  }

  // Custom hash code implementation, that gives better distribution than standard hashCode()

  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;

  public static int hashCode(String s) {
    int h1 = 0;

    // step through value 2 chars at a time
    for (int i = 1; i < s.length(); i += 2) {
      int k1 = s.charAt(i - 1) | (s.charAt(i) << 16);
      h1 = nextHashCode(k1, h1);
    }

    // deal with any remaining characters
    if ((s.length() & 1) == 1) {
      int k1 = s.charAt(s.length() - 1);
      k1 = mixK1(k1);
      h1 ^= k1;
    }

    return fmix(h1, s.length() * 2);
  }

  private static int nextHashCode(int value, int prevHashCode) {
    int k1 = mixK1(value);
    return mixH1(prevHashCode, k1);
  }

  private static int mixK1(int k1) {
    k1 *= C1;
    k1 = Integer.rotateLeft(k1, 15);
    k1 *= C2;
    return k1;
  }

  private static int mixH1(int h1, int k1) {
    h1 ^= k1;
    h1 = Integer.rotateLeft(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
  }

  private static int fmix(int h1, int len) {
    // Force all bits to avalanche
    h1 ^= len;
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;
    return h1;
  }
}
