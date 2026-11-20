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
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


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
    Interner<String> falfInternerCustomHash =
        new FALFInterner(nUniqueObjs, s -> FALFInterner.hashCode((String) s), Objects::equals);

    // Go over all objects and intern them using both exact and FALF interners
    int nHits1 = runInterning(allObjs, exactInterner);
    int nHits2 = runInterning(allObjs, falfInterner);
    int nHits3 = runInterning(allObjs, falfInternerCustomHash);

    // For the exact interner, we should get a hit for each object except the
    // first nUniqueObjs.
    assertEquals(nHits1, nTotalObjs - nUniqueObjs);

    // For the FALF interner, due to its fixed size and thus almost inevitable hash
    // collisions, the number of hits is smaller. Let's verify that it's not too small, though.
    assertTrue(nHits2 > (nTotalObjs - nUniqueObjs) * 0.4);

    // With the better hash function, FALF interner should have more hits
    assertTrue(nHits3 > (nTotalObjs - nUniqueObjs) * 0.6);
  }

  private int runInterning(String[] objs, Interner<String> interner) {
    int nHits = 0;
    for (String origObj : objs) {
      String internedObj = interner.intern(origObj);
      assertEquals(origObj, internedObj);
      if (origObj != internedObj) {
        nHits++;
      }
    }
    return nHits;
  }
}
