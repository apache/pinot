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
package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class NativeMutableFSTIndexTest {
  private NativeMutableFSTIndex _nativeMutableFSTIndex;

  @BeforeClass
  public void setUp()
      throws Exception {
    _nativeMutableFSTIndex = new NativeMutableFSTIndex();
    List<String> documents = getTextData();

    for (String doc : documents) {
      _nativeMutableFSTIndex.add(doc);
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _nativeMutableFSTIndex.close();
  }

  @Test
  public void testQueries() {
    String nativeQuery = "P.*";
    List<Integer> resultList = Arrays.asList(0, 9);
    testSelectionResults(nativeQuery, 2, resultList);

    nativeQuery = "a.*";
    resultList = Arrays.asList(5, 6);
    testSelectionResults(nativeQuery, 2, resultList);

    nativeQuery = ".*ed";
    resultList = Arrays.asList(6);
    testSelectionResults(nativeQuery, 1, resultList);

    nativeQuery = ".*m.*";
    resultList = Arrays.asList(6, 7, 8);
    testSelectionResults(nativeQuery, 3, resultList);
  }

  private List<String> getTextData() {
    return Arrays.asList("Prince", "Andrew", "kept", "looking", "with", "an", "amused", "smile", "from", "Pierre");
  }

  private void testSelectionResults(String nativeQuery, int resultCount, @Nullable List<Integer> results) {
    ImmutableRoaringBitmap resultMap = _nativeMutableFSTIndex.getDictIds(nativeQuery);
    assertEquals(resultMap.getCardinality(), resultCount);

    if (results != null) {
      for (int result : results) {
        assertTrue(resultMap.contains(result));
      }
    }
  }
}
