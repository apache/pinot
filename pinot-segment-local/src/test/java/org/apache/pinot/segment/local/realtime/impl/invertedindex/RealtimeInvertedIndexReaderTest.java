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

import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class RealtimeInvertedIndexReaderTest {

  @Test
  public void testRealtimeInvertedIndexReader() {
    RealtimeInvertedIndexReader realtimeInvertedIndexReader = new RealtimeInvertedIndexReader();

    // Add dictionary id 0, document id 0 to the inverted index (single-value dictionary id not added yet)
    // Before adding
    MutableRoaringBitmap docIds = realtimeInvertedIndexReader.getDocIds(0);
    assertNotNull(docIds);
    assertTrue(docIds.isEmpty());
    // After adding
    realtimeInvertedIndexReader.add(0, 0);
    docIds = realtimeInvertedIndexReader.getDocIds(0);
    assertNotNull(docIds);
    assertFalse(docIds.isEmpty());
    assertTrue(docIds.contains(0));
    assertFalse(docIds.contains(1));

    // Add dictionary id 0, document id 1 to the inverted index (single-value dictionary id already added)
    // Before adding
    docIds = realtimeInvertedIndexReader.getDocIds(1);
    assertNotNull(docIds);
    assertTrue(docIds.isEmpty());
    realtimeInvertedIndexReader.add(0, 1);
    // After adding
    docIds = realtimeInvertedIndexReader.getDocIds(0);
    assertNotNull(docIds);
    assertFalse(docIds.isEmpty());
    assertTrue(docIds.contains(0));
    assertTrue(docIds.contains(1));

    // Add dictionary id 1 and 2, document id 2 to the inverted index (multi-value dictionary ids not added yet)
    // Before adding dictionary id 1
    docIds = realtimeInvertedIndexReader.getDocIds(1);
    assertNotNull(docIds);
    assertTrue(docIds.isEmpty());
    docIds = realtimeInvertedIndexReader.getDocIds(2);
    assertNotNull(docIds);
    assertTrue(docIds.isEmpty());
    // After adding dictionary id 1 but before adding dictionary id 2
    realtimeInvertedIndexReader.add(1, 2);
    docIds = realtimeInvertedIndexReader.getDocIds(0);
    assertNotNull(docIds);
    assertFalse(docIds.isEmpty());
    assertTrue(docIds.contains(0));
    assertTrue(docIds.contains(1));
    assertFalse(docIds.contains(2));
    docIds = realtimeInvertedIndexReader.getDocIds(1);
    assertNotNull(docIds);
    assertFalse(docIds.isEmpty());
    assertFalse(docIds.contains(0));
    assertFalse(docIds.contains(1));
    assertTrue(docIds.contains(2));
    docIds = realtimeInvertedIndexReader.getDocIds(2);
    assertNotNull(docIds);
    assertTrue(docIds.isEmpty());
    // After adding dictionary id 2
    realtimeInvertedIndexReader.add(2, 2);
    docIds = realtimeInvertedIndexReader.getDocIds(0);
    assertNotNull(docIds);
    assertFalse(docIds.isEmpty());
    assertTrue(docIds.contains(0));
    assertTrue(docIds.contains(1));
    assertFalse(docIds.contains(2));
    docIds = realtimeInvertedIndexReader.getDocIds(1);
    assertNotNull(docIds);
    assertFalse(docIds.isEmpty());
    assertFalse(docIds.contains(0));
    assertFalse(docIds.contains(1));
    assertTrue(docIds.contains(2));
    docIds = realtimeInvertedIndexReader.getDocIds(2);
    assertFalse(docIds.isEmpty());
    assertFalse(docIds.contains(0));
    assertFalse(docIds.contains(1));
    assertTrue(docIds.contains(2));
  }
}
