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
package org.apache.pinot.core.realtime.impl.geospatial;

import com.uber.h3core.H3Core;
import java.io.IOException;
import org.apache.pinot.core.segment.creator.impl.geospatial.H3IndexResolution;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class RealtimeH3IndexReaderTest {

  @Test
  public void testRealtimeH3IndexReader()
      throws IOException {
    RealtimeH3IndexReader realtimeH3IndexReader =
        new RealtimeH3IndexReader(new H3IndexResolution(Lists.newArrayList(5, 6)));

    // empty index
    ImmutableRoaringBitmap docIds = realtimeH3IndexReader.getDocIds(0);
    assertNotNull(docIds);
    assertTrue(docIds.isEmpty());

    // adding some docs in bay area
    realtimeH3IndexReader.add(0, 37.01, -121.99);
    realtimeH3IndexReader.add(1, 37.39, -121.9);
    realtimeH3IndexReader.add(2, 37.1, -121.9);

    // retrieve docid of some random location
    docIds = realtimeH3IndexReader.getDocIds(30);
    assertNotNull(docIds);
    assertTrue(docIds.isEmpty());

    H3Core h3Core = H3Core.newInstance();
    // use the h3id of mountain view to retrieve the docs
    Long h3Id = h3Core.geoToH3(37, -122, 5);
    docIds = realtimeH3IndexReader.getDocIds(h3Id);
    assertNotNull(docIds);
    assertTrue(docIds.contains(0));
    assertFalse(docIds.contains(1));
    assertTrue(docIds.contains(2));
  }
}
