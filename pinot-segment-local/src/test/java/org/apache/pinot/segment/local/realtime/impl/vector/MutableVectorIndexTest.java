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
package org.apache.pinot.segment.local.realtime.impl.vector;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexSearcherPool;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MutableVectorIndexTest {
  private static final String COLUMN_NAME = "embedding";

  @BeforeClass
  public void setUpSearcherPool() {
    RealtimeLuceneTextIndexSearcherPool.init(1);
  }

  @Test
  public void testEfSearchChangesRuntimeSearchBehavior() {
    MutableVectorIndex index = createIndex();
    try {
      int[] defaultMatches = index.getDocIds(new float[]{5.0F, 42.0F, 54.33333F, 42.24F, 3413.4F}, 3).toArray();
      Assert.assertEquals(defaultMatches.length, 3);

      index.setEfSearch(1);
      int[] limitedMatches = index.getDocIds(new float[]{5.0F, 42.0F, 54.33333F, 42.24F, 3413.4F}, 3).toArray();
      Assert.assertEquals(limitedMatches.length, 1,
          "efSearch=1 should cap mutable HNSW traversal the same way as immutable HNSW");
    } finally {
      index.close();
    }
  }

  @Test
  public void testDisableBoundedQueueRequiresEfSearch() {
    MutableVectorIndex index = createIndex();
    try {
      index.setUseBoundedQueue(false);
      IllegalArgumentException error = Assert.expectThrows(IllegalArgumentException.class,
          () -> index.getDocIds(new float[]{1.0F, 2.0F, 3.0F, 4.0F, 5.0F}, 2));
      Assert.assertTrue(error.getMessage().contains("vectorEfSearch"));
    } finally {
      index.close();
    }
  }

  @Test
  public void testRuntimeControlDebugInfoReflectsOverrides() {
    MutableVectorIndex index = createIndex();
    try {
      index.setEfSearch(6);
      index.setUseRelativeDistance(false);
      index.setUseBoundedQueue(false);

      Map<String, Object> debugInfo = index.getIndexDebugInfo();
      Assert.assertEquals(debugInfo.get("backend"), "HNSW");
      Assert.assertEquals(debugInfo.get("column"), COLUMN_NAME);
      Assert.assertEquals(debugInfo.get("numDocs"), 4);
      Assert.assertEquals(debugInfo.get("effectiveEfSearch"), 6);
      Assert.assertEquals(debugInfo.get("effectiveHnswUseRelativeDistance"), Boolean.FALSE);
      Assert.assertEquals(debugInfo.get("effectiveHnswUseBoundedQueue"), Boolean.FALSE);
      Assert.assertEquals(debugInfo.get("supportsPreFilter"), Boolean.FALSE);
    } finally {
      index.close();
    }
  }

  private static MutableVectorIndex createIndex() {
    MutableVectorIndex index =
        new MutableVectorIndex("mutableVectorIndexTest_" + System.nanoTime(), COLUMN_NAME, createConfig());
    addVector(index, new float[]{5.0F, 42.0F, 54.33333F, 42.24F, 1001.045F}, 0);
    addVector(index, new float[]{42.0F, 23423.0F, 42431.32532F, 6785676.3242F, 42.3F}, 1);
    addVector(index, new float[]{1.0F, 2.0F, 3.0F, 4.0F, 5.0F}, 2);
    addVector(index, new float[]{42.678F, 23423423.0F, 42431.32523432F, 6723485.3242F, 42342.3F}, 3);
    return index;
  }

  private static void addVector(MutableVectorIndex index, float[] values, int docId) {
    Object[] boxed = new Object[values.length];
    for (int i = 0; i < values.length; i++) {
      boxed[i] = values[i];
    }
    index.add(boxed, null, docId);
  }

  private static VectorIndexConfig createConfig() {
    Map<String, String> properties = new HashMap<>();
    properties.put("commitDocs", "4");
    properties.put("vectorIndexType", "HNSW");
    properties.put("vectorDimension", "5");
    return new VectorIndexConfig(false, "HNSW", 5, 1, VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
        properties);
  }
}
