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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class RealtimeLuceneTextIndexSearcherPoolTest {

  @Test
  public void testInitAndGetInstance() {
    RealtimeLuceneTextIndexSearcherPool pool = RealtimeLuceneTextIndexSearcherPool.init(4);
    assertNotNull(pool);
    assertEquals(pool.getMaxPoolSize(), 4);
    assertNotNull(pool.getExecutorService());
    assertEquals(RealtimeLuceneTextIndexSearcherPool.getInstance(), pool);
  }

  @Test
  public void testResizeIncrease() {
    RealtimeLuceneTextIndexSearcherPool pool = RealtimeLuceneTextIndexSearcherPool.init(4);
    assertEquals(pool.getMaxPoolSize(), 4);

    pool.resize(8);
    assertEquals(pool.getMaxPoolSize(), 8);
  }

  @Test
  public void testResizeDecrease() {
    RealtimeLuceneTextIndexSearcherPool pool = RealtimeLuceneTextIndexSearcherPool.init(8);
    assertEquals(pool.getMaxPoolSize(), 8);

    pool.resize(4);
    assertEquals(pool.getMaxPoolSize(), 4);
  }

  @Test
  public void testResizeSameSize() {
    RealtimeLuceneTextIndexSearcherPool pool = RealtimeLuceneTextIndexSearcherPool.init(4);
    assertEquals(pool.getMaxPoolSize(), 4);

    // No-op when size is unchanged
    pool.resize(4);
    assertEquals(pool.getMaxPoolSize(), 4);
  }

  @Test
  public void testResizeInvalidSize() {
    RealtimeLuceneTextIndexSearcherPool pool = RealtimeLuceneTextIndexSearcherPool.init(4);
    assertEquals(pool.getMaxPoolSize(), 4);

    // Zero and negative values should be ignored
    pool.resize(0);
    assertEquals(pool.getMaxPoolSize(), 4);

    pool.resize(-1);
    assertEquals(pool.getMaxPoolSize(), 4);
  }
}
