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
import java.util.List;
import org.apache.lucene.search.SearcherManager;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class RealtimeLuceneIndexRefreshManagerTest {

  @BeforeClass
  public void init() {
    RealtimeLuceneIndexRefreshManager.init(2, 0);
  }

  @BeforeMethod
  public void setup() {
    RealtimeLuceneIndexRefreshManager.getInstance().reset();
  }

  @Test
  public void testSingleSearcherManager() {
    RealtimeLuceneIndexRefreshManager realtimeLuceneIndexRefreshManager =
        RealtimeLuceneIndexRefreshManager.getInstance();

    assertEquals(realtimeLuceneIndexRefreshManager.getPoolSize(), 0, "Initial pool size should be 0");

    RealtimeLuceneIndexRefreshManager.SearcherManagerHolder searcherManagerHolder = getSearcherManagerHolder(1);
    realtimeLuceneIndexRefreshManager.addSearcherManagerHolder(searcherManagerHolder);
    TestUtils.waitForCondition(aVoid -> realtimeLuceneIndexRefreshManager.getPoolSize() == 1, 5000,
        "Timed out waiting for thead pool to scale up for the initial searcher manager holder");

    searcherManagerHolder.setIndexClosed();
    TestUtils.waitForCondition(aVoid -> realtimeLuceneIndexRefreshManager.getPoolSize() == 0, 5000,
        "Timed out waiting for thread pool to scale down");
  }

  @Test
  public void testManySearcherManagers() {
    RealtimeLuceneIndexRefreshManager realtimeLuceneIndexRefreshManager =
        RealtimeLuceneIndexRefreshManager.getInstance();

    assertEquals(realtimeLuceneIndexRefreshManager.getPoolSize(), 0, "Initial pool size should be 0");

    RealtimeLuceneIndexRefreshManager.SearcherManagerHolder searcherManagerHolder1 = getSearcherManagerHolder(1);
    RealtimeLuceneIndexRefreshManager.SearcherManagerHolder searcherManagerHolder2 = getSearcherManagerHolder(2);
    RealtimeLuceneIndexRefreshManager.SearcherManagerHolder searcherManagerHolder3 = getSearcherManagerHolder(3);
    RealtimeLuceneIndexRefreshManager.SearcherManagerHolder searcherManagerHolder4 = getSearcherManagerHolder(4);

    realtimeLuceneIndexRefreshManager.addSearcherManagerHolder(searcherManagerHolder1);
    TestUtils.waitForCondition(aVoid -> realtimeLuceneIndexRefreshManager.getPoolSize() == 1, 5000,
        "Timed out waiting for thead pool to scale up for the initial searcher manager holder");

    realtimeLuceneIndexRefreshManager.addSearcherManagerHolder(searcherManagerHolder2);
    TestUtils.waitForCondition(aVoid -> realtimeLuceneIndexRefreshManager.getPoolSize() == 2, 5000,
        "Timed out waiting for thead pool to scale up for the second searcher manager holder");

    realtimeLuceneIndexRefreshManager.addSearcherManagerHolder(searcherManagerHolder3);
    TestUtils.waitForCondition(aVoid -> realtimeLuceneIndexRefreshManager.getListSizes().equals(List.of(1, 2)), 5000,
        "Timed out waiting for the searcher manager holder to be added to another queue");

    realtimeLuceneIndexRefreshManager.addSearcherManagerHolder(searcherManagerHolder4);
    TestUtils.waitForCondition(aVoid -> realtimeLuceneIndexRefreshManager.getListSizes().equals(List.of(2, 2)), 5000,
        "Timed out waiting for the searcher manager holder to be added to the smallest queue");

    searcherManagerHolder1.setIndexClosed();
    searcherManagerHolder2.setIndexClosed();
    searcherManagerHolder3.setIndexClosed();
    TestUtils.waitForCondition(aVoid -> realtimeLuceneIndexRefreshManager.getPoolSize() == 1, 5000,
        "Timed out waiting for thead pool to scale down as only one searcher manager holder is left");

    searcherManagerHolder4.setIndexClosed();
    TestUtils.waitForCondition(aVoid -> realtimeLuceneIndexRefreshManager.getPoolSize() == 0, 5000,
        "Timed out waiting for thead pool to scale down as all searcher manager holders have been closed");
  }

  @Test
  public void testDelayMs()
      throws Exception {
    RealtimeLuceneIndexRefreshManager realtimeLuceneIndexRefreshManager =
        RealtimeLuceneIndexRefreshManager.getInstance();
    realtimeLuceneIndexRefreshManager.setDelayMs(501);

    RealtimeLuceneIndexRefreshManager.SearcherManagerHolder searcherManagerHolder = getSearcherManagerHolder(1);
    realtimeLuceneIndexRefreshManager.addSearcherManagerHolder(searcherManagerHolder);

    Thread.sleep(1500);
    searcherManagerHolder.setIndexClosed();

    SearcherManager searcherManager = searcherManagerHolder.getSearcherManager();
    verify(searcherManager, times(3)).maybeRefresh();
  }

  @Test
  public void testTerminationForEmptyPool() {
    assertTrue(RealtimeLuceneIndexRefreshManager.getInstance().awaitTermination());
  }

  @Test
  public void testTerminationWhileActive() {
    RealtimeLuceneIndexRefreshManager realtimeLuceneIndexRefreshManager =
        RealtimeLuceneIndexRefreshManager.getInstance();

    assertEquals(realtimeLuceneIndexRefreshManager.getPoolSize(), 0, "Initial pool size should be 0");

    RealtimeLuceneIndexRefreshManager.SearcherManagerHolder searcherManagerHolder1 = getSearcherManagerHolder(1);
    RealtimeLuceneIndexRefreshManager.SearcherManagerHolder searcherManagerHolder2 = getSearcherManagerHolder(2);
    RealtimeLuceneIndexRefreshManager.SearcherManagerHolder searcherManagerHolder3 = getSearcherManagerHolder(3);
    realtimeLuceneIndexRefreshManager.addSearcherManagerHolder(searcherManagerHolder1);
    realtimeLuceneIndexRefreshManager.addSearcherManagerHolder(searcherManagerHolder2);
    realtimeLuceneIndexRefreshManager.addSearcherManagerHolder(searcherManagerHolder3);

    TestUtils.waitForCondition(aVoid -> realtimeLuceneIndexRefreshManager.getPoolSize() == 2, 10000,
        "Timed out waiting for thread pool to scale up");

    searcherManagerHolder1.setIndexClosed();
    searcherManagerHolder2.setIndexClosed();
    searcherManagerHolder3.setIndexClosed();
    assertTrue(RealtimeLuceneIndexRefreshManager.getInstance().awaitTermination());
  }

  private RealtimeLuceneIndexRefreshManager.SearcherManagerHolder getSearcherManagerHolder(int id) {
    SearcherManager searcherManager = mock(SearcherManager.class);
    try {
      when(searcherManager.maybeRefresh()).thenReturn(true);
    } catch (IOException e) {
      Assert.fail("Mocked searcher manager should not throw exception");
    }
    return new RealtimeLuceneIndexRefreshManager.SearcherManagerHolder("segment" + id, "col", searcherManager);
  }
}
