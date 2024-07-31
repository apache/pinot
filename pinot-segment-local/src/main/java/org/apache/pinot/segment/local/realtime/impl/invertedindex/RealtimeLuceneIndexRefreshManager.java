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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.lucene.search.SearcherManager;
import org.apache.pinot.common.utils.ScalingThreadPoolExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class manages the refreshing of all realtime Lucene index readers. It uses an auto-scaling pool of threads,
 * which expands up to a configurable size, to refresh the readers.
 * <p>
 * During instantiation of a RealtimeLuceneTextIndex the corresponding SearcherManager is registered with this class.
 * When the RealtimeLuceneTextIndex is closed, the flag set by the RealtimeLuceneTextIndex is checked before attempting
 * a refresh, and SearcherManagerHolder instance previously registered to this class is dropped from the queue of
 * readers to be refreshed.
 */
public class RealtimeLuceneIndexRefreshManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeLuceneIndexRefreshManager.class);
  // max number of parallel refresh threads
  private final int _maxParallelism;
  // delay between refresh iterations
  private int _delayMs;
  // partitioned lists of SearcherManagerHolders, each gets its own thread for refreshing. SearcherManagerHolders
  // are added to the list with the smallest size to roughly balance the load across threads
  private final List<List<SearcherManagerHolder>> _partitionedListsOfSearchers;
  private static RealtimeLuceneIndexRefreshManager _singletonInstance;
  private static ExecutorService _executorService;

  private RealtimeLuceneIndexRefreshManager(int maxParallelism, int delayMs) {
    _maxParallelism = maxParallelism;
    _delayMs = delayMs;
    // Set min pool size to 0, scale up/down as needed. Set keep alive time to 0, as threads are generally long-lived
    _executorService = ScalingThreadPoolExecutor.newScalingThreadPool(0, _maxParallelism, 0L);
    _partitionedListsOfSearchers = new ArrayList<>();
  }

  public static RealtimeLuceneIndexRefreshManager getInstance() {
    Preconditions.checkArgument(_singletonInstance != null,
        "RealtimeLuceneIndexRefreshManager.init() must be called first");
    return _singletonInstance;
  }

  /**
   * Initializes the RealtimeLuceneIndexRefreshManager with the given maxParallelism and delayMs. This is
   * intended to be called only once at the beginning of the server lifecycle.
   * @param maxParallelism maximum number of refresh threads to use
   * @param delayMs minimum delay between refreshes
   */
  public static RealtimeLuceneIndexRefreshManager init(int maxParallelism, int delayMs) {
    _singletonInstance = new RealtimeLuceneIndexRefreshManager(maxParallelism, delayMs);
    return _singletonInstance;
  }

  @VisibleForTesting
  public void reset() {
    _partitionedListsOfSearchers.clear();
    _executorService.shutdownNow();
    _executorService = ScalingThreadPoolExecutor.newScalingThreadPool(0, _maxParallelism, 0L);
  }

  @VisibleForTesting
  public void setDelayMs(int delayMs) {
    _delayMs = delayMs;
  }

  /**
   * Add a new SearcherManagerHolder and submit it to the executor service for refreshing.
   * <p>
   * If the _partitionedListsOfSearchers has less than _maxParallelism lists, a new list is created and submitted to
   * the executor service to begin refreshing. If there are already _maxParallelism lists, the SearcherManagerHolder
   * will be added to the list with the smallest size. If the smallest list is empty, it will be submitted to the
   * executor as the old one was stopped.
   * <p>
   * The RealtimeLuceneRefreshRunnable will drop closed indexes from the list, and if all indexes are closed, the
   * empty list will ensure the Runnable finishes. ScalingThreadPoolExecutor will then scale down the number of
   * threads. This ensures that we do not leave any threads if there are no tables with text index, or text indices
   * are removed on the server through actions such as config update or re-balance.
   */
  public synchronized void addSearcherManagerHolder(SearcherManagerHolder searcherManagerHolder) {
    if (_partitionedListsOfSearchers.size() < _maxParallelism) {
      List<SearcherManagerHolder> searcherManagers = Collections.synchronizedList(new ArrayList<>());
      searcherManagers.add(searcherManagerHolder);
      _partitionedListsOfSearchers.add(searcherManagers);
      _executorService.submit(new RealtimeLuceneRefreshRunnable(searcherManagers, _delayMs));
      return;
    }

    List<SearcherManagerHolder> smallestList = null;
    for (List<SearcherManagerHolder> list : _partitionedListsOfSearchers) {
      if (smallestList == null || list.size() < smallestList.size()) {
        smallestList = list;
      }
    }
    assert smallestList != null;
    smallestList.add(searcherManagerHolder);

    // If the list was empty before adding the SearcherManagerHolder, the runnable containing the list
    // has exited or will soon exit. Therefore, we need to submit a new runnable to the executor for the list.
    if (smallestList.size() == 1) {
      _executorService.submit(new RealtimeLuceneRefreshRunnable(smallestList, _delayMs));
    }
  }

  /**
   * Blocks for up to 45 seconds waiting for refreshes of realtime Lucene index readers to complete.
   * If all segments were previously closed, it should return immediately.
   */
  public boolean awaitTermination() {
    // Interrupts will be handled by the RealtimeLuceneRefreshRunnable refresh loop. In general, all
    // indexes should be marked closed before this method is called, and _executorService should
    // shutdown immediately as there are no active threads. If for some reason an index did not close correctly,
    // SearcherManager.maybeRefresh() should be on the order of seconds in the worst case and this should
    // return shortly after.
    _executorService.shutdownNow();
    boolean terminated = false;
    try {
      terminated = _executorService.awaitTermination(45, TimeUnit.SECONDS);
      if (!terminated) {
        LOGGER.warn("Realtime Lucene index refresh pool did not terminate in 45 seconds.");
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Interrupted while waiting for realtime Lucene index refresh to shutdown.");
    }
    return terminated;
  }

  @VisibleForTesting
  public int getPoolSize() {
    return ((ThreadPoolExecutor) _executorService).getPoolSize();
  }

  @VisibleForTesting
  public List<Integer> getListSizes() {
    return _partitionedListsOfSearchers.stream().map(List::size).sorted().collect(Collectors.toList());
  }

  /**
   * SearcherManagerHolder is a class that holds a SearcherManager instance for a segment and column. Instances
   * of this class should be registered with the RealtimeLuceneIndexRefreshManager class to manage refreshing of
   * the SearcherManager instance it holds.
   */
  public static class SearcherManagerHolder {
    private final String _segmentName;
    private final String _columnName;
    private final Lock _lock;
    private volatile boolean _indexClosed;
    private final SearcherManager _searcherManager;

    public SearcherManagerHolder(String segmentName, String columnName, SearcherManager searcherManager) {
      _segmentName = segmentName;
      _columnName = columnName;
      _lock = new ReentrantLock();
      _indexClosed = false;
      _searcherManager = searcherManager;
    }

    public void setIndexClosed() {
      _indexClosed = true;
    }

    public Lock getLock() {
      return _lock;
    }

    public String getSegmentName() {
      return _segmentName;
    }

    public String getColumnName() {
      return _columnName;
    }

    public SearcherManager getSearcherManager() {
      return _searcherManager;
    }

    public boolean isIndexClosed() {
      return _indexClosed;
    }
  }

  /**
   * Runnable that refreshes a list of SearcherManagerHolder instances. This class is responsible for refreshing
   * each SearcherManagerHolder in the list, and re-adding it to the list if it has not been closed. If every
   * instance has been closed, the thread will terminate as the list size will be empty.
   */
  private static class RealtimeLuceneRefreshRunnable implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeLuceneRefreshRunnable.class);
    private final int _delayMs;
    private final List<SearcherManagerHolder> _searchers;

    public RealtimeLuceneRefreshRunnable(List<SearcherManagerHolder> searchers, int delayMs) {
      _searchers = searchers;
      _delayMs = delayMs;
    }

    @Override
    public void run() {
      int i = 0; // current index in _searchers
      while (!_searchers.isEmpty() && i <= _searchers.size() && !Thread.interrupted()) {
        if (i == _searchers.size()) {
          i = 0; // reset cursor to the beginning if we've reached the end
        }
        SearcherManagerHolder searcherManagerHolder = _searchers.get(i);
        assert searcherManagerHolder != null;
        searcherManagerHolder.getLock().lock();
        try {
          if (searcherManagerHolder.isIndexClosed()) {
            _searchers.remove(i);
            continue; // do not increment i, as the remaining elements in the list have been shifted
          }

          if (!searcherManagerHolder.isIndexClosed()) {
            try {
              searcherManagerHolder.getSearcherManager().maybeRefresh();
            } catch (Exception e) {
              // we should never be here since the locking semantics between RealtimeLuceneTextIndex.close()
              // and this code along with volatile state isIndexClosed protect against the cases where this thread
              // might attempt to refresh a realtime lucene reader after it has already been closed during
              // RealtimeLuceneTextIndex.commit()
              LOGGER.warn("Caught exception {} while refreshing realtime lucene reader for segment: {} and column: {}",
                  e, searcherManagerHolder.getSegmentName(), searcherManagerHolder.getColumnName());
            }
            i++;
          }
        } finally {
          searcherManagerHolder.getLock().unlock();
        }

        try {
          Thread.sleep(_delayMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }
}
