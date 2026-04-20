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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.pinot.common.utils.ScalingThreadPoolExecutor;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class manages a thread pool used for searching over realtime Lucene segments by {@link RealtimeLuceneTextIndex}.
 * The pool max size is equivalent to pinot.query.scheduler.query_worker_threads to ensure each worker thread can have
 * an accompanying Lucene searcher thread if needed. init() is called in BaseServerStarter to avoid creating a
 * dependency on pinot-core.
 *
 * <p>The pool supports dynamic resizing via {@link #resize(int)} so that it stays in sync when
 * query_worker_threads is changed at runtime through cluster config.
 *
 * <p>The executor is wrapped with QueryThreadContext.contextAwareExecutorService(executor, false) to propagate
 * QueryThreadContext for CPU/memory tracking, but WITHOUT registering tasks for cancellation. This prevents
 * Thread.interrupt() during Lucene search which could corrupt FSDirectory used by IndexWriter.
 * See https://github.com/apache/lucene/issues/3315 and https://github.com/apache/lucene/issues/9309
 */
public class RealtimeLuceneTextIndexSearcherPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeLuceneTextIndexSearcherPool.class);

  private static RealtimeLuceneTextIndexSearcherPool _singletonInstance;

  private final ThreadPoolExecutor _baseExecutor;
  private final ExecutorService _executorService;

  private RealtimeLuceneTextIndexSearcherPool(int size) {
    _baseExecutor = (ThreadPoolExecutor) ScalingThreadPoolExecutor.newScalingThreadPool(0, size, 500);
    _executorService = QueryThreadContext.contextAwareExecutorService(_baseExecutor, false);
  }

  public static RealtimeLuceneTextIndexSearcherPool getInstance() {
    if (_singletonInstance == null) {
      throw new AssertionError("RealtimeLuceneTextIndexSearcherPool.init() must be called first");
    }
    return _singletonInstance;
  }

  public static RealtimeLuceneTextIndexSearcherPool init(int size) {
    _singletonInstance = new RealtimeLuceneTextIndexSearcherPool(size);
    return _singletonInstance;
  }

  public ExecutorService getExecutorService() {
    return _executorService;
  }

  /**
   * Dynamically resizes the maximum pool size of the underlying {@link ScalingThreadPoolExecutor}.
   * Since the base executor has corePoolSize=0, only maximumPoolSize needs to be adjusted.
   * When shrinking, idle threads are interrupted immediately; busy threads finish their current
   * Lucene search task before exiting, so no in-flight search is disrupted.
   *
   * @param newMaxSize the new maximum thread count (must be &gt; 0)
   */
  public synchronized void resize(int newMaxSize) {
    if (newMaxSize <= 0) {
      LOGGER.warn("Invalid Lucene searcher pool size: {}. Must be > 0. Skipping resize.", newMaxSize);
      return;
    }
    int oldMaxSize = _baseExecutor.getMaximumPoolSize();
    if (oldMaxSize == newMaxSize) {
      LOGGER.debug("Lucene searcher pool size unchanged at {}. Skipping resize.", newMaxSize);
      return;
    }
    _baseExecutor.setMaximumPoolSize(newMaxSize);
    LOGGER.info("Resized Lucene searcher pool: {} -> {}", oldMaxSize, newMaxSize);
  }

  /**
   * Returns the current maximum pool size. Primarily for testing.
   */
  public int getMaxPoolSize() {
    return _baseExecutor.getMaximumPoolSize();
  }
}
