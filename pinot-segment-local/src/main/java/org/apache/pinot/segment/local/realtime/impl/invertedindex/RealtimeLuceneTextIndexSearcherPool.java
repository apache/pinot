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
import org.apache.pinot.common.utils.ScalingThreadPoolExecutor;


/**
 * This class manages a thread pool used for searching over realtime Lucene segments by {@link RealtimeLuceneTextIndex}.
 * The pool max size is equivalent to pinot.query.scheduler.query_worker_threads to ensure each worker thread can have
 * an accompanying Lucene searcher thread if needed. init() is called in BaseServerStarter to avoid creating a
 * dependency on pinot-core.
 */
public class RealtimeLuceneTextIndexSearcherPool {
  private static RealtimeLuceneTextIndexSearcherPool _singletonInstance;
  private static ExecutorService _executorService;

  private RealtimeLuceneTextIndexSearcherPool(int size) {
    _executorService = ScalingThreadPoolExecutor.newScalingThreadPool(0, size, 500);
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
}
