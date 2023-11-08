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
package org.apache.pinot.core.query.reduce;

import java.util.concurrent.ExecutorService;


/**
 * POJO class to encapsulate DataTableReducer context information
 */
public class DataTableReducerContext {

  private final ExecutorService _executorService;
  private final int _maxReduceThreadsPerQuery;
  private final long _reduceTimeOutMs;
  // used for SQL GROUP BY
  private final int _groupByTrimThreshold;
  private final int _minGroupTrimSize;

  /**
   * Constructor for the class.
   *
   * @param executorService Executor service to use for DataTableReducer
   * @param maxReduceThreadsPerQuery Max number of threads to use for reduce phase
   * @param reduceTimeOutMs Reduce Phase timeOut in ms
   * @param groupByTrimThreshold trim threshold for SQL group by
   */
  public DataTableReducerContext(ExecutorService executorService, int maxReduceThreadsPerQuery, long reduceTimeOutMs,
      int groupByTrimThreshold, int minGroupTrimSize) {
    _executorService = executorService;
    _maxReduceThreadsPerQuery = maxReduceThreadsPerQuery;
    _reduceTimeOutMs = reduceTimeOutMs;
    _groupByTrimThreshold = groupByTrimThreshold;
    _minGroupTrimSize = minGroupTrimSize;
  }

  public ExecutorService getExecutorService() {
    return _executorService;
  }

  public int getMaxReduceThreadsPerQuery() {
    return _maxReduceThreadsPerQuery;
  }

  public long getReduceTimeOutMs() {
    return _reduceTimeOutMs;
  }

  public int getGroupByTrimThreshold() {
    return _groupByTrimThreshold;
  }

  public int getMinGroupTrimSize() {
    return _minGroupTrimSize;
  }
}
