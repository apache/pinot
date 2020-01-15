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
package org.apache.pinot.core.operator;

/**
 * The <code>ExecutionStatistics</code> class contains the operator statistics during execution time.
 */
public class ExecutionStatistics {
  // The number of documents scanned post filtering.
  private long _numDocsScanned;
  // The number of doc ids scanned in the filtering phase of the query execution: could be larger than the total doc num
  // because of multiple filtering predicates.
  private long _numEntriesScannedInFilter;
  // Equal to _numDocsScanned * number_of_projected_columns
  private long _numEntriesScannedPostFilter;
  private long _numTotalRawDocs;
  private long _numSegmentsProcessed;
  private long _numSegmentsMatched;

  public ExecutionStatistics() {
  }

  public ExecutionStatistics(long numDocsScanned, long numEntriesScannedInFilter, long numEntriesScannedPostFilter,
      long numTotalRawDocs) {
    _numDocsScanned = numDocsScanned;
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
    _numTotalRawDocs = numTotalRawDocs;
    _numSegmentsProcessed = 1;
    _numSegmentsMatched = (numDocsScanned == 0) ? 0 : 1;
  }

  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  public long getNumEntriesScannedInFilter() {
    return _numEntriesScannedInFilter;
  }

  public long getNumEntriesScannedPostFilter() {
    return _numEntriesScannedPostFilter;
  }

  public long getNumTotalRawDocs() {
    return _numTotalRawDocs;
  }

  public long getNumSegmentsProcessed() {
    return _numSegmentsProcessed;
  }

  public long getNumSegmentsMatched() {
    return _numSegmentsMatched;
  }

  /**
   * Merge another execution statistics into the current one.
   *
   * @param executionStatisticsToMerge execution statistics to merge.
   */
  public void merge(ExecutionStatistics executionStatisticsToMerge) {
    _numDocsScanned += executionStatisticsToMerge._numDocsScanned;
    _numEntriesScannedInFilter += executionStatisticsToMerge._numEntriesScannedInFilter;
    _numEntriesScannedPostFilter += executionStatisticsToMerge._numEntriesScannedPostFilter;
    _numTotalRawDocs += executionStatisticsToMerge._numTotalRawDocs;
    _numSegmentsProcessed += executionStatisticsToMerge._numSegmentsProcessed;
    _numSegmentsMatched += executionStatisticsToMerge._numSegmentsMatched;
  }

  @Override
  public String toString() {
    return "Execution Statistics:" + "\n  numDocsScanned: " + _numDocsScanned + "\n  numEntriesScannedInFilter: "
        + _numEntriesScannedInFilter + "\n  numEntriesScannedPostFilter: " + _numEntriesScannedPostFilter
        + "\n  numTotalRawDocs: " + _numTotalRawDocs + "\n  numSegmentsProcessed: " + _numSegmentsProcessed
        + "\n  numSegmentsMatched: " + _numSegmentsMatched;
  }
}
