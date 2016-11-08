/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator;

/**
 * The <code>ExecutionStatistics</code> class contains the operator statistics during execution time.
 */
public class ExecutionStatistics {
  private long _numDocsScanned;
  private long _numEntriesScannedInFilter;
  private long _numEntriesScannedPostFilter;
  private long _numTotalRawDocs;

  public ExecutionStatistics() {
  }

  public ExecutionStatistics(long numDocsScanned, long numEntriesScannedInFilter, long numEntriesScannedPostFilter,
      long numTotalRawDocs) {
    _numDocsScanned = numDocsScanned;
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
    _numTotalRawDocs = numTotalRawDocs;
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
  }

  @Override
  public String toString() {
    return "Execution Statistics:"
        + "\n  numDocsScanned: " + _numDocsScanned
        + "\n  numEntriesScannedInFilter: " + _numEntriesScannedInFilter
        + "\n  numEntriesScannedPostFilter: " + _numEntriesScannedPostFilter
        + "\n  numTotalRawDocs: " + _numTotalRawDocs;
  }
}
