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
  private final long _numDocsScanned;
  // The number of entries (single value entry contains 1 value, multi-value entry may contain multiple values) scanned
  // in the filtering phase of the query execution: could be larger than the total scanned doc num because of multiple
  // filtering predicates and multi-value entry.
  // TODO: make the semantic of entry the same in _numEntriesScannedInFilter and _numEntriesScannedPostFilter. Currently
  // _numEntriesScannedInFilter counts values in a multi-value entry multiple times whereas in
  // _numEntriesScannedPostFilter counts all values in a multi-value entry only once. We can add a new stats
  // _numValuesScannedInFilter to replace _numEntriesScannedInFilter.
  private final long _numEntriesScannedInFilter;
  // Equal to numDocsScanned * numberProjectedColumns.
  private final long _numEntriesScannedPostFilter;

  // TODO: Remove _numTotalDocs because it is not execution stats, and it is not set from the operators because they
  //       don't cover the pruned segments
  private final long _numTotalDocs;

  public ExecutionStatistics(long numDocsScanned, long numEntriesScannedInFilter, long numEntriesScannedPostFilter,
      long numTotalDocs) {
    _numDocsScanned = numDocsScanned;
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
    _numTotalDocs = numTotalDocs;
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

  public long getNumTotalDocs() {
    return _numTotalDocs;
  }
}
