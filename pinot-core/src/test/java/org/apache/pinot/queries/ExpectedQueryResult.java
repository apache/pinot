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
package org.apache.pinot.queries;

public class ExpectedQueryResult<T> {
  private long _numDocsScanned;
  private long _numEntriesScannedInFilter;
  private long _numEntriesScannedPostFilter;
  private long _numTotalDocs;
  private T[] _results;

  public ExpectedQueryResult(long numDocsScanned, long numEntriesScannedInFilter, long numEntriesScannedPostFilter,
      long numTotalDocs, T[] results) {
    _numDocsScanned = numDocsScanned;
    _numEntriesScannedInFilter = numEntriesScannedInFilter;
    _numEntriesScannedPostFilter = numEntriesScannedPostFilter;
    _numTotalDocs = numTotalDocs;
    _results = results;
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

  public T[] getResults() {
    return _results;
  }
}
