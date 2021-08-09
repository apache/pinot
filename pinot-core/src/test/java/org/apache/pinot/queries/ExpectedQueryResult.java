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
  private long numDocsScanned;
  private long numEntriesScannedInFilter;
  private long numEntriesScannedPostFilter;
  private long numTotalDocs;
  private T[] results;

  public ExpectedQueryResult(long numDocsScanned, long numEntriesScannedInFilter, long numEntriesScannedPostFilter,
      long numTotalDocs, T[] results) {
    this.numDocsScanned = numDocsScanned;
    this.numEntriesScannedInFilter = numEntriesScannedInFilter;
    this.numEntriesScannedPostFilter = numEntriesScannedPostFilter;
    this.numTotalDocs = numTotalDocs;
    this.results = results;
  }

  public long getNumDocsScanned() {
    return numDocsScanned;
  }

  public long getNumEntriesScannedInFilter() {
    return numEntriesScannedInFilter;
  }

  public long getNumEntriesScannedPostFilter() {
    return numEntriesScannedPostFilter;
  }

  public long getNumTotalDocs() {
    return numTotalDocs;
  }

  public T[] getResults() {
    return results;
  }
}
