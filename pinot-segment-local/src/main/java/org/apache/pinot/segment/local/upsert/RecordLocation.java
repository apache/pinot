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
package org.apache.pinot.segment.local.upsert;

import org.apache.pinot.segment.spi.IndexSegment;


/**
 * Indicate a record's location on the local host.
 */
public class RecordLocation {
  private final IndexSegment _segment;
  private final int _docId;
  /** value used to denote the order */
  private final Comparable _comparisonValue;

  public RecordLocation(IndexSegment indexSegment, int docId, Comparable comparisonValue) {
    _segment = indexSegment;
    _docId = docId;
    _comparisonValue = comparisonValue;
  }

  public IndexSegment getSegment() {
    return _segment;
  }

  public int getDocId() {
    return _docId;
  }

  public Comparable getComparisonValue() {
    return _comparisonValue;
  }
}
