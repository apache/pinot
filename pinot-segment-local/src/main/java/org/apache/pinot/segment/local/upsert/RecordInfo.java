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

import org.apache.pinot.spi.data.readers.PrimaryKey;


@SuppressWarnings("rawtypes")
public class RecordInfo {
  private final PrimaryKey _primaryKey;
  private final int _docId;
  private final Comparable _comparisonValue;
  private final boolean _deleteRecord;

  public RecordInfo(PrimaryKey primaryKey, int docId, Comparable comparisonValue, boolean deleteRecord) {
    _primaryKey = primaryKey;
    _docId = docId;
    _comparisonValue = comparisonValue;
    _deleteRecord = deleteRecord;
  }

  public PrimaryKey getPrimaryKey() {
    return _primaryKey;
  }

  public int getDocId() {
    return _docId;
  }

  public Comparable getComparisonValue() {
    return _comparisonValue;
  }

  public boolean isDeleteRecord() {
    return _deleteRecord;
  }
}
