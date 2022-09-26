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
package org.apache.pinot.segment.local.realtime.converter;

import java.util.List;


/**
 * Container class for holding all column indices necessary for realtime table
 */
public class ColumnIndicesForRealtimeTable {
  private final String _sortedColumn;
  private final List<String> _invertedIndexColumns;
  private final List<String> _textIndexColumns;
  private final List<String> _fstIndexColumns;
  private final List<String> _noDictionaryColumns;
  private final List<String> _varLengthDictionaryColumns;

  public ColumnIndicesForRealtimeTable(String sortedColumn, List<String> invertedIndexColumns,
      List<String> textIndexColumns, List<String> fstIndexColumns, List<String> noDictionaryColumns,
      List<String> varLengthDictionaryColumns) {
    _sortedColumn = sortedColumn;
    _invertedIndexColumns = invertedIndexColumns;
    _textIndexColumns = textIndexColumns;
    _fstIndexColumns = fstIndexColumns;
    _noDictionaryColumns = noDictionaryColumns;
    _varLengthDictionaryColumns = varLengthDictionaryColumns;
  }

  public String getSortedColumn() {
    return _sortedColumn;
  }

  public List<String> getInvertedIndexColumns() {
    return _invertedIndexColumns;
  }

  public List<String> getTextIndexColumns() {
    return _textIndexColumns;
  }

  public List<String> getFstIndexColumns() {
    return _fstIndexColumns;
  }

  public List<String> getNoDictionaryColumns() {
    return _noDictionaryColumns;
  }

  public List<String> getVarLengthDictionaryColumns() {
    return _varLengthDictionaryColumns;
  }
}
