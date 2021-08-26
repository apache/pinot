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
package org.apache.pinot.controller.recommender.rules.io.configs;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import java.util.HashSet;
import java.util.Set;


/**
 * The output format of index, bloomFilter, and dictionary recommendation,
 * can also be used as input to overwrite index configurations
 */
public class IndexConfig {
  Set<String> _invertedIndexColumns = new HashSet<>();
  Set<String> _rangeIndexColumns = new HashSet<>();
  String _sortedColumn = "";
  Set<String> _bloomFilterColumns = new HashSet<>();

  Set<String> _noDictionaryColumns = new HashSet<>();
  Set<String> _onHeapDictionaryColumns = new HashSet<>();
  Set<String> _varLengthDictionaryColumns = new HashSet<>();

  boolean _isSortedColumnOverwritten = false;

  @JsonSetter(nulls = Nulls.SKIP)
  public void setVariedLengthDictionaryColumns(Set<String> variedLengthDictionaryColumns) {
    _varLengthDictionaryColumns = variedLengthDictionaryColumns;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setBloomFilterColumns(Set<String> bloomFilterColumns) {
    _bloomFilterColumns = bloomFilterColumns;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setNoDictionaryColumns(Set<String> noDictionaryColumns) {
    _noDictionaryColumns = noDictionaryColumns;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setOnHeapDictionaryColumns(Set<String> onHeapDictionaryColumns) {
    _onHeapDictionaryColumns = onHeapDictionaryColumns;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setInvertedIndexColumns(Set<String> invertedIndexColumns) {
    _invertedIndexColumns = invertedIndexColumns;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setSortedColumn(String sortedColumn) {
    _sortedColumn = sortedColumn;
    _isSortedColumnOverwritten = true;
  }

  @JsonSetter(nulls = Nulls.SKIP)
  public void setRangeIndexColumns(Set<String> rangeIndexColumns) {
    _rangeIndexColumns = rangeIndexColumns;
  }

  public boolean isSortedColumnOverwritten() {
    return _isSortedColumnOverwritten;
  }

  public void setSortedColumnOverwritten(boolean sortedColumnOverwritten) {
    _isSortedColumnOverwritten = sortedColumnOverwritten;
  }

  public Set<String> getVarLengthDictionaryColumns() {
    return _varLengthDictionaryColumns;
  }

  public Set<String> getBloomFilterColumns() {
    return _bloomFilterColumns;
  }

  public Set<String> getNoDictionaryColumns() {
    return _noDictionaryColumns;
  }

  public Set<String> getOnHeapDictionaryColumns() {
    return _onHeapDictionaryColumns;
  }

  public Set<String> getInvertedIndexColumns() {
    return _invertedIndexColumns;
  }

  public String getSortedColumn() {
    return _sortedColumn;
  }

  public Set<String> getRangeIndexColumns() {
    return _rangeIndexColumns;
  }

  public boolean hasInvertedIndex(String colname) {
    return _invertedIndexColumns.contains(colname);
  }

  public boolean hasSortedIndex(String colName) {
    return _sortedColumn.equals(colName);
  }

  public boolean hasRangeIndex(String colName) {
    return _rangeIndexColumns.contains(colName);
  }

  public boolean hasAnyIndex() {
    return !_sortedColumn.isEmpty() || !_rangeIndexColumns.isEmpty() || !_invertedIndexColumns.isEmpty();
  }
}
