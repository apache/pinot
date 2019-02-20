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
package org.apache.pinot.core.query.utils;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.Serializable;
import java.util.List;


public class TrieNode {
  private Int2ObjectOpenHashMap<TrieNode> _nextGroupedColumnValues = null;
  private List<Serializable> _aggregationResults = null;
  private Serializable _aggregationResult = null;
  private boolean _isLeaf = false;

  public Int2ObjectOpenHashMap<TrieNode> getNextGroupedColumnValues() {
    return _nextGroupedColumnValues;
  }

  public void setNextGroupedColumnValues(Int2ObjectOpenHashMap<TrieNode> nextGroupedColumnValues) {
    this._nextGroupedColumnValues = nextGroupedColumnValues;
  }

  public List<Serializable> getAggregationResults() {
    return _aggregationResults;
  }

  public void setAggregationResults(List<Serializable> aggregationResults) {
    this._aggregationResults = aggregationResults;
  }

  public Serializable getAggregationResult() {
    return _aggregationResult;
  }

  public void setAggregationResult(Serializable aggregationResult) {
    _aggregationResult = aggregationResult;
  }

  public boolean isLeaf() {
    return _isLeaf;
  }

  public void setIsLeaf(boolean isLeaf) {
    this._isLeaf = isLeaf;
  }
}
