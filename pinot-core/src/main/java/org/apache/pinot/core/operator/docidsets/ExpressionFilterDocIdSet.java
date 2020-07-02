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
package org.apache.pinot.core.operator.docidsets;

import java.util.Map;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.dociditerators.ExpressionScanDocIdIterator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.transform.function.TransformFunction;


public class ExpressionFilterDocIdSet implements ScanBasedDocIdSet {
  private final TransformFunction _transformFunction;
  private final PredicateEvaluator _predicateEvaluator;
  private final Map<String, DataSource> _dataSourceMap;

  private int _startDocId;
  private int _endDocId;
  private ExpressionScanDocIdIterator _iterator;

  public ExpressionFilterDocIdSet(TransformFunction transformFunction, PredicateEvaluator predicateEvaluator,
      Map<String, DataSource> dataSourceMap, int numDocs) {
    _transformFunction = transformFunction;
    _predicateEvaluator = predicateEvaluator;
    _dataSourceMap = dataSourceMap;
    _startDocId = 0;
    _endDocId = numDocs;
  }

  @Override
  public int getMinDocId() {
    return _startDocId;
  }

  @Override
  public int getMaxDocId() {
    // NOTE: Return value is inclusive
    return _endDocId - 1;
  }

  @Override
  public void setStartDocId(int startDocId) {
    _startDocId = startDocId;
  }

  @Override
  public void setEndDocId(int endDocId) {
    // NOTE: The passed in argument is inclusive
    _endDocId = endDocId + 1;
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return _iterator.getNumEntriesScanned();
  }

  @Override
  public ExpressionScanDocIdIterator iterator() {
    _iterator = new ExpressionScanDocIdIterator(_transformFunction, _predicateEvaluator, _dataSourceMap, _startDocId,
        _endDocId);
    return _iterator;
  }

  @Override
  public <T> T getRaw() {
    throw new UnsupportedOperationException();
  }
}
