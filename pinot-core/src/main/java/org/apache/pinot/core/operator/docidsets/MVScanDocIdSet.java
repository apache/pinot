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

import org.apache.pinot.core.operator.dociditerators.MVScanDocIdIterator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.segment.spi.datasource.DataSource;


public final class MVScanDocIdSet implements FilterBlockDocIdSet {
  private final MVScanDocIdIterator _docIdIterator;

  public MVScanDocIdSet(PredicateEvaluator predicateEvaluator, DataSource dataSource, int numDocs) {
    _docIdIterator = new MVScanDocIdIterator(predicateEvaluator, dataSource, numDocs);
  }

  @Override
  public MVScanDocIdIterator iterator() {
    return _docIdIterator;
  }

  @Override
  public long getNumEntriesScannedInFilter() {
    return _docIdIterator.getNumEntriesScanned();
  }
}
