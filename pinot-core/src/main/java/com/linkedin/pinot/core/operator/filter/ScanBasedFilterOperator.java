/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.filter;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.ScanBasedMultiValueDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.ScanBasedSingleValueDocIdSet;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;


public class ScanBasedFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "ScanBasedFilterOperator";

  private final PredicateEvaluator _predicateEvaluator;
  private final DataSource _dataSource;
  private final int _startDocId;
  // TODO: change it to exclusive
  // Inclusive
  private final int _endDocId;

  public ScanBasedFilterOperator(PredicateEvaluator predicateEvaluator, DataSource dataSource, int startDocId,
      int endDocId) {
    _predicateEvaluator = predicateEvaluator;
    _dataSource = dataSource;
    _startDocId = startDocId;
    _endDocId = endDocId;
  }

  @Override
  protected BaseFilterBlock getNextBlock() {
    DataSourceMetadata dataSourceMetadata = _dataSource.getDataSourceMetadata();
    FilterBlockDocIdSet docIdSet;
    Block nextBlock = _dataSource.nextBlock();
    BlockValSet blockValueSet = nextBlock.getBlockValueSet();
    BlockMetadata blockMetadata = nextBlock.getMetadata();
    if (dataSourceMetadata.isSingleValue()) {
      docIdSet = new ScanBasedSingleValueDocIdSet(_dataSource.getOperatorName(), blockValueSet, blockMetadata,
          _predicateEvaluator);
    } else {
      docIdSet = new ScanBasedMultiValueDocIdSet(_dataSource.getOperatorName(), blockValueSet, blockMetadata,
          _predicateEvaluator);
    }

    docIdSet.setStartDocId(_startDocId);
    docIdSet.setEndDocId(_endDocId);
    return new ScanBlock(docIdSet);
  }

  @Override
  public boolean isResultEmpty() {
    return _predicateEvaluator.isAlwaysFalse();
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  public static class ScanBlock extends BaseFilterBlock {

    private FilterBlockDocIdSet docIdSet;

    public ScanBlock(FilterBlockDocIdSet docIdSet) {
      this.docIdSet = docIdSet;
    }

    @Override
    public BlockValSet getBlockValueSet() {
      throw new UnsupportedOperationException("getBlockValueSet not supported in " + this.getClass());
    }

    @Override
    public BlockDocIdValueSet getBlockDocIdValueSet() {
      throw new UnsupportedOperationException("getBlockDocIdValueSet not supported in " + this.getClass());
    }

    @Override
    public BlockMetadata getMetadata() {
      throw new UnsupportedOperationException("getMetadata not supported in " + this.getClass());
    }

    @Override
    public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
      return docIdSet;
    }
  }
}
