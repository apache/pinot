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
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.ScanBasedMultiValueDocIdSet;
import com.linkedin.pinot.core.operator.docidsets.ScanBasedSingleValueDocIdSet;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ScanBasedFilterOperator extends BaseFilterOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScanBasedFilterOperator.class);
  private static final String OPERATOR_NAME = "ScanBasedFilterOperator";

  private final PredicateEvaluator predicateEvaluator;
  private DataSource dataSource;
  private Integer startDocId;
  private Integer endDocId;
  private String name;

  /**
   * @param predicate
   * @param dataSource
   * @param startDocId inclusive
   * @param endDocId inclusive
   */
  public ScanBasedFilterOperator(Predicate predicate, DataSource dataSource, Integer startDocId, Integer endDocId) {
    this.predicateEvaluator = PredicateEvaluatorProvider.getPredicateFunctionFor(predicate, dataSource);
    this.dataSource = dataSource;
    this.startDocId = startDocId;
    this.endDocId = endDocId;
    this.name = ScanBasedFilterOperator.class.getName() + "[" + dataSource.getOperatorName() + "]";
  }

  @Override
  public boolean open() {
    dataSource.open();
    return true;
  }

  @Override
  public BaseFilterBlock nextFilterBlock(BlockId BlockId) {
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    FilterBlockDocIdSet docIdSet;
    Block nextBlock = dataSource.nextBlock();
    BlockValSet blockValueSet = nextBlock.getBlockValueSet();
    BlockMetadata blockMetadata = nextBlock.getMetadata();
    if (dataSourceMetadata.isSingleValue()) {
      docIdSet = new ScanBasedSingleValueDocIdSet(dataSource.getOperatorName(), blockValueSet, blockMetadata,
          predicateEvaluator);
    } else {
      docIdSet = new ScanBasedMultiValueDocIdSet(dataSource.getOperatorName(), blockValueSet, blockMetadata,
          predicateEvaluator);
    }

    if (startDocId != null) {
      docIdSet.setStartDocId(startDocId);
    }

    if (endDocId != null) {
      docIdSet.setEndDocId(endDocId);
    }
    return new ScanBlock(docIdSet);
  }

  @Override
  public boolean isResultEmpty() {
    return predicateEvaluator.alwaysFalse();
  }

  @Override
  public boolean close() {
    dataSource.close();
    return true;
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
    public BlockId getId() {
      return new BlockId(0);
    }

    @Override
    public boolean applyPredicate(Predicate predicate) {
      throw new UnsupportedOperationException("applypredicate not supported in " + this.getClass());
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

  @Override
  public String toString() {
    return ScanBasedFilterOperator.class.getName() + "-" + name;
  }
}
