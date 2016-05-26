/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.operator.aggregation.groupby;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.docidsets.DocIdSetBlock;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionFactory;
import java.util.List;


/**
 * Operator class for implementing aggregation group by functionality.
 * Extends the BaseOperator class.
 *
 */
public class AggregationGroupByOperator extends BaseOperator {

  GroupByExecutor _groupByExecutor;
  private List<AggregationInfo> _aggregationInfoList;
  private MProjectionOperator _projectionOperator;
  private IndexSegment _indexSegment;
  private int _nextBlockCallCounter = 0;

  private int[] _reusableDocIdSet;

  /**
   * Constructor for the class.
   *
   * @param indexSegment Index on which aggregation group by is to be performed.
   * @param aggregationsInfoList List of AggregationInfo (contains context for applying aggregation functions).
   * @param groupBy GroupBy to perform
   * @param projectionOperator Projection
   */
  public AggregationGroupByOperator(IndexSegment indexSegment, List<AggregationInfo> aggregationsInfoList,
      GroupBy groupBy, MProjectionOperator projectionOperator) {

    Preconditions.checkNotNull(indexSegment);
    Preconditions.checkArgument((aggregationsInfoList != null) && (aggregationsInfoList.size() > 0));
    Preconditions.checkNotNull(groupBy);
    Preconditions.checkNotNull(projectionOperator);

    _indexSegment = indexSegment;
    _aggregationInfoList = aggregationsInfoList;
    _projectionOperator = projectionOperator;
    _groupByExecutor = GroupByExecutorFactory.getGroupByExecutor(indexSegment, aggregationsInfoList, groupBy);
  }

  /**
   * Returns the next ResultBlock containing the result of aggregation group by.
   * @return
   */
  @Override
  public Block getNextBlock() {
    return getNextBlock(new BlockId(_nextBlockCallCounter++));
  }

  /**
   * This method is currently not supported.
   */
  @Override
  public Block getNextBlock(BlockId blockId) {
    if (blockId.getId() > 0) {
      return null;
    }
    final long startTimeMillis = System.currentTimeMillis();
    int numDocsScanned = 0;

    _groupByExecutor.init();
    while (_projectionOperator.nextBlock() != null) {
      ProjectionBlock currentBlock = _projectionOperator.getCurrentBlock();
      Block block = currentBlock.getDocIdSetBlock();
      numDocsScanned = processBlock(numDocsScanned, currentBlock, block);
    }
    _groupByExecutor.finish();

    AggregationGroupByResult aggregationGroupByResult = _groupByExecutor.getResult();
    final IntermediateResultsBlock resultBlock =
        new IntermediateResultsBlock(AggregationFunctionFactory.getAggregationFunction(_aggregationInfoList),
            aggregationGroupByResult);

    resultBlock.setNumDocsScanned(numDocsScanned);
    resultBlock.setTotalRawDocs(_indexSegment.getSegmentMetadata().getTotalRawDocs());
    resultBlock.setTimeUsedMs(System.currentTimeMillis() - startTimeMillis);

    return resultBlock;
  }

  /**
   * Process a block of docIdSets. If the passed in block is an instance of DocIdSetBlock,
   * get the docIdSet array directly from it. Else, iterate over all docIds and call the
   * groupByExecutor after each 5k docIds.
   *
   * @param numDocsScanned
   * @param currentBlock
   * @param block
   * @return
   */
  private int processBlock(int numDocsScanned, ProjectionBlock currentBlock, Block block) {
    if (block instanceof DocIdSetBlock) {
      DocIdSetBlock docIdSetBlock = (DocIdSetBlock) block;
      int length = docIdSetBlock.getSearchableLength();

      _groupByExecutor.process(docIdSetBlock.getDocIdSet(), 0, length);
      numDocsScanned += length;
    } else {
      if (_reusableDocIdSet == null) {
        _reusableDocIdSet = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];
      }

      BlockDocIdSet blockDocIdSet = currentBlock.getBlockDocIdSet();
      BlockDocIdIterator iterator = blockDocIdSet.iterator();

      int docId;
      int pos = 0;
      while ((docId = iterator.next()) != Constants.EOF) {
        _reusableDocIdSet[pos++] = docId;
        if (pos == DocIdSetPlanNode.MAX_DOC_PER_CALL) {
          numDocsScanned += pos;
          _groupByExecutor.process(_reusableDocIdSet, 0, pos);
          pos = 0;
        }
      }

      if (pos > 0) {
        _groupByExecutor.process(_reusableDocIdSet, 0, pos);
        numDocsScanned += pos;
      }
    }
    return numDocsScanned;
  }

  @Override
  public String getOperatorName() {
    return "GroupByAggregationOperator";
  }

  @Override
  public boolean open() {
    _projectionOperator.open();
    return true;
  }

  @Override
  public boolean close() {
    _projectionOperator.close();
    return true;
  }
}
