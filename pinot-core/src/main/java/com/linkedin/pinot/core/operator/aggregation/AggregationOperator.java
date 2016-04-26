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
package com.linkedin.pinot.core.operator.aggregation;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.AggregationInfo;
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
import java.io.Serializable;
import java.util.List;


/**
 * This class implements the aggregation operator, extends BaseOperator.
 */
public class AggregationOperator extends BaseOperator {
  private static final String OPERATOR_NAME = "AggregationOperator";

  AggregationExecutor _aggregationExecutor;
  private List<AggregationInfo> _aggregationInfoList;
  private MProjectionOperator _projectionOperator;

  private IndexSegment _indexSegment;
  private int _nextBlockCallCounter = 0;

  /**
   * Constructor for the class.
   *
   * @param indexSegment Index on which aggregation group by is to be performed.
   * @param aggregationsInfoList List of AggregationInfo (contains context for applying aggregation functions).
   * @param projectionOperator Projection
   */
  public AggregationOperator(IndexSegment indexSegment, List<AggregationInfo> aggregationsInfoList,
      MProjectionOperator projectionOperator) {

    Preconditions.checkNotNull(indexSegment);
    Preconditions.checkArgument((aggregationsInfoList != null) && (aggregationsInfoList.size() > 0));
    Preconditions.checkNotNull(projectionOperator);

    _indexSegment = indexSegment;
    _aggregationInfoList = aggregationsInfoList;
    _projectionOperator = projectionOperator;
    _aggregationExecutor = new DefaultAggregationExecutor(indexSegment, _aggregationInfoList);
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
   * {@inheritDoc}
   * Returns the nextBlock for the given docId.
   */
  @Override
  public Block getNextBlock(BlockId blockId) {
    if (blockId.getId() > 0) {
      return null;
    }
    final long startTimeMillis = System.currentTimeMillis();
    int numDocsScanned = 0;

    // Perform aggregation on all the blocks.
    _aggregationExecutor.init();
    while (_projectionOperator.nextBlock() != null) {
      ProjectionBlock currentBlock = _projectionOperator.getCurrentBlock();
      Block block = currentBlock.getDocIdSetBlock();
      numDocsScanned = processBlock(numDocsScanned, currentBlock, block);
    }
    _aggregationExecutor.finish();

    // Build intermediate result block based on aggregation result from the executor.
    List<Serializable> aggregationResults = _aggregationExecutor.getResult();
    final IntermediateResultsBlock resultBlock =
        new IntermediateResultsBlock(AggregationFunctionFactory.getAggregationFunction(_aggregationInfoList),
            aggregationResults);

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
    int[] docIdSet;

    if (block instanceof DocIdSetBlock) {
      DocIdSetBlock docIdSetBlock = (DocIdSetBlock) block;
      docIdSet = docIdSetBlock.getDocIdSet();

      _aggregationExecutor.aggregate(docIdSet, 0, docIdSetBlock.getSearchableLength());
      numDocsScanned += ((DocIdSetBlock) block).getSearchableLength();
    } else {
      docIdSet = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];

      BlockDocIdSet blockDocIdSet = currentBlock.getBlockDocIdSet();
      BlockDocIdIterator iterator = blockDocIdSet.iterator();

      int docId;
      int pos = 0;
      while ((docId = iterator.next()) != Constants.EOF) {
        docIdSet[pos++] = docId;
        if (pos == docIdSet.length) {
          numDocsScanned += pos;
          pos = 0;
          _aggregationExecutor.aggregate(docIdSet, 0, pos);
        }
      }

      if (pos > 0) {
        _aggregationExecutor.aggregate(docIdSet, 0, pos);
        numDocsScanned += pos;
      }
    }
    return numDocsScanned;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
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
