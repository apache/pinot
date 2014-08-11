package com.linkedin.pinot.core.block.aggregation;

import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;


/**
 * InstanceResponseBlock is just a holder to get InstanceResponse from InstanceResponseBlock.
 * 
 * @author xiafu
 *
 */
public class InstanceResponseBlock implements Block {
  private final InstanceResponse _instanceResponse;
  private final AggregationAndSelectionResultBlock _aggregationAndSelectionResultBlock;

  public InstanceResponseBlock(Block block) {
    _instanceResponse = new InstanceResponse();
    _aggregationAndSelectionResultBlock = (AggregationAndSelectionResultBlock) block;
    init();
  }

  private void init() {
    _instanceResponse.setAggregationResults(_aggregationAndSelectionResultBlock.getAggregationResult());
    _instanceResponse.setExceptions(_aggregationAndSelectionResultBlock.getExceptions());
    _instanceResponse.setNumDocsScanned(_aggregationAndSelectionResultBlock.getNumDocsScanned());
    _instanceResponse.setRequestId(_aggregationAndSelectionResultBlock.getRequestId());
    _instanceResponse.setRowEvents(_aggregationAndSelectionResultBlock.getRowEvents());
    _instanceResponse.setSegmentStatistics(_aggregationAndSelectionResultBlock.getSegmentStatistics());
    _instanceResponse.setTimeUsedMs(_aggregationAndSelectionResultBlock.getTimeUsedMs());
    _instanceResponse.setTotalDocs(_aggregationAndSelectionResultBlock.getTotalDocs());
    _instanceResponse.setTraceInfo(_aggregationAndSelectionResultBlock.getTraceInfo());
  }

  public InstanceResponse getInstanceResponse() {
    return _instanceResponse;
  }

  @Override
  public boolean applyPredicate(Predicate predicate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockId getId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockValSet getBlockValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdValueSet getBlockDocIdValueSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockDocIdSet getBlockDocIdSet() {
    throw new UnsupportedOperationException();
  }

  @Override
  public BlockMetadata getMetadata() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getIntValue(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public float getFloatValue(int docId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resetBlock() {
    throw new UnsupportedOperationException();
  }

}
