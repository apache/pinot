package com.linkedin.pinot.core.block.query;

import java.io.Serializable;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;


public class AggregationResultBlock implements Block {

  private Serializable _aggregationResult;

  public AggregationResultBlock(Serializable aggregationResult) {
    _aggregationResult = aggregationResult;
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

  public Serializable getAggregationResult() {
    return _aggregationResult;
  }

  public void setAggregationResult(Serializable aggregationResult) {
    _aggregationResult = aggregationResult;
  }
}
