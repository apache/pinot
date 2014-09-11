package com.linkedin.pinot.core.query.utils;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.Serializable;
import java.util.List;


public class TrieNode {
  private Int2ObjectOpenHashMap<TrieNode> _nextGroupedColumnValues = null;
  private List<Serializable> _aggregationResults = null;
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

  public boolean isLeaf() {
    return _isLeaf;
  }

  public void setIsLeaf(boolean isLeaf) {
    this._isLeaf = isLeaf;
  }

}
