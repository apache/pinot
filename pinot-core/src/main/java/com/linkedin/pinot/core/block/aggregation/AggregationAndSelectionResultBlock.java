package com.linkedin.pinot.core.block.aggregation;

import java.util.List;
import java.util.Map;

import com.linkedin.pinot.common.response.AggregationResult;
import com.linkedin.pinot.common.response.ProcessingException;
import com.linkedin.pinot.common.response.ResponseStatistics;
import com.linkedin.pinot.common.response.RowEvent;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Predicate;


/**
 * A holder of InstanceResponse components. Easy to do merge.
 * 
 * @author xiafu
 *
 */
public class AggregationAndSelectionResultBlock implements Block {

  private List<AggregationResult> _aggregationResultList;
  private List<ProcessingException> _processingExceptions;
  private long _numDocsScanned;
  private long _requestId;
  private List<RowEvent> _rowEvents;
  private List<ResponseStatistics> _segmentStatistics;
  private long _timeUsedMs;
  private long _totalDocs;
  private Map<String, String> _traceInfo;

  public AggregationAndSelectionResultBlock(List<AggregationResult> aggregationResult) {
    _aggregationResultList = aggregationResult;
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

  public List<AggregationResult> getAggregationResult() {
    return _aggregationResultList;
  }

  public void setAggregationResults(List<AggregationResult> aggregationResults) {
    _aggregationResultList = aggregationResults;
  }

  public List<ProcessingException> getExceptions() {
    return _processingExceptions;
  }

  public long getNumDocsScanned() {
    return _numDocsScanned;
  }

  public long getRequestId() {
    return _requestId;
  }

  public List<RowEvent> getRowEvents() {
    return _rowEvents;
  }

  public List<ResponseStatistics> getSegmentStatistics() {
    return _segmentStatistics;
  }

  public long getTimeUsedMs() {
    return _timeUsedMs;
  }

  public long getTotalDocs() {
    return _totalDocs;
  }

  public Map<String, String> getTraceInfo() {
    return _traceInfo;
  }

  public void setExceptionsList(List<ProcessingException> processingExceptions) {
    _processingExceptions = processingExceptions;
  }

  public void setNumDocsScanned(long numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  public void setRequestId(long requestId) {
    _requestId = requestId;
  }

  public void setRowEvents(List<RowEvent> rowEvents) {
    _rowEvents = rowEvents;
  }

  public void setSegmentStatistics(List<ResponseStatistics> segmentStatistics) {
    _segmentStatistics = segmentStatistics;
  }

  public void setTimeUsedMs(long timeUsedMs) {
    _timeUsedMs = timeUsedMs;
  }

  public void setTotalDocs(long totalDocs) {
    _totalDocs = totalDocs;
  }

  public void setTraceInfo(Map<String, String> traceInfo) {
    _traceInfo = traceInfo;
  }
}
