package com.linkedin.pinot.query.response;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import com.linkedin.pinot.index.data.RowEvent;
import com.linkedin.pinot.query.aggregation.AggregationResult;


/**
 * This is the partial query response from instance level, will be gathered by broker.
 *
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class InstanceResponse implements Serializable {
  // Response Id.
  private Long _rid;
  // Total documents inside this instance.
  private Integer _totaldocs;
  // Number of Documents meet the filtering condition.
  private Integer _numDocsScanned;
  // Query in Json format.
  private String _parsedQuery;
  // Time used for this query in milliseconds.
  private Long _timeUsedMs;
  // Aggregation query result
  private List<List<AggregationResult>> _aggregationResults;
  // Data browser query result
  private Collection<RowEvent> _documentHits;
  // Errors from the query execution.
  private Error _error;
  // Segment level query result statistics, triggered by debug mode.
  private Collection<ResultStatistics> _segmentsStatistics;

  public Long getRid() {
    return _rid;
  }

  public void setRid(Long rid) {
    _rid = rid;
  }

  public Integer getNumDocsScanned() {
    return _numDocsScanned;
  }

  public void setNumDocsScanned(Integer numDocsScanned) {
    _numDocsScanned = numDocsScanned;
  }

  public String getParsedQuery() {
    return _parsedQuery;
  }

  public void setParsedQuery(String parsedQuery) {
    _parsedQuery = parsedQuery;
  }

  public Long getTimeUsedMs() {
    return _timeUsedMs;
  }

  public void setTimeUsedMs(Long timeUsedMs) {
    _timeUsedMs = timeUsedMs;
  }

  public List<List<AggregationResult>> getAggregationResults() {
    return _aggregationResults;
  }

  public void setAggregationResults(List<List<AggregationResult>> aggregationResults) {
    _aggregationResults = aggregationResults;
  }

  public Collection<RowEvent> getDocumentHits() {
    return _documentHits;
  }

  public void setDocumentHits(Collection<RowEvent> documentHits) {
    _documentHits = documentHits;
  }

  public Error getError() {
    return _error;
  }

  public void setError(Error error) {
    _error = error;
  }

  public Collection<ResultStatistics> getSegmentsStatistics() {
    return _segmentsStatistics;
  }

  public void setSegmentsStatistics(Collection<ResultStatistics> segmentsStatistics) {
    _segmentsStatistics = segmentsStatistics;
  }

  public Integer getTotaldocs() {
    return _totaldocs;
  }

  public void setTotaldocs(Integer totaldocs) {
    _totaldocs = totaldocs;
  }

}
