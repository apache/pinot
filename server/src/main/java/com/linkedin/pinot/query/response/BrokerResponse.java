package com.linkedin.pinot.query.response;

import java.io.Serializable;
import java.util.Collection;

import org.json.JSONObject;

import com.linkedin.pinot.index.data.RowEvent;


/**
 * This is the final query response from broker level.
 *
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class BrokerResponse implements Serializable {
  // Response Id.
  private Long _rid;
  // Total documents inside the whole cluster.
  private Integer _totaldocs;
  // Number of Documents meet the filtering condition.
  private Integer _numDocsScanned;
  // Query in Json format.
  private String _parsedQuery;
  // Time used for this query in milliseconds.
  private Long _timeUsedMs;
  // Aggregation query result
  private JSONObject _aggregationResult;
  // Data browser query result
  private Collection<RowEvent> _documentHits;
  // The error code of this query, based on all the errors gathered.
  private Integer _errorCode;
  // Errors from the query execution.
  private InstanceError _error;
  // Segment level query result statistics, triggered by debug mode.
  private Collection<ResultStatistics> _segmentsStatistics;

  public JSONObject getAggregationResult() {
    return _aggregationResult;
  }

  public void setAggregationResult(JSONObject aggregationResult) {
    _aggregationResult = aggregationResult;
  }

  public Long getRid() {
    return _rid;
  }

  public void setRid(Long rid) {
    _rid = rid;
  }

  public Integer getTotaldocs() {
    return _totaldocs;
  }

  public void setTotaldocs(Integer totaldocs) {
    _totaldocs = totaldocs;
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

  public Collection<RowEvent> getDocumentHits() {
    return _documentHits;
  }

  public void setDocumentHits(Collection<RowEvent> documentHits) {
    _documentHits = documentHits;
  }

  public Integer getErrorCode() {
    return _errorCode;
  }

  public void setErrorCode(Integer errorCode) {
    _errorCode = errorCode;
  }

  public InstanceError getError() {
    return _error;
  }

  public void setError(InstanceError error) {
    _error = error;
  }

  public Collection<ResultStatistics> getSegmentsStatistics() {
    return _segmentsStatistics;
  }

  public void setSegmentsStatistics(Collection<ResultStatistics> segmentsStatistics) {
    _segmentsStatistics = segmentsStatistics;
  }

  public JSONObject toJsonObject() {
    JSONObject jsonResult = new JSONObject();
    return jsonResult;
  }
}
