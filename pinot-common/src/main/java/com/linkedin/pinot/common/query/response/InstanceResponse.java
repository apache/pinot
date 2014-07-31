package com.linkedin.pinot.common.query.response;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.linkedin.pinot.common.data.RowEvent;


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
  private InstanceError _error;
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

  public Integer getTotaldocs() {
    return _totaldocs;
  }

  public void setTotaldocs(Integer totaldocs) {
    _totaldocs = totaldocs;
  }

  public JSONObject toJSON() throws JSONException {
    JSONObject ret = new JSONObject();
    ret.put("rid", _rid);
    ret.put("numDocs", _totaldocs);
    ret.put("numScanned", _numDocsScanned);
    ret.put("parsedQuery", _parsedQuery);
    ret.put("timeUsedMs", _timeUsedMs);
    ret.put("aggregationResults", _aggregationResults);
    ret.put("documentHits", _documentHits);
    ret.put("error", _error);
    ret.put("segmentsStatistics", _segmentsStatistics);
    return ret;
  }
  
  @Override
  public String toString() {
    return "InstanceResponse [_rid=" + _rid + ", _totaldocs=" + _totaldocs + ", _numDocsScanned=" + _numDocsScanned
        + ", _parsedQuery=" + _parsedQuery + ", _timeUsedMs=" + _timeUsedMs + ", _aggregationResults="
        + _aggregationResults + ", _documentHits=" + _documentHits + ", _error=" + _error + ", _segmentsStatistics="
        + _segmentsStatistics + "]";
  }

}
