package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.pinot.segment.local.data.manager.StaleSegment;


public class TableStaleSegmentResponse {
  private final List<StaleSegment> _staleSegmentList;
  private final boolean _isValidResponse;
  private final String _errorMessage;

  @JsonCreator
  public TableStaleSegmentResponse(@JsonProperty("staleSegmentList") List<StaleSegment> staleSegmentList,
      @JsonProperty("validResponse") boolean isValidResponse,
      @JsonProperty("errorMessage") String errorMessage) {
    _staleSegmentList = staleSegmentList;
    _isValidResponse = isValidResponse;
    _errorMessage = errorMessage;
  }

  public TableStaleSegmentResponse(List<StaleSegment> staleSegmentList) {
    _staleSegmentList = staleSegmentList;
    _isValidResponse = true;
    _errorMessage = null;
  }

  public TableStaleSegmentResponse(String errorMessage) {
    _staleSegmentList = null;
    _isValidResponse = false;
    _errorMessage = errorMessage;
  }

  @JsonProperty
  public List<StaleSegment> getStaleSegmentList() {
    return _staleSegmentList;
  }

  @JsonProperty
  public boolean isValidResponse() {
    return _isValidResponse;
  }

  @JsonProperty
  public String getErrorMessage() {
    return _errorMessage;
  }
}
