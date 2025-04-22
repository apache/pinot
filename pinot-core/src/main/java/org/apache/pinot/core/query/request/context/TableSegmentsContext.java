package org.apache.pinot.core.query.request.context;

import java.util.List;


public class TableSegmentsContext {
  private final String _tableName;
  private final List<String> _segments;
  private final List<String> _optionalSegments;

  public TableSegmentsContext(String tableName, List<String> segments, List<String> optionalSegments) {
    _tableName = tableName;
    _segments = segments;
    _optionalSegments = optionalSegments;
  }

  public String getTableName() {
    return _tableName;
  }

  public List<String> getSegments() {
    return _segments;
  }

  public List<String> getOptionalSegments() {
    return _optionalSegments;
  }
}
