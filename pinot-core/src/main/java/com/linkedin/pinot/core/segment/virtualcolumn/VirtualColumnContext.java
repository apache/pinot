/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.virtualcolumn;

/**
 * Context used to pass arguments to the virtual column provider.
 */
public class VirtualColumnContext {
  private String _hostname;
  private String _tableName;
  private String _segmentName;
  private String _columnName;
  private int _totalDocCount;

  public VirtualColumnContext(String hostname, String tableName, String segmentName, String columnName,
      int totalDocCount) {
    _hostname = hostname;
    _tableName = tableName;
    _segmentName = segmentName;
    _columnName = columnName;
    _totalDocCount = totalDocCount;
  }

  public String getHostname() {
    return _hostname;
  }

  public String getTableName() {
    return _tableName;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getColumnName() {
    return _columnName;
  }

  public int getTotalDocCount() {
    return _totalDocCount;
  }
}
