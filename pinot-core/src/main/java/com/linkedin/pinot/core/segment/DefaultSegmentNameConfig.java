/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment;

/**
 * DO NOT remove any getters, setters, or fields from this class otherwise it will be backwards incompatible.
 */
public class DefaultSegmentNameConfig implements SegmentNameConfig {
  private String _timeColumnName;
  private String _tableName;
  private String _segmentNamePostfix;
  private String _segmentName;

  public DefaultSegmentNameConfig(String timeColumnName, String tableName, String segmentNamePostfix) {
    _timeColumnName = timeColumnName;
    _tableName = tableName;
    _segmentNamePostfix = segmentNamePostfix;
  }

  public DefaultSegmentNameConfig(String segmentName) {
    _segmentName = segmentName;
  }

  /**
   * We have this here because this option is available in the current SegmentGeneratorConfig, and we don't want to take
   * away this functionality in case users are setting their own segment names. This option is deprecated and will be
   * removed soon.
   */
  @Deprecated
  public void setSegmentName(String segmentName) {
    _segmentName = segmentName;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getTimeColumnName() {
    return _timeColumnName;
  }

  public String getTableName() {
    return _tableName;
  }

  public String getSegmentNamePostfix() {
    return _segmentNamePostfix;
  }

  public void setTimeColumnName(String timeColumnName) {
    _timeColumnName = timeColumnName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public void setSegmentNamePostfix(String segmentNamePostfix) {
    _segmentNamePostfix = segmentNamePostfix;
  }

}
