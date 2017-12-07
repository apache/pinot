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
package com.linkedin.pinot.core.data.readers;

import java.util.Set;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codehaus.jackson.annotate.JsonIgnore;


@SuppressWarnings("unused")
public class CSVRecordReaderConfig implements RecordReaderConfig {
  private String _fileFormat;
  private String _header;
  private char _delimiter = ',';
  private char _multiValueDelimiter = ';';
  private String _dateFormat;
  private Set<String> _dateColumns;

  public String getFileFormat() {
    return _fileFormat;
  }

  public void setFileFormat(String fileFormat) {
    _fileFormat = fileFormat;
  }

  public String getHeader() {
    return _header;
  }

  public void setHeader(String header) {
    _header = header;
  }

  public char getDelimiter() {
    return _delimiter;
  }

  public void setDelimiter(char delimiter) {
    _delimiter = delimiter;
  }

  public char getMultiValueDelimiter() {
    return _multiValueDelimiter;
  }

  public void setMultiValueDelimiter(char multiValueDelimiter) {
    _multiValueDelimiter = multiValueDelimiter;
  }

  public String getDateFormat() {
    return _dateFormat;
  }

  public void setDateFormat(String dateFormat) {
    _dateFormat = dateFormat;
  }

  public Set<String> getDateColumns() {
    return _dateColumns;
  }

  public void setDateColumns(Set<String> dateColumns) {
    _dateColumns = dateColumns;
  }

  @JsonIgnore
  public boolean isDataColumn(String columnName) {
    return _dateColumns.contains(columnName);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
