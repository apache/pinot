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

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.codehaus.jackson.annotate.JsonProperty;


public class CSVRecordReaderConfig implements RecordReaderConfig {
  private String _csvFileFormat;
  private String _csvHeader;
  private String _csvDelimiter;
  private String _csvDateFormat;
  private HashSet<String> _csvDateColumns;

  public CSVRecordReaderConfig() {
    _csvDelimiter = ",";
  }

  @JsonProperty("CsvFileFormat")
  public String getCsvFileFormat() {
    return _csvFileFormat;
  }

  @JsonProperty("CsvFileFormat")
  public CSVRecordReaderConfig setCsvFileFormat(String csvFileFormat) {
    _csvFileFormat = csvFileFormat;
    return this;
  }

  @JsonProperty("CsvHeader")
  public String getCsvHeader() {
    return _csvHeader;
  }

  @JsonProperty("CsvHeader")
  public CSVRecordReaderConfig setCsvHeader(String csvHeader) {
    _csvHeader = csvHeader;
    return this;
  }

  @JsonProperty("CsvDelimiter")
  public String getCsvDelimiter() {
    return _csvDelimiter;
  }

  @JsonProperty("CsvDelimiter")
  public CSVRecordReaderConfig setCsvDelimiter(String csvDelimiter) {
    _csvDelimiter = csvDelimiter;
    return this;
  }

  @JsonProperty("CsvDateFormat")
  public String getCsvDateFormat() {
    return _csvDateFormat;
  }

  @JsonProperty("CsvDateFormat")
  public CSVRecordReaderConfig setCsvDateFormat(String csvDateFormat) {
    _csvDateFormat = csvDateFormat;
    return this;
  }

  @JsonProperty("CsvDateColumns")
  public Set<String> getCsvDateColumns() {
    return _csvDateColumns;
  }

  @JsonProperty("CsvDateColumns")
  public void setCsvDateColumns(HashSet<String> csvDateColumns) {
    _csvDateColumns = csvDateColumns;
  }

  public boolean columnIsDate(String column) {
    return ((_csvDateColumns != null) && (_csvDateColumns.contains(column)));
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
