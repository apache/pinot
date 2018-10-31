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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


@SuppressWarnings("unused")
public class CSVRecordReaderConfig implements RecordReaderConfig {
  public static final char DEFAULT_DELIMITER = ',';
  public static final char DEFAULT_MULTI_VALUE_DELIMITER = ';';

  private String _fileFormat;
  private String _header;
  private char _delimiter = DEFAULT_DELIMITER;
  private char _multiValueDelimiter = DEFAULT_MULTI_VALUE_DELIMITER;

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

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
