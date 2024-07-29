/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.inputformat.csv;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


@SuppressWarnings("unused")
public class CSVRecordReaderConfig implements RecordReaderConfig {
  public static final char DEFAULT_DELIMITER = ',';
  public static final char DEFAULT_MULTI_VALUE_DELIMITER = ';';

  private String _fileFormat;
  private String _header;
  private char _delimiter = DEFAULT_DELIMITER;
  private char _multiValueDelimiter = DEFAULT_MULTI_VALUE_DELIMITER;
  private boolean _multiValueDelimiterEnabled = true; // when false, skip parsing for multiple values
  private Character _commentMarker;   // Default is null
  private Character _escapeCharacter; // Default is null
  private String _nullStringValue;
  private boolean _skipHeader;
  private boolean _skipUnParseableLines = false;
  private boolean _ignoreEmptyLines = true;
  private boolean _ignoreSurroundingSpaces = true;
  private Character _quoteCharacter = '"';
  private String _quoteMode;
  private String _recordSeparator;


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

  public boolean isSkipUnParseableLines() {
    return _skipUnParseableLines;
  }

  public void setSkipUnParseableLines(boolean skipUnParseableLines) {
    _skipUnParseableLines = skipUnParseableLines;
  }

  public boolean isMultiValueDelimiterEnabled() {
    return _multiValueDelimiterEnabled;
  }

  public void setMultiValueDelimiterEnabled(boolean multiValueDelimiterEnabled) {
    _multiValueDelimiterEnabled = multiValueDelimiterEnabled;
  }

  public Character getCommentMarker() {
    return _commentMarker;
  }

  public void setCommentMarker(Character commentMarker) {
    _commentMarker = commentMarker;
  }

  public Character getEscapeCharacter() {
    return _escapeCharacter;
  }

  public void setEscapeCharacter(Character escapeCharacter) {
    _escapeCharacter = escapeCharacter;
  }

  public String getNullStringValue() {
    return _nullStringValue;
  }

  public void setNullStringValue(String nullStringValue) {
    _nullStringValue = nullStringValue;
  }

  public boolean isSkipHeader() {
    return _skipHeader;
  }

  public void setSkipHeader(boolean skipHeader) {
    _skipHeader = skipHeader;
  }

  public boolean isIgnoreEmptyLines() {
    return _ignoreEmptyLines;
  }

  public void setIgnoreEmptyLines(boolean ignoreEmptyLines) {
    _ignoreEmptyLines = ignoreEmptyLines;
  }

  public boolean isIgnoreSurroundingSpaces() {
    return _ignoreSurroundingSpaces;
  }

  public void setIgnoreSurroundingSpaces(boolean ignoreSurroundingSpaces) {
    _ignoreSurroundingSpaces = ignoreSurroundingSpaces;
  }

  public Character getQuoteCharacter() {
    return _quoteCharacter;
  }

  public void setQuoteCharacter(Character quoteCharacter) {
    _quoteCharacter = quoteCharacter;
  }

  public String getQuoteMode() {
    return _quoteMode;
  }

  public void setQuoteMode(String quoteMode) {
    _quoteMode = quoteMode;
  }

  public String getRecordSeparator() {
    return _recordSeparator;
  }

  public void setRecordSeparator(String recordSeparator) {
    _recordSeparator = recordSeparator;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
  }
}
