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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;


/**
 * Record reader for CSV file.
 */
public class CSVRecordReader implements RecordReader {
  private File _dataFile;
  private CSVFormat _format;
  private CSVParser _parser;
  private Iterator<CSVRecord> _iterator;
  private CSVRecordExtractor _recordExtractor;

  public CSVRecordReader() {
  }

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _dataFile = dataFile;
    CSVRecordReaderConfig config = (CSVRecordReaderConfig) recordReaderConfig;
    Character multiValueDelimiter = null;
    if (config == null) {
      _format = CSVFormat.DEFAULT.withDelimiter(CSVRecordReaderConfig.DEFAULT_DELIMITER).withHeader();
      multiValueDelimiter = CSVRecordReaderConfig.DEFAULT_MULTI_VALUE_DELIMITER;
    } else {
      CSVFormat format;
      String formatString = config.getFileFormat();
      if (formatString == null) {
        format = CSVFormat.DEFAULT;
      } else {
        switch (formatString.toUpperCase()) {
          case "EXCEL":
            format = CSVFormat.EXCEL;
            break;
          case "MYSQL":
            format = CSVFormat.MYSQL;
            break;
          case "RFC4180":
            format = CSVFormat.RFC4180;
            break;
          case "TDF":
            format = CSVFormat.TDF;
            break;
          default:
            format = CSVFormat.DEFAULT;
            break;
        }
      }
      char delimiter = config.getDelimiter();
      format = format.withDelimiter(delimiter);
      String csvHeader = config.getHeader();
      if (csvHeader == null) {
        format = format.withHeader();
      } else {
        //validate header for the delimiter before splitting
        validateHeaderForDelimiter(delimiter, csvHeader, format);
        format = format.withHeader(StringUtils.split(csvHeader, delimiter));
      }

      format = format.withCommentMarker(config.getCommentMarker());
      format = format.withEscape(config.getEscapeCharacter());
      format = format.withIgnoreEmptyLines(config.isIgnoreEmptyLines());
      format = format.withIgnoreSurroundingSpaces(config.isIgnoreSurroundingSpaces());
      format = format.withSkipHeaderRecord(config.isSkipHeader());
      format = format.withQuote(config.getQuoteCharacter());

      if (config.getQuoteMode() != null) {
        format = format.withQuoteMode(QuoteMode.valueOf(config.getQuoteMode()));
      }

      if (config.getRecordSeparator() != null) {
        format = format.withRecordSeparator(config.getRecordSeparator());
      }

      String nullString = config.getNullStringValue();
      if (nullString != null) {
        format = format.withNullString(nullString);
      }

      _format = format;
      if (config.isMultiValueDelimiterEnabled()) {
        multiValueDelimiter = config.getMultiValueDelimiter();
      }
    }
    _recordExtractor = new CSVRecordExtractor();

    init();

    CSVRecordExtractorConfig recordExtractorConfig = new CSVRecordExtractorConfig();
    recordExtractorConfig.setMultiValueDelimiter(multiValueDelimiter);
    recordExtractorConfig.setColumnNames(_parser.getHeaderMap().keySet());
    _recordExtractor.init(fieldsToRead, recordExtractorConfig);
  }

  private void validateHeaderForDelimiter(char delimiter, String csvHeader, CSVFormat format)
      throws IOException {
    CSVParser parser = format.parse(RecordReaderUtils.getBufferedReader(_dataFile));
    Iterator<CSVRecord> iterator = parser.iterator();
    if (iterator.hasNext() && recordHasMultipleValues(iterator.next()) && delimiterNotPresentInHeader(delimiter,
        csvHeader)) {
      throw new IllegalArgumentException("Configured header does not contain the configured delimiter");
    }
  }

  private boolean recordHasMultipleValues(CSVRecord record) {
    return record.size() > 1;
  }

  private boolean delimiterNotPresentInHeader(char delimiter, String csvHeader) {
    return !StringUtils.contains(csvHeader, delimiter);
  }

  private void init()
      throws IOException {
    _parser = _format.parse(RecordReaderUtils.getBufferedReader(_dataFile));
    _iterator = _parser.iterator();
  }

  /**
   * Returns a copy of the header map that iterates in column order.
   * <p>
   * The map keys are column names. The map values are 0-based indices.
   * </p>
   * @return a copy of the header map that iterates in column order.
   */
  public Map<String, Integer> getCSVHeaderMap() {
    // if header row is not configured and input file doesn't contain a valid header record, the returned map would
    // contain values from the first row in the input file.
    return _parser.getHeaderMap();
  }

  @Override
  public boolean hasNext() {
    return _iterator.hasNext();
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) {
    CSVRecord record = _iterator.next();
    _recordExtractor.extract(record, reuse);
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _parser.close();
    init();
  }

  @Override
  public void close()
      throws IOException {
    _parser.close();
  }
}
