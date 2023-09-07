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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.data.readers.RecordReaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Record reader for CSV file.
 */
@NotThreadSafe
public class CSVRecordReader implements RecordReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(CSVRecordReader.class);

  private File _dataFile;
  private CSVFormat _format;
  private CSVParser _parser;
  private Iterator<CSVRecord> _iterator;
  private CSVRecordExtractor _recordExtractor;
  private Map<String, Integer> _headerMap = new HashMap<>();
  private boolean _isHeaderProvided = false;

  // line iterator specific variables
  private boolean _useLineIterator = false;
  private boolean _skipHeaderRecord = false;
  private long _skippedLinesCount;
  private BufferedReader _bufferedReader;
  private String _nextLine;
  private GenericRow _nextRecord;

  public CSVRecordReader() {
  }

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _dataFile = dataFile;
    CSVRecordReaderConfig config = (CSVRecordReaderConfig) recordReaderConfig;
    Character multiValueDelimiter = null;
    if (config == null) {
      _format = CSVFormat.DEFAULT.builder().setDelimiter(CSVRecordReaderConfig.DEFAULT_DELIMITER).setHeader().build();
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
      format = format.builder().setDelimiter(delimiter).build();

      if (config.isSkipUnParseableLines()) {
        _useLineIterator = true;
      }

      _isHeaderProvided = config.getHeader() != null;
      _skipHeaderRecord = config.isSkipHeader();
      _format = format.builder()
          .setHeader()
          .setSkipHeaderRecord(config.isSkipHeader())
          .setCommentMarker(config.getCommentMarker())
          .setEscape(config.getEscapeCharacter())
          .setIgnoreEmptyLines(config.isIgnoreEmptyLines())
          .setIgnoreSurroundingSpaces(config.isIgnoreSurroundingSpaces())
          .setQuote(config.getQuoteCharacter())
          .build();

      if (config.getQuoteMode() != null) {
        _format = _format.builder().setQuoteMode(QuoteMode.valueOf(config.getQuoteMode())).build();
      }

      if (config.getRecordSeparator() != null) {
        _format = _format.builder().setRecordSeparator(config.getRecordSeparator()).build();
      }

      String nullString = config.getNullStringValue();
      if (nullString != null) {
        _format = _format.builder().setNullString(nullString).build();
      }

      if (_isHeaderProvided) {
        if (!_useLineIterator) {
          validateHeaderForDelimiter(delimiter, config.getHeader(), _format);
        }
        _headerMap = parseLineAsHeader(config.getHeader());
        _format = _format.builder().setHeader(_headerMap.keySet().toArray(new String[0])).build();
      }

      if (config.isMultiValueDelimiterEnabled()) {
        multiValueDelimiter = config.getMultiValueDelimiter();
      }
    }
    _recordExtractor = new CSVRecordExtractor();

    init();

    CSVRecordExtractorConfig recordExtractorConfig = new CSVRecordExtractorConfig();
    recordExtractorConfig.setMultiValueDelimiter(multiValueDelimiter);
    recordExtractorConfig.setColumnNames(_headerMap.keySet());
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
    if (_useLineIterator) {
      initLineIteratorResources();
      return;
    }
    _parser = _format.parse(RecordReaderUtils.getBufferedReader(_dataFile));
    _headerMap = _parser.getHeaderMap();
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
    return _headerMap;
  }

  @Override
  public boolean hasNext() {
    if (_useLineIterator) {
      // When line iterator is used, the call to this method won't throw an exception. The default and the only iterator
      // from commons-csv library can throw an exception upon calling the hasNext() method. The line iterator overcomes
      // this limitation.
      return readNextRecord();
    }
    return _iterator.hasNext();
  }

  @Override
  public GenericRow next()
      throws IOException {
    if (_useLineIterator) {
      return _nextRecord;
    } else {
      return next(new GenericRow());
    }
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    if (_useLineIterator) {
      throw new UnsupportedOperationException("Method signature 'next(GenericRow genericRow)'not supported while using "
          + "the config option 'skipUnParseableLines'.");
    } else {
      CSVRecord record = _iterator.next();
      _recordExtractor.extract(record, reuse);
    }
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    if (_useLineIterator) {
      resetLineIteratorResources();
    }

    if (_parser != null && !_parser.isClosed()) {
      _parser.close();
    }

    init();
  }

  @Override
  public void close()
      throws IOException {
    if (_useLineIterator) {
      resetLineIteratorResources();
    }

    if (_parser != null && !_parser.isClosed()) {
      _parser.close();
    }
  }

  private boolean readNextRecord() {
    try {
      _nextRecord = null;
      GenericRow genericRow = new GenericRow();
      readNextLine(genericRow);
      _nextRecord = genericRow;
    } catch (Exception e) {
      LOGGER.info("Error parsing next record.", e);
    }
    return _nextRecord != null;
  }

  private void readNextLine(GenericRow reuse)
      throws IOException {
    while (_nextLine != null) {
      try (Reader reader = new StringReader(_nextLine)) {
        try (CSVParser csvParser = _format.parse(reader)) {
          List<CSVRecord> csvRecords = csvParser.getRecords();
          if (csvRecords != null && csvRecords.size() > 0) {
            // There would be only one record as lines are read one after the other
            CSVRecord record = csvRecords.get(0);
            _recordExtractor.extract(record, reuse);
            break;
          } else {
            // Can be thrown on: 1) Empty lines 2) Commented lines
            throw new NoSuchElementException("Failed to find any records");
          }
        } catch (Exception e) {
          _skippedLinesCount++;
          LOGGER.debug("Skipped input line: {} from file: {}", _nextLine, _dataFile, e);
          // Find the next line that can be parsed
          _nextLine = _bufferedReader.readLine();
        }
      }
    }
    if (_nextLine != null) {
      // Advance the pointer to the next line for future reading
      _nextLine = _bufferedReader.readLine();
    } else {
      throw new RuntimeException("No more parseable lines. Line iterator reached end of file.");
    }
  }

  private Map<String, Integer> parseLineAsHeader(String line)
      throws IOException {
    Map<String, Integer> headerMap;
    try (StringReader stringReader = new StringReader(line)) {
      try (CSVParser parser = _format.parse(stringReader)) {
        headerMap = parser.getHeaderMap();
      }
    }
    return headerMap;
  }

  private void initLineIteratorResources()
      throws IOException {
    _bufferedReader = new BufferedReader(new FileReader(_dataFile), 1024 * 32); // 32KB buffer size

    // When header is supplied by the client
    if (_isHeaderProvided) {
      if (_skipHeaderRecord) {
        // When skip header config is set and header is supplied – skip the first line from the input file
        _bufferedReader.readLine();
        // turn off the property so that it doesn't interfere with further parsing
        _format = _format.builder().setSkipHeaderRecord(false).build();
      }
    } else {
      // read the first line
      String headerLine = _bufferedReader.readLine();
      _headerMap = parseLineAsHeader(headerLine);
      _format = _format.builder().setHeader(_headerMap.keySet().toArray(new String[0])).build();
    }
    _nextLine = _bufferedReader.readLine();
  }

  private void resetLineIteratorResources()
      throws IOException {
    _nextLine = null;

    LOGGER.info("Total lines skipped in file: {} were: {}", _dataFile, _skippedLinesCount);
    _skippedLinesCount = 0;

    // if header is not provided by the client it would be rebuilt. When it's provided by the client it's initialized
    // once in the constructor
    if (!_isHeaderProvided) {
      _headerMap.clear();
    }

    if (_bufferedReader != null) {
      _bufferedReader.close();
    }
  }
}
