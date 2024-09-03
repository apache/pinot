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
import java.util.Optional;
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

  private BufferedReader _bufferedReader;
  private CSVRecordReaderConfig _config = null;

  public CSVRecordReader() {
  }

  private static CSVFormat baseCsvFormat(CSVRecordReaderConfig config) {
    if (config.getFileFormat() == null) {
      return CSVFormat.DEFAULT;
    }
    switch (config.getFileFormat().toUpperCase()) {
      case "EXCEL":
        return CSVFormat.EXCEL;
      case "MYSQL":
        return CSVFormat.MYSQL;
      case "RFC4180":
        return CSVFormat.RFC4180;
      case "TDF":
        return CSVFormat.TDF;
      default:
        return CSVFormat.DEFAULT;
    }
  }

  private static <T> Optional<T> optional(T value) {
    return Optional.ofNullable(value);
  }

  private static Map<String, Integer> parseHeaderMapFromLine(CSVFormat format, String line) {
    try (StringReader stringReader = new StringReader(line)) {
      try (CSVParser parser = format.parse(stringReader)) {
        return parser.getHeaderMap();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse header from line: " + line, e);
    }
  }

  private static Character getMultiValueDelimiter(CSVRecordReaderConfig config) {
    if (config == null) {
      return CSVRecordReaderConfig.DEFAULT_MULTI_VALUE_DELIMITER;
    } else if (config.isMultiValueDelimiterEnabled()) {
      return config.getMultiValueDelimiter();
    }
    return null;
  }

  private static boolean useLineIterator(CSVRecordReaderConfig config) {
    return config != null && config.isSkipUnParseableLines();
  }

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _dataFile = dataFile;
    _config = (CSVRecordReaderConfig) recordReaderConfig;
    _format = createCSVFormat();

    // If header is provided by the client, use it. Otherwise, parse the header from the first line of the file.
    // Overwrite the format with the header information.
    optional(_config).map(CSVRecordReaderConfig::getHeader).ifPresent(header -> {
      _headerMap = parseHeaderMapFromLine(_format, header);
      _format = _format.builder().setHeader(_headerMap.keySet().toArray(new String[0])).build();
    });

    validateHeaderWithDelimiter();
    initIterator();
    initRecordExtractor(fieldsToRead);
  }

  private void initRecordExtractor(Set<String> fieldsToRead) {
    final CSVRecordExtractorConfig recordExtractorConfig = new CSVRecordExtractorConfig();
    recordExtractorConfig.setMultiValueDelimiter(getMultiValueDelimiter(_config));
    recordExtractorConfig.setColumnNames(_headerMap.keySet());
    _recordExtractor = new CSVRecordExtractor();
    _recordExtractor.init(fieldsToRead, recordExtractorConfig);
  }

  private CSVFormat createCSVFormat() {
    if (_config == null) {
      return CSVFormat.DEFAULT.builder().setDelimiter(CSVRecordReaderConfig.DEFAULT_DELIMITER).setHeader().build();
    }

    final CSVFormat.Builder builder = baseCsvFormat(_config).builder()
        .setDelimiter(_config.getDelimiter())
        .setHeader()
        .setSkipHeaderRecord(_config.isSkipHeader())
        .setCommentMarker(_config.getCommentMarker())
        .setEscape(_config.getEscapeCharacter())
        .setIgnoreEmptyLines(_config.isIgnoreEmptyLines())
        .setIgnoreSurroundingSpaces(_config.isIgnoreSurroundingSpaces())
        .setQuote(_config.getQuoteCharacter());

    optional(_config.getQuoteMode()).map(QuoteMode::valueOf).ifPresent(builder::setQuoteMode);
    optional(_config.getRecordSeparator()).ifPresent(builder::setRecordSeparator);
    optional(_config.getNullStringValue()).ifPresent(builder::setNullString);
    return builder.build();
  }

  private void initIterator()
      throws IOException {
    if (useLineIterator(_config)) {
      _bufferedReader = new BufferedReader(new FileReader(_dataFile), 1024 * 32); // 32KB buffer size
      _iterator = new LineIterator();
    } else {
      _parser = _format.parse(RecordReaderUtils.getBufferedReader(_dataFile));
      _headerMap = _parser.getHeaderMap();
      _iterator = _parser.iterator();
    }
  }

  private void validateHeaderWithDelimiter()
      throws IOException {
    if (_config == null || _config.getHeader() == null || useLineIterator(_config)) {
      return;
    }
    final CSVParser parser = _format.parse(RecordReaderUtils.getBufferedReader(_dataFile));
    final Iterator<CSVRecord> iterator = parser.iterator();
    if (iterator.hasNext() && recordHasMultipleValues(iterator.next()) && delimiterNotPresentInHeader(
        _config.getDelimiter(), _config.getHeader())) {
      throw new IllegalArgumentException("Configured header does not contain the configured delimiter");
    }
  }

  private boolean recordHasMultipleValues(CSVRecord record) {
    return record.size() > 1;
  }

  private boolean delimiterNotPresentInHeader(char delimiter, String csvHeader) {
    return !StringUtils.contains(csvHeader, delimiter);
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
    return _iterator.hasNext();
  }

  @Override
  public GenericRow next()
      throws IOException {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    CSVRecord record = _iterator.next();
    _recordExtractor.extract(record, reuse);
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    if (_parser != null && !_parser.isClosed()) {
      _parser.close();
    }
    closeIterator();
    initIterator();
  }

  @Override
  public void close()
      throws IOException {
    closeIterator();

    if (_parser != null && !_parser.isClosed()) {
      _parser.close();
    }
  }

  private void closeIterator()
      throws IOException {
    // if header is not provided by the client it would be rebuilt. When it's provided by the client it's initialized
    // once in the constructor
    if (useLineIterator(_config) && _config.getHeader() == null) {
      _headerMap.clear();
    }

    if (_bufferedReader != null) {
      _bufferedReader.close();
    }
  }

  class LineIterator implements Iterator<CSVRecord> {
    private String _nextLine;
    private CSVRecord _current;

    public LineIterator() {
      init();
    }

    private void init() {
      try {
        if (_config.getHeader() != null) {
          if (_config.isSkipHeader()) {
            // When skip header config is set and header is supplied – skip the first line from the input file
            _bufferedReader.readLine();
            // turn off the property so that it doesn't interfere with further parsing
            _format = _format.builder().setSkipHeaderRecord(false).build();
          }
        } else {
          // read the first line
          String headerLine = _bufferedReader.readLine();
          _headerMap = parseHeaderMapFromLine(_format, headerLine);
          // If header isn't provided, the first line would be set as header and the 'skipHeader' property
          // is set to false.
          _format = _format.builder()
              .setSkipHeaderRecord(false)
              .setHeader(_headerMap.keySet().toArray(new String[0]))
              .build();
        }
        _nextLine = _bufferedReader.readLine();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private CSVRecord getNextRecord() {
      while (_nextLine != null) {
        try (Reader reader = new StringReader(_nextLine)) {
          try (CSVParser csvParser = _format.parse(reader)) {
            List<CSVRecord> csvRecords = csvParser.getRecords();
            if (csvRecords == null || csvRecords.isEmpty()) {
              // Can be thrown on: 1) Empty lines 2) Commented lines
              throw new NoSuchElementException("Failed to find any records");
            }
            // There would be only one record as lines are read one after the other
            CSVRecord csvRecord = csvRecords.get(0);

            // move the pointer to the next line
            _nextLine = _bufferedReader.readLine();
            return csvRecord;
          } catch (Exception e) {
            // Find the next line that can be parsed
            _nextLine = _bufferedReader.readLine();
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      return null;
    }

    @Override
    public boolean hasNext() {
      if (_current == null) {
        _current = getNextRecord();
      }

      return _current != null;
    }

    @Override
    public CSVRecord next() {
      CSVRecord next = _current;
      _current = null;

      if (next == null) {
        // hasNext() wasn't called before
        next = getNextRecord();
        if (next == null) {
          throw new NoSuchElementException("No more CSV records available");
        }
      }

      return next;
    }
  }
}
