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
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

  private static final Map<String, CSVFormat> CSV_FORMAT_MAP = new HashMap<>();

  static {
    for (CSVFormat.Predefined format : CSVFormat.Predefined.values()) {
      CSV_FORMAT_MAP.put(canonicalize(format.name()), format.getFormat());
    }
  }

  private static String canonicalize(String format) {
    return StringUtils.remove(format, '_').toUpperCase();
  }

  private static CSVFormat getCSVFormat(@Nullable String format) {
    if (format == null) {
      return CSVFormat.DEFAULT;
    }
    CSVFormat csvFormat = CSV_FORMAT_MAP.get(canonicalize(format));
    if (csvFormat != null) {
      return csvFormat;
    } else {
      LOGGER.warn("Failed to find CSV format for: {}, using DEFAULT format", format);
      return CSVFormat.DEFAULT;
    }
  }

  private File _dataFile;
  private CSVRecordReaderConfig _config;
  private CSVFormat _format;
  private BufferedReader _reader;
  private CSVParser _parser;
  private List<String> _columns;
  private Iterator<CSVRecord> _iterator;
  private CSVRecordExtractor _recordExtractor;

  // Following fields are used to handle exceptions in hasNext() method
  private int _nextLineId;
  private int _numSkippedLines;
  private RuntimeException _exceptionInHasNext;
  private CSVFormat _recoveryFormat;

  @Override
  public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig)
      throws IOException {
    _dataFile = dataFile;
    _config = recordReaderConfig != null ? (CSVRecordReaderConfig) recordReaderConfig : new CSVRecordReaderConfig();
    _format = getCSVFormat();
    _reader = RecordReaderUtils.getBufferedReader(_dataFile);
    _parser = _format.parse(_reader);
    _columns = _parser.getHeaderNames();
    _iterator = _parser.iterator();
    _recordExtractor = getRecordExtractor(fieldsToRead);
    _nextLineId = (int) _parser.getCurrentLineNumber();

    // Read the first record, and validate if the header uses the configured delimiter
    // (address https://github.com/apache/pinot/issues/7187)
    boolean hasNext;
    try {
      hasNext = _iterator.hasNext();
    } catch (RuntimeException e) {
      throw new IOException("Failed to read first record from file: " + _dataFile, e);
    }
    if (hasNext) {
      CSVRecord record = _iterator.next();
      if (record.size() > 1 && _columns.size() <= 1) {
        throw new IllegalStateException("Header does not contain the configured delimiter");
      }
      _reader.close();
      _reader = RecordReaderUtils.getBufferedReader(_dataFile);
      _parser = _format.parse(_reader);
      _iterator = _parser.iterator();
    }
  }

  private CSVFormat getCSVFormat() {
    CSVFormat.Builder builder = getCSVFormat(_config.getFileFormat()).builder()
        .setHeader()  // Parse header from the file
        .setDelimiter(_config.getDelimiter())
        .setIgnoreEmptyLines(_config.isIgnoreEmptyLines())
        .setIgnoreSurroundingSpaces(_config.isIgnoreSurroundingSpaces())
        .setQuote(_config.getQuoteCharacter());
    if (_config.getCommentMarker() != null) {
      builder.setCommentMarker(_config.getCommentMarker());
    }
    if (_config.getEscapeCharacter() != null) {
      builder.setEscape(_config.getEscapeCharacter());
    }
    if (_config.getNullStringValue() != null) {
      builder.setNullString(_config.getNullStringValue());
    }
    if (_config.getQuoteMode() != null) {
      builder.setQuoteMode(QuoteMode.valueOf(_config.getQuoteMode()));
    }
    if (_config.getRecordSeparator() != null) {
      builder.setRecordSeparator(_config.getRecordSeparator());
    }
    CSVFormat format = builder.build();
    String header = _config.getHeader();
    if (header == null) {
      return format;
    }
    // Parse header using the current format, and set it into the builder
    try (CSVParser parser = CSVParser.parse(header, format)) {
      format = builder.setHeader(parser.getHeaderNames().toArray(new String[0]))
          .setSkipHeaderRecord(_config.isSkipHeader()).build();
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse header from line: " + header, e);
    }
    return format;
  }

  private CSVRecordExtractor getRecordExtractor(@Nullable Set<String> fieldsToRead) {
    CSVRecordExtractorConfig recordExtractorConfig = new CSVRecordExtractorConfig();
    if (_config.isMultiValueDelimiterEnabled()) {
      recordExtractorConfig.setMultiValueDelimiter(_config.getMultiValueDelimiter());
    }
    recordExtractorConfig.setColumnNames(new HashSet<>(_columns));
    CSVRecordExtractor recordExtractor = new CSVRecordExtractor();
    recordExtractor.init(fieldsToRead, recordExtractorConfig);
    return recordExtractor;
  }

  public List<String> getColumns() {
    return _columns;
  }

  @Override
  public boolean hasNext() {
    try {
      return _iterator.hasNext();
    } catch (RuntimeException e) {
      if (_config.isStopOnError()) {
        LOGGER.warn("Caught exception while reading CSV file: {}, stopping processing", _dataFile, e);
        return false;
      } else {
        // Cache exception here and throw it in next() method
        _exceptionInHasNext = e;
        return true;
      }
    }
  }

  @Override
  public GenericRow next(GenericRow reuse)
      throws IOException {
    if (_exceptionInHasNext != null) {
      // When hasNext() throws an exception, recreate the reader and skip to the next line, then throw the exception
      // TODO: This is very expensive. Consider marking the reader then reset it. The challenge here is that the reader
      //       offset is not the same as parsed offset, and we need to mark at the correct offset.
      _reader.close();
      _reader = RecordReaderUtils.getBufferedReader(_dataFile);
      _numSkippedLines = _nextLineId + 1;
      for (int i = 0; i < _numSkippedLines; i++) {
        _reader.readLine();
      }
      _nextLineId = _numSkippedLines;
      // Create recovery format if not created yet. Recovery format has header preset, and does not skip header record.
      if (_recoveryFormat == null) {
        _recoveryFormat =
            _format.builder().setHeader(_columns.toArray(new String[0])).setSkipHeaderRecord(false).build();
      }
      _parser = _recoveryFormat.parse(_reader);
      _iterator = _parser.iterator();

      RuntimeException exception = _exceptionInHasNext;
      _exceptionInHasNext = null;
      LOGGER.warn("Caught exception while reading CSV file: {}, recovering from line: {}", _dataFile, _numSkippedLines,
          exception);

      throw exception;
    }

    CSVRecord record = _iterator.next();
    _recordExtractor.extract(record, reuse);
    _nextLineId = _numSkippedLines + (int) _parser.getCurrentLineNumber();
    return reuse;
  }

  @Override
  public void rewind()
      throws IOException {
    _reader.close();
    _reader = RecordReaderUtils.getBufferedReader(_dataFile);
    _parser = _format.parse(_reader);
    _iterator = _parser.iterator();
    _nextLineId = (int) _parser.getCurrentLineNumber();
    _numSkippedLines = 0;
  }

  @Override
  public void close()
      throws IOException {
    if (_reader != null) {
      _reader.close();
    }
  }
}
