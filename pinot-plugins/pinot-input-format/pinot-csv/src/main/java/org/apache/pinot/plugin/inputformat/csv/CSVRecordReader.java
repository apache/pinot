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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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
    char multiValueDelimiter;
    if (config == null) {
      _format = CSVFormat.DEFAULT.
          withDelimiter(CSVRecordReaderConfig.DEFAULT_DELIMITER).
          withEscape(CSVRecordReaderConfig.DEFAULT_ESCAPE).withHeader();
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
        format = format.withHeader(StringUtils.split(csvHeader, delimiter));
      }
      Character commentMarker = config.getCommentMarker();
      format = format.withCommentMarker(commentMarker);
      format = format.withEscape(config.getEscapeCharacter());
      _format = format;
      multiValueDelimiter = config.getMultiValueDelimiter();
    }
    _recordExtractor = new CSVRecordExtractor();

    init();

    CSVRecordExtractorConfig recordExtractorConfig = new CSVRecordExtractorConfig();
    recordExtractorConfig.setMultiValueDelimiter(multiValueDelimiter);
    recordExtractorConfig.setColumnNames(_parser.getHeaderMap().keySet());
    _recordExtractor.init(fieldsToRead, recordExtractorConfig);
  }

  private void init()
      throws IOException {
    _parser = _format.parse(RecordReaderUtils.getBufferedReader(_dataFile));
    _iterator = _parser.iterator();
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
