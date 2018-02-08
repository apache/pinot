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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;


/**
 * Record reader for CSV file.
 */
public class CSVRecordReader implements RecordReader {
  private final File _dataFile;
  private final Schema _schema;
  private final CSVFormat _format;
  private final char _multiValueDelimiter;

  private CSVParser _parser;
  private Iterator<CSVRecord> _iterator;

  public CSVRecordReader(File dataFile, Schema schema, CSVRecordReaderConfig config) throws IOException {
    _dataFile = dataFile;
    _schema = schema;

    if (config == null) {
      _format = CSVFormat.DEFAULT.withDelimiter(CSVRecordReaderConfig.DEFAULT_DELIMITER).withHeader();
      _multiValueDelimiter = CSVRecordReaderConfig.DEFAULT_MULTI_VALUE_DELIMITER;
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
      _format = format;
      _multiValueDelimiter = config.getMultiValueDelimiter();
    }

    init();
  }

  private void init() throws IOException {
    _parser = _format.parse(RecordReaderUtils.getFileReader(_dataFile));
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

    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      String token = record.isSet(column) ? record.get(column) : null;

      Object value;
      if (fieldSpec.isSingleValueField()) {
        value = RecordReaderUtils.convertToDataType(token, fieldSpec);
      } else {
        String[] tokens = token != null ? StringUtils.split(token, _multiValueDelimiter) : null;
        value = RecordReaderUtils.convertToDataTypeArray(tokens, fieldSpec);
      }

      reuse.putField(column, value);
    }

    return reuse;
  }

  @Override
  public void rewind() throws IOException {
    _parser.close();
    init();
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void close() throws IOException {
    _parser.close();
  }
}
