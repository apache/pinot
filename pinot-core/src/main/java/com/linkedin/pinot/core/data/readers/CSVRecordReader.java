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
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;


/**
 * Record reader for CSV file.
 */
public class CSVRecordReader implements RecordReader {
  private final Schema _schema;
  private final char _multiValueDelimiter;
  private final SimpleDateFormat _simpleDateFormat;
  private final Set<String> _dateColumns;
  private final CSVParser _parser;

  private Iterator<CSVRecord> _iterator;

  public CSVRecordReader(File dataFile, Schema schema, CSVRecordReaderConfig recordReaderConfig) throws IOException {
    _schema = schema;
    if (recordReaderConfig == null) {
      _multiValueDelimiter = ';';
      _simpleDateFormat = null;
      _dateColumns = null;
    } else {
      _multiValueDelimiter = recordReaderConfig.getMultiValueDelimiter();
      if (recordReaderConfig.getDateFormat() == null) {
        _simpleDateFormat = null;
        _dateColumns = null;
      } else {
        _simpleDateFormat = new SimpleDateFormat(recordReaderConfig.getDateFormat());
        _dateColumns = recordReaderConfig.getDateColumns();
      }
    }
    _parser = getCSVParser(dataFile, recordReaderConfig);
    _iterator = _parser.iterator();
  }

  public static CSVParser getCSVParser(File dataFile, CSVRecordReaderConfig recordReaderConfig) throws IOException {
    Reader fileReader = RecordReaderUtils.getFileReader(dataFile);

    if (recordReaderConfig == null) {
      return CSVFormat.DEFAULT.withDelimiter(',').withHeader().parse(fileReader);
    } else {
      CSVFormat format;

      String csvFileFormat = recordReaderConfig.getFileFormat();
      if (csvFileFormat == null) {
        format = CSVFormat.DEFAULT;
      } else {
        switch (csvFileFormat.toUpperCase()) {
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
      char delimiter = recordReaderConfig.getDelimiter();
      format = format.withDelimiter(delimiter);
      String csvHeader = recordReaderConfig.getHeader();
      if (csvHeader == null) {
        format = format.withHeader();
      } else {
        format = format.withHeader(StringUtils.split(csvHeader, delimiter));
      }

      return format.parse(fileReader);
    }
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
      String fieldName = fieldSpec.getName();

      String token;
      if (!record.isSet(fieldName)) {
        token = null;
      } else {
        token = record.get(fieldName);
        if (_simpleDateFormat != null && _dateColumns.contains(token)) {
          try {
            token = Long.toString(_simpleDateFormat.parse(token).getTime());
          } catch (Exception e) {
            throw new RuntimeException(
                "Caught exception while parsing token: " + token + " from date column: " + fieldName, e);
          }
        }
      }

      Object value;
      if (fieldSpec.isSingleValueField()) {
        value = RecordReaderUtils.convertToDataType(token, fieldSpec);
      } else {
        String[] tokens;
        if (token != null) {
          tokens = StringUtils.split(token, _multiValueDelimiter);
        } else {
          tokens = null;
        }
        value = RecordReaderUtils.convertToDataTypeArray(tokens, fieldSpec);
      }

      reuse.putField(fieldName, value);
    }

    return reuse;
  }

  @Override
  public void rewind() {
    _iterator = _parser.iterator();
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
