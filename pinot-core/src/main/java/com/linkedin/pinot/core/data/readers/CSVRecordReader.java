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
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CSVRecordReader extends BaseRecordReader {
  private static final Logger _logger = LoggerFactory.getLogger(CSVRecordReader.class);

  private String _delimiterString = ",";
  private String _fileName;
  private Schema _schema = null;

  private CSVParser _parser = null;
  private Iterator<CSVRecord> _iterator = null;
  CSVRecordReaderConfig _config = null;

  public CSVRecordReader(String dataFile, RecordReaderConfig recordReaderConfig, Schema schema) {
    super();
    super.initNullCounters(schema);
    _fileName = dataFile;
    _schema = schema;

    _config = (CSVRecordReaderConfig) recordReaderConfig;
    _delimiterString = (_config != null) ? _config.getCsvDelimiter() : ",";
  }

  @Override
  public void init() throws Exception {
    InputStream fileStream = new FileInputStream(_fileName);
    InputStream inputStream;
    if (_fileName.endsWith(".gz")) {
      inputStream = new GZIPInputStream(fileStream);
    } else {
      inputStream = fileStream;
    }
    Reader decoder = new InputStreamReader(inputStream, "UTF-8");
    BufferedReader bufferedReader = new BufferedReader(decoder);
    _parser = new CSVParser(bufferedReader, getFormat());
    _iterator = _parser.iterator();
  }

  @Override
  public void rewind() throws Exception {
    _parser.close();
    init();
  }

  @Override
  public boolean hasNext() {
    return _iterator.hasNext();
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow row) {
    CSVRecord record = _iterator.next();

    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String columnName = fieldSpec.getName();
      String token;
      if (!record.isSet(columnName)) {
        token = null;
      } else {
        token = getValueForColumn(record, columnName);
      }
      if (token == null || token.isEmpty()) {
        incrementNullCountFor(columnName);
      }

      Object value;
      if (fieldSpec.isSingleValueField()) {
        value = RecordReaderUtils.convertToDataType(token, fieldSpec);
      } else {
        String[] tokens = (token != null) ? StringUtils.split(token, _delimiterString) : null;
        value = RecordReaderUtils.convertToDataTypeArray(tokens, fieldSpec);
      }

      row.putField(columnName, value);
    }

    return row;
  }

  @Override
  public void close() throws Exception {
    _parser.close();
  }

  private String getValueForColumn(CSVRecord record, String column) {
    if ((_config != null) && (_config.columnIsDate(column))) {
      return dateToDaysSinceEpochMilli(record.get(column)).toString();
    } else {
      return record.get(column);
    }
  }

  private Long dateToDaysSinceEpochMilli(String token) {
    if ((token == null) || (_config == null)) {
      return 0L;
    }

    SimpleDateFormat dateFormat = new SimpleDateFormat(_config.getCsvDateFormat());

    // Propagting this exception up causes a whole bunch of other readers to now throw exceptions.
    // Catch here, and return 0.
    try {
      Date date = dateFormat.parse(token);
      return date.getTime(); // This is in milli-seconds.
    } catch (ParseException e) {
      _logger.warn("Illegal date: Expected format: " + _config.getCsvDateFormat());
      return 0L;
    }
  }

  private CSVFormat getFormatFromConfig() {
    String format = (_config != null) ? _config.getCsvFileFormat() : null;

    if (format == null) {
      return CSVFormat.DEFAULT;
    }

    format = format.toUpperCase();
    if ((format.equals("DEFAULT"))) {
      return CSVFormat.DEFAULT;
    } else if (format.equals("EXCEL")) {
      return CSVFormat.EXCEL;
    } else if (format.equals("MYSQL")) {
      return CSVFormat.MYSQL;
    } else if (format.equals("RFC4180")) {
      return CSVFormat.RFC4180;
    } else if (format.equals("TDF")) {
      return CSVFormat.TDF;
    } else {
      return CSVFormat.DEFAULT;
    }
  }

  private String[] getHeaderFromConfig() {
    String token;
    if ((_config == null) || ((token = _config.getCsvHeader())) == null) {
      return null;
    }
    return StringUtils.split(token, _delimiterString);
  }

  private char getDelimiterFromConfig() {
    String delimiter;
    if ((_config == null) || ((delimiter = _config.getCsvDelimiter()) == null)) {
      return ',';
    } else {
      return StringEscapeUtils.unescapeJava(delimiter).charAt(0);
    }
  }

  private CSVFormat getFormat() {
    CSVFormat format = getFormatFromConfig().withDelimiter(getDelimiterFromConfig());
    String[] header = getHeaderFromConfig();

    if (header != null) {
      format = format.withHeader(header);
    } else {
      format = format.withHeader();
    }

    return format;
  }
}
