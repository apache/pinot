/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.FileReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;

public class CSVRecordReader implements RecordReader {
  private static final String CSV_FORMAT_ENV = "csv_reader_format";
  private static final String CSV_HEADER_ENV = "csv_header";
  private static final String CSV_DELIMITER_ENV = "csv_delimiter";

  private String _delimiterString = ",";
  private String _fileName;
  private Schema _schema = null;

  private CSVParser _parser = null;
  private Iterator<CSVRecord> _iterator = null;
  private Map<String, String> _env = null;

  public CSVRecordReader(String dataFile, Schema schema) {
    _fileName = dataFile;

    _schema = schema;
    _env = System.getenv();
    _delimiterString = Character.toString(getDelimiterFromEnv());
  }

  @Override
  public void init() throws Exception {
    final Reader reader = new FileReader(_fileName);
    _parser = new CSVParser(reader, getFormat());

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
    CSVRecord record = _iterator.next();
    Map<String, Object> fieldMap = new HashMap<String, Object>();

    for (final FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      String token = record.get(column);

      Object value=null;
      if (fieldSpec.isSingleValueField()) {
        value = RecordReaderUtils.convertToDataType(token, fieldSpec.getDataType());

      } else {
        String[] tokens = (token != null) ? StringUtils.split(token, _delimiterString) : null;
        value = RecordReaderUtils.convertToDataTypeArray(tokens, fieldSpec.getDataType());
      }

      fieldMap.put(column, value);
    }

    GenericRow genericRow = new GenericRow();
    genericRow.init(fieldMap);
    return genericRow;
  }

  @Override
  public void close() throws Exception {
    _parser.close();
  }

  private CSVFormat getFormatFromEnv() {
    String format = (_env != null) ? _env.get(CSV_FORMAT_ENV) : null;

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

  private String[] getHeaderFromEnv() {
    String token;
    if ((_env == null) || ((token = _env.get(CSV_HEADER_ENV))) == null) {
      return null;
    }
    return StringUtils.split(token, _delimiterString);
  }

  private char getDelimiterFromEnv() {
    String delimiter;
    if ((_env == null) || ((delimiter = _env.get(CSV_DELIMITER_ENV)) == null)) {
      return ',';
    } else {
      return StringEscapeUtils.unescapeJava(delimiter).charAt(0);
    }
  }

  private CSVFormat getFormat() {
    CSVFormat format = getFormatFromEnv().withDelimiter(getDelimiterFromEnv());
    String[] header = getHeaderFromEnv();

    if (header != null) {
      format = format.withHeader(header);
    } else {
      format = format.withHeader();
    }

    return format;
  }
}
