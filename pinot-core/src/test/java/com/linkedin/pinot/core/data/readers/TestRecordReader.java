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

import java.io.IOException;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.BaseRecordReader;

/**
 * Test record reader to generate GenericRows from test data
 */
public class TestRecordReader extends BaseRecordReader {

  private Schema schema;
  private List<GenericRow> rows;
  private int totalRecords;
  private int recordNumber;

  public TestRecordReader(List<GenericRow> rows, Schema schema)  throws IOException, ConfigurationException {
    this.rows = rows;
    this.totalRecords = rows.size();
    this.schema = schema;
  }

  @Override
  public void init() throws Exception {
    recordNumber = 0;
  }

  @Override
  public void rewind() throws Exception {
    init();
  }

  @Override
  public boolean hasNext() {
    return recordNumber < totalRecords;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow row) {
    GenericRow testRow = rows.get(recordNumber);
    for (String fieldName : testRow.getFieldNames()) {
      row.putField(fieldName, testRow.getValue(fieldName));
    }
    recordNumber ++;
    return row;
  }

  @Override
  public void close() throws Exception {
  }
}
