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
package com.linkedin.pinot.util;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import java.util.Map;
import org.apache.commons.lang.mutable.MutableLong;


public class TestDataRecordReader implements RecordReader {
  final Schema _schema;
  final GenericRow[] _segmentData;
  final int _numRows;
  int index = 0;

  public TestDataRecordReader(Schema schema, GenericRow[] segmentData) {
    _schema = schema;
    _segmentData = segmentData;
    _numRows = segmentData.length;
  }

  @Override
  public void init() throws Exception {
  }

  @Override
  public void rewind() throws Exception {
    index = 0;
  }

  @Override
  public boolean hasNext() {
    return index < _numRows;
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public GenericRow next() {
    return _segmentData[index++];
  }

  @Override
  public GenericRow next(GenericRow row) {
    return next();
  }

  @Override
  public Map<String, MutableLong> getNullCountMap() {
    return null;
  }

  @Override
  public void close() throws Exception {
  }
}
