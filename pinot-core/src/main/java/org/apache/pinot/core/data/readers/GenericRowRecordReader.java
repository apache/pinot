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
package org.apache.pinot.core.data.readers;

import java.io.File;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;


/**
 * Record reader for list of {@link GenericRow}.
 */
public class GenericRowRecordReader implements RecordReader {
  private final List<GenericRow> _rows;
  private final int _numRows;
  private final Schema _schema;

  private int _nextRowId = 0;

  public GenericRowRecordReader(List<GenericRow> rows, Schema schema) {
    _rows = rows;
    _numRows = rows.size();
    _schema = schema;
  }

  @Override
  public void init(File dataFile, Schema schema, @Nullable RecordReaderConfig recordReaderConfig) {
  }

  @Override
  public boolean hasNext() {
    return _nextRowId < _numRows;
  }

  @Override
  public GenericRow next() {
    return next(new GenericRow());
  }

  @Override
  public GenericRow next(GenericRow reuse) {
    reuse.init(_rows.get(_nextRowId++));
    return reuse;
  }

  @Override
  public void rewind() {
    _nextRowId = 0;
  }

  @Override
  public Schema getSchema() {
    return _schema;
  }

  @Override
  public void close() {
  }
}
