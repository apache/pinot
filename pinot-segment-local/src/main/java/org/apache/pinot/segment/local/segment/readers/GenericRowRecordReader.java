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
package org.apache.pinot.segment.local.segment.readers;

import java.io.File;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;


/**
 * Record reader for list of {@link GenericRow}.
 */
public class GenericRowRecordReader implements RecordReader {
  private final List<GenericRow> _rows;
  private final int _numRows;

  private int _nextRowId = 0;

  public GenericRowRecordReader(List<GenericRow> rows) {
    _rows = rows;
    _numRows = rows.size();
  }

  @Override
  public void init(File dataFile, Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig) {
  }

  @Override
  public boolean hasNext() {
    return _nextRowId < _numRows;
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
  public void close() {
  }
}
