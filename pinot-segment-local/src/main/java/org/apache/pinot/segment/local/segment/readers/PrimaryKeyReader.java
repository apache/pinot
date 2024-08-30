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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;


public class PrimaryKeyReader implements Closeable {
  public final List<PinotSegmentColumnReader> _primaryKeyColumnReaders;

  public PrimaryKeyReader(IndexSegment segment, List<String> primaryKeyColumns) {
    _primaryKeyColumnReaders = new ArrayList<>(primaryKeyColumns.size());
    for (String primaryKeyColumn : primaryKeyColumns) {
      _primaryKeyColumnReaders.add(new PinotSegmentColumnReader(segment, primaryKeyColumn));
    }
  }

  public PrimaryKey getPrimaryKey(int docId) {
    int numPrimaryKeys = _primaryKeyColumnReaders.size();
    Object[] values = new Object[numPrimaryKeys];
    for (int i = 0; i < numPrimaryKeys; i++) {
      values[i] = getValue(_primaryKeyColumnReaders.get(i), docId);
    }
    return new PrimaryKey(values);
  }

  public void getPrimaryKey(int docId, PrimaryKey buffer) {
    Object[] values = buffer.getValues();
    int numPrimaryKeys = values.length;
    for (int i = 0; i < numPrimaryKeys; i++) {
      values[i] = getValue(_primaryKeyColumnReaders.get(i), docId);
    }
  }

  @Override
  public void close()
      throws IOException {
    for (PinotSegmentColumnReader primaryKeyColumnReader : _primaryKeyColumnReaders) {
      primaryKeyColumnReader.close();
    }
  }

  private static Object getValue(PinotSegmentColumnReader columnReader, int docId) {
    Object value = columnReader.getValue(docId);
    return value instanceof byte[] ? new ByteArray((byte[]) value) : value;
  }
}
