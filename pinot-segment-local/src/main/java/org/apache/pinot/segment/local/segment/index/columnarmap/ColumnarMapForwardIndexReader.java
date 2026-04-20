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
package org.apache.pinot.segment.local.segment.index.columnarmap;

import java.io.IOException;
import java.util.Map;
import org.apache.pinot.segment.spi.index.reader.ColumnarMapIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * A {@link ForwardIndexReader} for the parent MAP column (with sparse map index) that delegates to
 * {@link ColumnarMapIndexReader#getMap(int)}.
 *
 * <p>This reader exposes the reconstructed {@code Map<String, Object>} for each document,
 * allowing downstream consumers ({@code PinotSegmentColumnReader}, {@code PinotSegmentRecordReader},
 * {@code RealtimeSegmentStatsContainer}) to read MAP columns with sparse map index through the standard forward
 * index API without any special-casing.
 *
 * <p>Only {@link #getMap(int, ForwardIndexReaderContext)} is implemented; other typed accessors
 * ({@code getInt}, {@code getString}, etc.) throw {@code UnsupportedOperationException} via the
 * default interface methods.
 *
 * <p>Lifecycle: this reader does NOT own the underlying {@link ColumnarMapIndexReader}—closing this
 * reader is a no-op. The owning {@link ColumnarMapDataSource} is responsible for closing the reader.
 */
public class ColumnarMapForwardIndexReader implements ForwardIndexReader<ForwardIndexReaderContext> {

  private final ColumnarMapIndexReader _columnarMapIndexReader;

  public ColumnarMapForwardIndexReader(ColumnarMapIndexReader columnarMapIndexReader) {
    _columnarMapIndexReader = columnarMapIndexReader;
  }

  @Override
  public boolean isDictionaryEncoded() {
    return false;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public DataType getStoredType() {
    return DataType.MAP;
  }

  @Override
  public Map<String, Object> getMap(int docId, ForwardIndexReaderContext context) {
    return _columnarMapIndexReader.getMap(docId);
  }

  @Override
  public void close()
      throws IOException {
    // no-op: the underlying reader is owned by ColumnarMapDataSource
  }
}
