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
package org.apache.pinot.segment.local.segment.index.map;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.MapIndexReader;
import org.apache.pinot.spi.data.ComplexFieldSpec.MapFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;


@SuppressWarnings("rawtypes")
public class MapIndexReaderWrapper implements MapIndexReader {
  private final ForwardIndexReader _forwardIndexReader;
  private final FieldSpec _valueFieldSpec;
  private final int _totalDocs;

  public MapIndexReaderWrapper(ForwardIndexReader forwardIndexReader, MapFieldSpec mapFieldSpec, int totalDocs) {
    _forwardIndexReader = forwardIndexReader;
    _valueFieldSpec = mapFieldSpec.getValueFieldSpec();
    _totalDocs = totalDocs;
  }

  @Override
  public Set<String> getKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<IndexType, IndexReader> getIndexes(String key) {
    return Map.of(StandardIndexes.forward(), new MapKeyIndexReader(_forwardIndexReader, key, _valueFieldSpec));
  }

  @Override
  public ColumnMetadata getColumnMetadata(String key) {
    return new SimpleColumnMetadata(_valueFieldSpec, _totalDocs);
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
  public void close()
      throws IOException {
  }
}
