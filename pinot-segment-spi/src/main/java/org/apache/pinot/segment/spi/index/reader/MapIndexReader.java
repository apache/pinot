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
package org.apache.pinot.segment.spi.index.reader;

import java.util.Map;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Interface for reading from the Mutable Map Index.
 *
 * @param <T> Type of the ReaderContext
 */
public interface MapIndexReader<T extends ForwardIndexReaderContext, R extends IndexReader>
    extends ForwardIndexReader<T> {
  IndexReader getKeyReader(String key, IndexType<?, ?, ?> type);
  Map<IndexType, R> getKeyIndexes(String key);

  /**
   * Returns the data type of the values in the forward index. Returns {@link DataType#INT} for dictionary-encoded
   * forward index.
   */
  DataType getStoredType(String key);

  ColumnMetadata getKeyMetadata(String key);
}
