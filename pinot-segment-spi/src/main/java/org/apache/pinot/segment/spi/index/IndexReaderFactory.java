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

package org.apache.pinot.segment.spi.index;

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.IndexConfig;


public interface IndexReaderFactory<R extends IndexReader> {

  /**
   * @throws IndexReaderConstraintException if the constraints of the index reader are not matched. For example, some
   * indexes may require the column to be dictionary based.
   */
  @Nullable
  R createIndexReader(SegmentDirectory.Reader segmentReader, FieldIndexConfigs fieldIndexConfigs,
      ColumnMetadata metadata)
      throws IOException, IndexReaderConstraintException;

  abstract class Default<C extends IndexConfig, R extends IndexReader> implements IndexReaderFactory<R> {

    protected abstract IndexType<C, R, ?> getIndexType();

    protected abstract R createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata, C indexConfig)
        throws IOException, IndexReaderConstraintException;

    /**
     * Sometimes the index configuration indicates that the index should be disabled but the reader actually contains
     * a buffer for the index type.
     *
     * By default, the buffer has priority over the configuration, so in case we have a buffer we would create an index
     * even if the configuration says otherwise. In case some index wants to use their own behavior, this method can
     * be overloaded.
     */
    protected boolean isBufferOverConfig() {
      return true;
    }

    @Override
    public R createIndexReader(SegmentDirectory.Reader segmentReader, FieldIndexConfigs fieldIndexConfigs,
        ColumnMetadata metadata)
        throws IOException, IndexReaderConstraintException {
      IndexType<C, R, ?> indexType = getIndexType();
      C indexConf;
      if (fieldIndexConfigs == null) {
        indexConf = getIndexType().getDefaultConfig();
      } else {
        indexConf = fieldIndexConfigs.getConfig(indexType);
      }

      String columnName = metadata.getColumnName();
      if (indexConf.isDisabled() && (!isBufferOverConfig() || !segmentReader.hasIndexFor(columnName, indexType))) {
        return null;
      }

      PinotDataBuffer buffer = segmentReader.getIndexFor(columnName, indexType);
      try {
        return createIndexReader(buffer, metadata, indexConf);
      } catch (RuntimeException ex) {
        throw new RuntimeException(
            "Cannot read index " + indexType + " for column " + columnName + " with config " + indexConf, ex);
      }
    }
  }
}
