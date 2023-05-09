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
package org.apache.pinot.segment.spi.store;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * Abstract class to map the columnar indices to their buffers
 *
 */
public abstract class ColumnIndexDirectory implements Closeable {

  public abstract void setSegmentMetadata(SegmentMetadataImpl segmentMetadata);

  /**
   * Allocation context for tracking memory
   * @param f file for which memory is allocatedx
   * @param context additional context string
   * @return formatted string for memory tracking
   */
  protected String allocationContext(File f, String context) {
    return this.getClass().getSimpleName() + "." + f.toString() + "." + context;
  }

  /**
   * Get data buffer of a specified indexType for a column
   * @param column column name
   * @param type index type
   * @return ByteBuffer like buffer for data
   * @throws IOException
   */
  public abstract PinotDataBuffer getBuffer(String column, IndexType<?, ?, ?> type)
      throws IOException;

  /**
   * Allocate a new data buffer of specified sizeBytes in the columnar index directory
   * @param column column name
   * @param type index type
   * @param sizeBytes sizeBytes for the buffer allocation
   * @return ByteBuffer like buffer for data
   * @throws IOException
   */
  public abstract PinotDataBuffer newBuffer(String column, IndexType<?, ?, ?> type, long sizeBytes)
      throws IOException;

  /**
   * Check if an index exists for a column
   * @param column column name
   * @param type index type
   * @return true if the index exists; false otherwise
   */
  public abstract boolean hasIndexFor(String column, IndexType<?, ?, ?> type);

  /**
   * Remove the specified index
   * @param columnName column name
   * @param indexType index type
   */
  public abstract void removeIndex(String columnName, IndexType<?, ?, ?> indexType);

  /**
   * Get the columns with specific index type, loaded by column index directory.
   * @return a set of columns with such index type.
   */
  public abstract Set<String> getColumnsWithIndex(IndexType<?, ?, ?> type);

  /**
   * A hint to prefetch the buffers for columns in the context, in preparation for operating on the segment.
   */
  public void prefetchBuffer(FetchContext fetchContext) {
  }

  /**
   * An instruction to fetch the buffers for columns in the context, in order to operate on the segment.
   */
  public void acquireBuffer(FetchContext fetchContext) {
  }

  /**
   * An instruction to release the fetched buffers for columns in this context, after operating on this segment.
   */
  public void releaseBuffer(FetchContext fetchContext) {
  }
}
