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
  public abstract PinotDataBuffer getBuffer(String column, ColumnIndexType type)
      throws IOException;

  /**
   * Allocate a new data buffer of specified sizeBytes in the columnar index directory
   * @param column column name
   * @param type index type
   * @param sizeBytes sizeBytes for the buffer allocation
   * @return ByteBuffer like buffer for data
   * @throws IOException
   */
  public abstract PinotDataBuffer newBuffer(String column, ColumnIndexType type, long sizeBytes)
      throws IOException;

  /**
   * Check if an index exists for a column
   * @param column column name
   * @param type index type
   * @return true if the index exists; false otherwise
   */
  public abstract boolean hasIndexFor(String column, ColumnIndexType type);

  /**
   * Remove the specified index
   * @param columnName column name
   * @param indexType index type
   */
  public abstract void removeIndex(String columnName, ColumnIndexType indexType);

  /**
   * Check if the implementation supports removing existing index
   * @return true if the index removal is supported
   */
  public abstract boolean isIndexRemovalSupported();

  /**
   * Get the columns with specific index type, loaded by column index directory.
   * @return a set of columns with such index type.
   */
  public abstract Set<String> getColumnsWithIndex(ColumnIndexType type);

  /**
   * Hint to prefetch the buffer for this column
   */
  public void prefetchBuffer(String columns) {
  }

  /**
   * Fetch the buffer for this column
   */
  public void acquireBuffer(String column) {
  }

  /**
   * Release the buffer for this column
   */
  public void releaseBuffer(String column) {
  }
}
