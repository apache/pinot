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
import java.net.URI;
import java.nio.file.Path;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 *
 * Basic top-level interface to access segment indexes.
 * Usage:
 * <pre>
 *   {@code
 *     SegmentDirectory segmentDir =
 *            SegmentDirectory.createFromLocalFS(dirName, segmentMetadata, ReadMode.mmap);
 *     SegmentDirectory.Writer writer =
 *          segmentDir.createWriter();
 *     try {
 *       writer.getIndexFor("column1", StandardIndexes.forward());
 *       PinotDataBufferOld buffer =
 *           writer.newIndexFor("column1", StandardIndexes.forward(), 1024);
 *       // write value 87 at index 512
 *       buffer.putLong(512, 87L);
 *       writer.saveAndClose();
 *     } finally {
 *       writer.close();
 *     }
 *
 *     SegmentDirectory.Reader reader =
 *           segmentDir.createReader();
 *     try {
 *       PinotDataBufferOld col1Dictionary = reader.getIndexFor("col1Dictionary", StandardIndexes.dictionary());
 *     } catch (Exception e) {
 *       // handle error
 *     } finally {
 *       reader.close();
 *     }
 *
 *     // this should be in finally{} block
 *     segmentDir.close();
 *   }
 * </pre>
 *
 * Typical use cases for Pinot:
 * 1. Read existing indexes
 * 2. Read forward index and create new inverted index
 * 3. drop inverted index
 * 4. Create dictionary, forward index and inverted index.
 *
 * Semantics:
 * ===========
 * The semantics below are explicitly tied to the use cases above. Typically, you
 * should cluster all the writes at the beginning (before reads). After writing,
 * save/close the writer and create reader for reads. saveAndClose() is a costly operation.
 * Reading after writes triggers full reload of data so use it with caution. For pinot, this
 * is a costly operation performed only at the initialization time so the penalty is acceptable.
 *
 * 1. Single writer, multiple reader semantics
 * 2. Writes are not visible till the user calls saveAndClose()
 * 3. saveAndClose() is costly! Creating readers after writers is a costly operation.
 * 4. saveAndClose() does not guarantee atomicity. Failures during saveAndClose()
 *    can leave the directory corrupted.
 * 5. SegmentDirectory controls placement of data. User should not make
 *    any assumptions about data storage.
 * 6. Use factory-methods to instantiate SegmentDirectory. This is with the
 *    goal of supporting networked/distributed file system reads in the future.
 *
 * All things said, users can always get the bytebuffers through readers and
 * change contents. If these buffers are mmapped then the changes will reflect
 * in the segment storage.
 */
public abstract class SegmentDirectory implements Closeable {

  public abstract URI getIndexDir();

  public abstract SegmentMetadataImpl getSegmentMetadata();

  public abstract void reloadMetadata()
      throws Exception;

  /**
   * Get the path/URL for the directory
   */
  public abstract Path getPath();

  public abstract long getDiskSizeBytes();

  /**
   * Get the columns with specific index type, in this local segment directory.
   * @return a set of columns with such index type.
   */
  public abstract Set<String> getColumnsWithIndex(IndexType<?, ?, ?> type);

  /**
   * This is a hint to the segment directory, to begin prefetching buffers for given context.
   * Typically, this should be an async call made before operating on the segment.
   * @param fetchContext context for this segment's fetch
   */
  public void prefetch(FetchContext fetchContext) {
  }

  /**
   * This is an instruction to the segment directory, to fetch buffers for the given context.
   * When enabled, this should be a blocking call made before operating on the segment.
   * @param fetchContext context for this segment's fetch
   */
  public void acquire(FetchContext fetchContext) {
  }

  /**
   * This is an instruction to the segment directory to release the fetched buffers for given context.
   * When enabled, this should be a call made after operating on the segment.
   * It is possible that this called multiple times.
   * @param fetchContext context for this segment's fetch
   */
  public void release(FetchContext fetchContext) {
  }

  /**
   * Copy segment directory to a local directory.
   * @param dest the destination directory
   */
  public void copyTo(File dest)
      throws Exception {
  }

  /**
   * Get the storage tier where the segment directory is placed by server.
   *
   * @return storage tier, null by default.
   */
  @Nullable
  public abstract String getTier();

  /**
   * Set the storage tier where the segment directory is placed by server.
   */
  public abstract void setTier(@Nullable String tier);

  /**
   * Reader for columnar index buffers from segment directory
   */
  public abstract class Reader implements Closeable {

    /**
     * Get columnar index data buffer.
     * @param column column name
     * @param type index type
     * @return a bytebuffer-like buffer holding index data
     * @throws IOException
     */
    public abstract PinotDataBuffer getIndexFor(String column, IndexType<?, ?, ?> type)
        throws IOException;

    public abstract boolean hasIndexFor(String column, IndexType<?, ?, ?> type);

    public boolean hasStarTreeIndex() {
      return false;
    }

    /**
     * The StarTree index is a multi-column index but modelled like those single-column indices kept in columns.psf and
     * index_map files, so access to its index data can be abstracted with SegmentDirectory.Reader interface too. In the
     * future, if new kinds of multi-column index e.g. materialized views is added, this interface can be generalized to
     * access them too.
     *
     * @param starTreeId to look for a specific index instance. The StarTree index can contain multiple index instances,
     *                   each one of them has a tree and a set of fwd indices referred to by the tree.
     * @return SegmentDirectory.Reader object to access the index buffers inside the specific index instance.
     */
    public Reader getStarTreeIndexReader(int starTreeId) {
      return null;
    }

    public SegmentDirectory toSegmentDirectory() {
      return SegmentDirectory.this;
    }

    public abstract String toString();
  }

  /**
   *
   * Writer to update columnar index. Read about the semantics at the top
   */
  public abstract class Writer extends Reader {

    /**
     * create a new buffer for writers to store index. This buffer will be visible
     * after this point.
     * Failures in the middle can cause corruption.
     * @param columnName column name
     * @param indexType column index type
     * @param sizeBytes sizeBytes of index data
     * @return PinotDataBufferOld that writers can update
     * @throws IOException
     */
    // NOTE: an interface like readFrom(File f, String column, IndexType<?, ?, ?>, int sizeBytes) will be safe
    // but it can lead to potential endianness issues. Endianness used to create data may not be
    // same as PinotDataBufferOld
    public abstract PinotDataBuffer newIndexFor(String columnName, IndexType<?, ?, ?> indexType, long sizeBytes)
        throws IOException;

    /**
     * Removes an existing column index from directory
     * @param columnName column name
     * @param indexType column index type
     */
    public abstract void removeIndex(String columnName, IndexType<?, ?, ?> indexType);

    public abstract void save()
        throws IOException;

    public abstract String toString();
  }

  /**
   * Create Reader for the directory
   * @return Reader object if successfully created. null if the directory
   *   is already locked for writes
   * @throws IOException
   */
  public abstract Reader createReader()
      throws IOException, ConfigurationException;

  /**
   * Create Writer for the directory
   * @return Writer object on success. null if the directory has active readers
   * @throws IOException
   */
  public abstract Writer createWriter()
      throws IOException;

  public abstract String toString();

  protected SegmentDirectory() {
  }
}
