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
package org.apache.pinot.segment.local.segment.index.vector;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;


/**
 * Canonical helper for opening an IVF combined index file as a {@link PinotDataBuffer}.
 *
 * <p>Used by the production reader factory ({@code VectorIndexType.ReaderFactory}) and by tests
 * and benchmarks that instantiate IVF readers directly against a temp directory. Keeping one
 * implementation guarantees identical file resolution, byte order, mmap mode, and ownership
 * semantics across all callers, and makes a future consolidation (when IVF moves into the
 * combined-form segment index file) a single point of change.</p>
 *
 * <p><b>Ownership:</b> the returned buffer is owned by the caller (typically the IVF reader,
 * which closes it in its own {@code close()}).</p>
 *
 * <p><b>Byte order:</b> the buffer is mapped {@link ByteOrder#BIG_ENDIAN} to match the IVF_FLAT
 * on-disk format. IVF_PQ handles its own little-endian payload via stream wrappers inside
 * {@link IvfPqIndexFormat}, so the buffer-level order is irrelevant for IVF_PQ but is consistent
 * with IVF_FLAT for symmetry.</p>
 */
public final class IvfCombinedBuffers {

  private IvfCombinedBuffers() {
  }

  /**
   * Resolves the IVF index sidecar file for {@code column} under {@code segmentDir} and maps
   * the full file as a read-only {@link PinotDataBuffer}.
   *
   * @param segmentDir segment directory containing the sidecar file
   * @param column     column name; used to locate the sidecar file
   * @param config     vector index configuration; used to determine the file extension
   * @param ownerLabel short owner label included in {@code PinotDataBuffer} accounting
   *                   (e.g. {@code "vector-ivf_flat"}, {@code "test-vector"}, {@code "bench-vector"});
   *                   used only for diagnostics
   * @return a {@code PinotDataBuffer} holding the full sidecar file contents; the caller takes ownership
   *         and is responsible for closing it
   * @throws IllegalStateException if the sidecar file is missing
   * @throws RuntimeException       if mmap fails
   */
  public static PinotDataBuffer mapCombined(File segmentDir, String column, VectorIndexConfig config,
      String ownerLabel) {
    Preconditions.checkArgument(segmentDir != null, "segmentDir must not be null");
    Preconditions.checkArgument(column != null && !column.isEmpty(), "column must be non-empty");
    Preconditions.checkArgument(config != null, "config must not be null");
    Preconditions.checkArgument(ownerLabel != null && !ownerLabel.isEmpty(), "ownerLabel must be non-empty");

    File indexFile = SegmentDirectoryPaths.findVectorIndexIndexFile(segmentDir, column, config);
    if (indexFile == null || !indexFile.exists()) {
      throw new IllegalStateException(
          "Vector index sidecar file not found under " + segmentDir + " for column " + column);
    }
    try {
      return PinotDataBuffer.mapFile(indexFile, /* readOnly */ true, 0, indexFile.length(),
          ByteOrder.BIG_ENDIAN, ownerLabel + "-" + column);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to map vector index sidecar file " + indexFile + " for column " + column, e);
    }
  }

  /**
   * Convenience overload for callers that already hold the resolved {@link File} (e.g. the
   * production reader factory). Maps the given file directly.
   */
  public static PinotDataBuffer mapCombinedFile(File indexFile, String column, String ownerLabel) {
    Preconditions.checkArgument(indexFile != null, "indexFile must not be null");
    Preconditions.checkArgument(column != null && !column.isEmpty(), "column must be non-empty");
    Preconditions.checkArgument(ownerLabel != null && !ownerLabel.isEmpty(), "ownerLabel must be non-empty");
    if (!indexFile.exists()) {
      throw new IllegalStateException("Vector index sidecar file does not exist: " + indexFile);
    }
    try {
      return PinotDataBuffer.mapFile(indexFile, /* readOnly */ true, 0, indexFile.length(),
          ByteOrder.BIG_ENDIAN, ownerLabel + "-" + column);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to map vector index sidecar file " + indexFile + " for column " + column, e);
    }
  }

  /**
   * Best-effort close of a {@link PinotDataBuffer}. Used by IVF readers during constructor
   * failure to release the mmap when the caller never sees a reader instance to close. Swallows
   * any close-time exception so the original cause propagates unmasked.
   */
  public static void closeQuietly(@Nullable PinotDataBuffer buffer) {
    if (buffer == null) {
      return;
    }
    try {
      buffer.close();
    } catch (Exception ignored) {
      // best-effort
    }
  }
}
