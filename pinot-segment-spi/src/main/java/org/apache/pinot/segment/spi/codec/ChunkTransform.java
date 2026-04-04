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
package org.apache.pinot.segment.spi.codec;

import java.nio.ByteBuffer;
import java.util.Set;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Interface for a reversible numeric transform that operates in-place on a {@link ByteBuffer}.
 * Transforms reduce entropy in typed numeric data before byte-level compression.
 *
 * <p>Implementations must be stateless and thread-safe. A single instance is shared across
 * all chunks of all segments that use the same transform.</p>
 *
 * <p>The transform operates on a window of {@code numBytes} starting at {@code buffer.position()}.
 * It must not change the buffer's position or limit.</p>
 */
public interface ChunkTransform {

  /**
   * Returns the set of stored {@link DataType}s this transform supports. Callers should check
   * this before applying the transform to a column.
   */
  Set<DataType> supportedTypes();

  /**
   * Validates that the given stored type is supported by this transform.
   *
   * @param storedType the stored data type of the column
   * @param columnName the column name (for error messages)
   * @throws IllegalArgumentException if the type is not supported
   */
  default void validateStoredType(DataType storedType, String columnName) {
    if (!supportedTypes().contains(storedType)) {
      throw new IllegalArgumentException(
          String.format("Transform '%s' does not support stored type %s for column '%s'. Supported types: %s",
              getClass().getSimpleName(), storedType, columnName, supportedTypes()));
    }
  }

  /**
   * Applies the forward (encoding) transform in-place.
   *
   * @param buffer the buffer containing typed values to transform
   * @param numBytes the number of bytes in the active region (from current position)
   * @param valueSizeInBytes size of each typed value: 4 for INT/FLOAT, 8 for LONG/DOUBLE
   */
  void encode(ByteBuffer buffer, int numBytes, int valueSizeInBytes);

  /**
   * Applies the reverse (decoding) transform in-place, undoing a prior {@link #encode} call.
   *
   * @param buffer the buffer containing transformed data to restore
   * @param numBytes the number of bytes in the active region (from current position)
   * @param valueSizeInBytes size of each typed value: 4 for INT/FLOAT, 8 for LONG/DOUBLE
   */
  void decode(ByteBuffer buffer, int numBytes, int valueSizeInBytes);
}
