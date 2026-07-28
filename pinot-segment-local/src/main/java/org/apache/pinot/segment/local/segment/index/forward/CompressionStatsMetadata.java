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
package org.apache.pinot.segment.local.segment.index.forward;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;

import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.FORWARD_INDEX_DICTIONARY_ENCODED_UNCOMPRESSED_VALUE_SIZE_IN_BYTES;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.FORWARD_INDEX_RAW_CHUNK_COMPRESSION_TYPE;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.FORWARD_INDEX_RAW_UNCOMPRESSED_VALUE_SIZE_IN_BYTES;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.getKeyFor;


/// Immutable, thread-safe value object for a column's persisted compression-statistics metadata.
///
/// Applying a value always updates all three related keys, preventing stale raw and dictionary states from being mixed
/// after an encoding transition.
public final class CompressionStatsMetadata {
  private static final CompressionStatsMetadata UNAVAILABLE = new CompressionStatsMetadata(null, null, null);

  private final Long _rawForwardIndexUncompressedValueSizeInBytes;
  private final ChunkCompressionType _rawForwardIndexChunkCompressionType;
  private final Long _dictionaryEncodedUncompressedValueSizeInBytes;

  private CompressionStatsMetadata(@Nullable Long forwardIndexUncompressedValueSizeInBytes,
      @Nullable ChunkCompressionType chunkCompressionType, @Nullable Long dictionaryUncompressedValueSizeInBytes) {
    _rawForwardIndexUncompressedValueSizeInBytes = forwardIndexUncompressedValueSizeInBytes;
    _rawForwardIndexChunkCompressionType = chunkCompressionType;
    _dictionaryEncodedUncompressedValueSizeInBytes = dictionaryUncompressedValueSizeInBytes;
  }

  /// Creates metadata for a raw forward index, or unavailable metadata when either required value is absent.
  public static CompressionStatsMetadata forRawForwardIndex(long uncompressedValueSizeInBytes,
      @Nullable ChunkCompressionType chunkCompressionType) {
    return uncompressedValueSizeInBytes >= 0 && chunkCompressionType != null
        ? new CompressionStatsMetadata(uncompressedValueSizeInBytes, chunkCompressionType, null) : UNAVAILABLE;
  }

  /// Creates metadata for a dictionary-encoded column, or unavailable metadata when the size was not tracked.
  public static CompressionStatsMetadata forDictionary(long uncompressedValueSizeInBytes) {
    return uncompressedValueSizeInBytes >= 0
        ? new CompressionStatsMetadata(null, null, uncompressedValueSizeInBytes) : UNAVAILABLE;
  }

  /// Returns the canonical metadata value that clears all compression-statistics keys.
  public static CompressionStatsMetadata unavailable() {
    return UNAVAILABLE;
  }

  /// Applies this state to a loader metadata-update map. Null values instruct the metadata updater to remove keys.
  public void applyTo(Map<String, String> metadataProperties, String column) {
    metadataProperties.put(getKeyFor(column, FORWARD_INDEX_RAW_UNCOMPRESSED_VALUE_SIZE_IN_BYTES),
        stringValue(_rawForwardIndexUncompressedValueSizeInBytes));
    metadataProperties.put(getKeyFor(column, FORWARD_INDEX_RAW_CHUNK_COMPRESSION_TYPE),
        _rawForwardIndexChunkCompressionType != null ? _rawForwardIndexChunkCompressionType.name() : null);
    metadataProperties.put(getKeyFor(column, FORWARD_INDEX_DICTIONARY_ENCODED_UNCOMPRESSED_VALUE_SIZE_IN_BYTES),
        stringValue(_dictionaryEncodedUncompressedValueSizeInBytes));
  }

  /// Applies this state directly to a segment metadata properties configuration.
  public void applyTo(PropertiesConfiguration properties, String column) {
    setOrClear(properties, getKeyFor(column, FORWARD_INDEX_RAW_UNCOMPRESSED_VALUE_SIZE_IN_BYTES),
        _rawForwardIndexUncompressedValueSizeInBytes);
    setOrClear(properties, getKeyFor(column, FORWARD_INDEX_RAW_CHUNK_COMPRESSION_TYPE),
        _rawForwardIndexChunkCompressionType != null ? _rawForwardIndexChunkCompressionType.name() : null);
    setOrClear(properties, getKeyFor(column, FORWARD_INDEX_DICTIONARY_ENCODED_UNCOMPRESSED_VALUE_SIZE_IN_BYTES),
        _dictionaryEncodedUncompressedValueSizeInBytes);
  }

  @Nullable
  private static String stringValue(@Nullable Long value) {
    return value != null ? String.valueOf(value) : null;
  }

  private static void setOrClear(PropertiesConfiguration properties, String key, @Nullable Object value) {
    if (value != null) {
      properties.setProperty(key, value);
    } else {
      properties.clearProperty(key);
    }
  }
}
