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
package org.apache.pinot.plugin.inputformat.arrow;

import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Decodes Apache Arrow IPC stream-format messages into Pinot [GenericRow]s. The output shape depends on the Arrow
/// batch's row count:
/// - 0 row → returns `null` (nothing to ingest).
/// - 1 row → the single row's fields are populated directly into the destination [GenericRow].
/// - multiple rows → the rows are wrapped in a `List<GenericRow>` stored under [GenericRow#MULTIPLE_RECORDS_KEY]
///   on the destination.
public class ArrowMessageDecoder implements StreamMessageDecoder<byte[]> {
  public static final String ARROW_ALLOCATOR_LIMIT = "arrow.allocator.limit";
  public static final String DEFAULT_ALLOCATOR_LIMIT = "268435456"; // 256MB default

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowMessageDecoder.class);

  private final ArrowRecordExtractor.Record _record = new ArrowRecordExtractor.Record();
  private ArrowRecordExtractor _extractor;
  private String _streamTopicName;
  private RootAllocator _allocator;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    // Resolve the extractor + config classes from props. Defaults to `ArrowRecordExtractor` /
    // `ArrowRecordExtractorConfig`; user-supplied extractors must extend [ArrowRecordExtractor]
    // so the per-batch `setReader` hook is honored.
    String extractorClass = props.get(RECORD_EXTRACTOR_CONFIG_KEY);
    String configClass = props.get(RECORD_EXTRACTOR_CONFIG_CONFIG_KEY);
    if (extractorClass == null) {
      extractorClass = ArrowRecordExtractor.class.getName();
      configClass = ArrowRecordExtractorConfig.class.getName();
    }
    RecordExtractorConfig extractorConfig = null;
    if (configClass != null) {
      extractorConfig = PluginManager.get().createInstance(configClass);
      extractorConfig.init(props);
    }
    // Validate the extractor extends ArrowRecordExtractor: the decoder loop calls `setReader` per batch,
    // and that hook only exists on this base class.
    Object extractor = PluginManager.get().createInstance(extractorClass);
    Preconditions.checkState(extractor instanceof ArrowRecordExtractor,
        "Record extractor class %s must extend ArrowRecordExtractor", extractorClass);
    _extractor = (ArrowRecordExtractor) extractor;
    _extractor.init(fieldsToRead, extractorConfig);
    _streamTopicName = topicName;

    // Initialize Arrow allocator with configurable memory limit
    long allocatorLimit = Long.parseLong(props.getOrDefault(ARROW_ALLOCATOR_LIMIT, DEFAULT_ALLOCATOR_LIMIT));
    _allocator = new RootAllocator(allocatorLimit);

    LOGGER.info("Initialized ArrowMessageDecoder for topic: {} with allocator limit: {} bytes", topicName,
        allocatorLimit);
  }

  @Nullable
  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(payload);
        ReadableByteChannel channel = Channels.newChannel(inputStream);
        ArrowStreamReader reader = new ArrowStreamReader(channel, _allocator)) {
      if (!reader.loadNextBatch()) {
        LOGGER.warn("No data found in Arrow message for topic: {}", _streamTopicName);
        return null;
      }

      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      int rowCount = root.getRowCount();
      if (rowCount == 0) {
        return null;
      }

      if (destination == null) {
        destination = new GenericRow();
      }
      _extractor.setReader(reader);
      _extractor.prepareBatch(_record);

      if (rowCount == 1) {
        // Single row — fill destination directly (the GenericRow is the row).
        _record.setRowId(0);
        _extractor.extract(_record, destination);
        return destination;
      }

      // Multiple rows — wrap them under MULTIPLE_RECORDS_KEY.
      List<GenericRow> rows = new ArrayList<>(rowCount);
      for (int rowId = 0; rowId < rowCount; rowId++) {
        _record.setRowId(rowId);
        GenericRow row = new GenericRow();
        _extractor.extract(_record, row);
        rows.add(row);
      }
      destination.putValue(GenericRow.MULTIPLE_RECORDS_KEY, rows);
      return destination;
    } catch (Exception e) {
      LOGGER.error("Error decoding Arrow message for stream topic {} ({} bytes)", _streamTopicName, payload.length, e);
      return null;
    }
  }

  @Nullable
  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }

  /** Clean up resources */
  public void close() {
    _record.close();
    if (_allocator != null) {
      try {
        _allocator.close();
      } catch (Exception e) {
        LOGGER.warn("Error closing Arrow allocator", e);
      }
    }
  }
}
