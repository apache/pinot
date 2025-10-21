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


import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ArrowMessageDecoder is used to decode Apache Arrow IPC format messages into Pinot GenericRow.
 * This decoder handles Arrow streaming format and converts Arrow data to Pinot's columnar format.
 */
public class ArrowMessageDecoder implements StreamMessageDecoder<byte[]> {
  public static final String ARROW_ALLOCATOR_LIMIT = "arrow.allocator.limit";
  public static final String DEFAULT_ALLOCATOR_LIMIT = "268435456"; // 256MB default

  private static final Logger logger = LoggerFactory.getLogger(ArrowMessageDecoder.class);

  private String _streamTopicName;
  private RootAllocator _allocator;
  private ArrowToGenericRowConverter _converter;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    _streamTopicName = topicName;

    // Initialize Arrow allocator with configurable memory limit
    long allocatorLimit =
        Long.parseLong(props.getOrDefault(ARROW_ALLOCATOR_LIMIT, DEFAULT_ALLOCATOR_LIMIT));
    _allocator = new RootAllocator(allocatorLimit);

    // Initialize Arrow to GenericRow converter (processes all fields)
    _converter = new ArrowToGenericRowConverter();

    logger.info(
        "Initialized ArrowMessageDecoder for topic: {} with allocator limit: {} bytes",
        topicName,
        allocatorLimit);
  }

  @Nullable
  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(payload);
        ReadableByteChannel channel = Channels.newChannel(inputStream);
        ArrowStreamReader reader = new ArrowStreamReader(channel, _allocator)) {

      // Read the Arrow schema and data
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      if (!reader.loadNextBatch()) {
        logger.warn("No data found in Arrow message for topic: {}", _streamTopicName);
        return null;
      }

      // Convert Arrow data to GenericRow using converter
      GenericRow row = _converter.convert(reader, root, destination);

      return row;
    } catch (Exception e) {
      logger.error(
          "Error decoding Arrow message for stream topic {} : {}",
          _streamTopicName,
          Arrays.toString(payload),
          e);
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
    if (_allocator != null) {
      try {
        _allocator.close();
      } catch (Exception e) {
        logger.warn("Error closing Arrow allocator", e);
      }
    }
  }
}
