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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Factory to create MicroBatchStreamPartitionMsgOffset from serialized strings.
 */
public class MicroBatchStreamPartitionMsgOffsetFactory implements StreamPartitionMsgOffsetFactory {

  @Override
  public void init(StreamConfig streamConfig) {
  }

  @Override
  public StreamPartitionMsgOffset create(String offsetStr) {
    // Handle simple numeric offsets (e.g., "0" from smallest offset criteria)
    if (offsetStr != null && !offsetStr.startsWith("{")) {
      try {
        long offset = Long.parseLong(offsetStr.trim());
        return MicroBatchStreamPartitionMsgOffset.of(offset);
      } catch (NumberFormatException ignored) {
        // Fall through to JSON parsing
      }
    }
    // Parse JSON format: {"kmo":0,"mbro":0}
    try {
      return JsonUtils.stringToObject(offsetStr, MicroBatchStreamPartitionMsgOffset.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse MicroBatchStreamPartitionMsgOffset from: " + offsetStr, e);
    }
  }
}
