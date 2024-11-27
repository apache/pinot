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
package org.apache.pinot.spi.stream;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * A class that provides relevant row-level metadata for rows indexed into a segment.
 *
 * Currently this is relevant for rows ingested into a mutable segment - the metadata is expected to be
 * provided by the underlying stream.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RowMetadata {

  /**
   * Returns the timestamp associated with the record. This typically refers to the time it was ingested into the
   * (last) upstream source. In some cases, it may be the time at which the record was created, aka event time
   * (eg. in kafka, a topic may be configured to use record `CreateTime` instead of `LogAppendTime`).
   *
   * Expected to be used for stream-based sources.
   *
   * @return timestamp (epoch in milliseconds) when the row was ingested upstream
   *         Long.MIN_VALUE if not available
   */
  long getRecordIngestionTimeMs();

  /**
   * When supported by the underlying stream, this method returns the timestamp in milliseconds associated with
   * the ingestion of the record in the first stream.
   *
   * Complex ingestion pipelines may be composed of multiple streams:
   * (EventCreation) -> {First Stream} -> ... -> {Last Stream}
   *
   * @return timestamp (epoch in milliseconds) when the row was initially ingested upstream for the first
   *         time Long.MIN_VALUE if not supported by the underlying stream.
   */
  default long getFirstStreamRecordIngestionTimeMs() {
    return Long.MIN_VALUE;
  }

  /**
   * @return The serialized size of the record
   */
  default int getRecordSerializedSize() {
    return Integer.MIN_VALUE;
  }

  /**
   * Returns the stream offset of the message.
   */
  @Nullable
  default StreamPartitionMsgOffset getOffset() {
    return null;
  }

  /**
   * Returns the next stream offset of the message.
   */
  @Nullable
  default StreamPartitionMsgOffset getNextOffset() {
    return null;
  }

  /**
   * Returns the stream message headers.
   */
  @Nullable
  default GenericRow getHeaders() {
    return null;
  }

  /**
   * Returns the metadata associated with the stream message.
   */
  @Nullable
  default Map<String, String> getRecordMetadata() {
    return null;
  }
}
