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

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * A class that provides metadata associated with the message of a stream, for e.g.,
 * timestamp derived from the incoming record (not the ingestion time).
 */
public class StreamMessageMetadata implements RowMetadata {
  private final long _recordIngestionTimeMs;
  private final long _firstStreamRecordIngestionTimeMs;
  private final GenericRow _headers;
  private final Map<String, String> _metadata;

  public StreamMessageMetadata(long recordIngestionTimeMs) {
    this(recordIngestionTimeMs, Long.MIN_VALUE, null, Collections.emptyMap());
  }

  public StreamMessageMetadata(long recordIngestionTimeMs, @Nullable GenericRow headers) {
    this(recordIngestionTimeMs, Long.MIN_VALUE, headers, Collections.emptyMap());
  }

  public StreamMessageMetadata(long recordIngestionTimeMs, @Nullable GenericRow headers,
      Map<String, String> metadata) {
    this(recordIngestionTimeMs, Long.MIN_VALUE, headers, metadata);
  }
  /**
   * Construct the stream based message/row message metadata
   *
   * @param recordIngestionTimeMs  the time that the message was ingested by the stream provider.
   *                         use Long.MIN_VALUE if not applicable
   * @param firstStreamRecordIngestionTimeMs the time that the message was ingested by the first stream provider
   *                         in the ingestion pipeline. use Long.MIN_VALUE if not applicable
   * @param metadata
   */
  public StreamMessageMetadata(long recordIngestionTimeMs, long firstStreamRecordIngestionTimeMs,
      @Nullable GenericRow headers, Map<String, String> metadata) {
    _recordIngestionTimeMs = recordIngestionTimeMs;
    _firstStreamRecordIngestionTimeMs = firstStreamRecordIngestionTimeMs;
    _headers = headers;
    _metadata = metadata;
  }

  @Override
  public long getRecordIngestionTimeMs() {
    return _recordIngestionTimeMs;
  }

  @Override
  public long getFirstStreamRecordIngestionTimeMs() {
    return _firstStreamRecordIngestionTimeMs;
  }

  @Override
  public GenericRow getHeaders() {
    return _headers;
  }

  @Override
  public Map<String, String> getRecordMetadata() {
    return _metadata;
  }
}
