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
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * A class that provides metadata associated with the message of a stream, for e.g.,
 * timestamp derived from the incoming record (not the ingestion time).
 */
public class StreamMessageMetadata implements RowMetadata {
  private final long _recordIngestionTimeMs;
  private final long _firstStreamRecordIngestionTimeMs;
  private final int _recordSerializedSize;
  private final StreamPartitionMsgOffset _offset;
  private final StreamPartitionMsgOffset _nextOffset;
  private final GenericRow _headers;
  private final Map<String, String> _metadata;

  @Deprecated
  public StreamMessageMetadata(long recordIngestionTimeMs) {
    this(recordIngestionTimeMs, null);
  }

  @Deprecated
  public StreamMessageMetadata(long recordIngestionTimeMs, @Nullable GenericRow headers) {
    this(recordIngestionTimeMs, headers, null);
  }

  @Deprecated
  public StreamMessageMetadata(long recordIngestionTimeMs, @Nullable GenericRow headers, Map<String, String> metadata) {
    this(recordIngestionTimeMs, Long.MIN_VALUE, headers, metadata);
  }

  @Deprecated
  public StreamMessageMetadata(long recordIngestionTimeMs, long firstStreamRecordIngestionTimeMs,
      @Nullable GenericRow headers, Map<String, String> metadata) {
    this(recordIngestionTimeMs, firstStreamRecordIngestionTimeMs, null, null, Integer.MIN_VALUE, headers, metadata);
  }

  public StreamMessageMetadata(long recordIngestionTimeMs, long firstStreamRecordIngestionTimeMs,
      @Nullable StreamPartitionMsgOffset offset, @Nullable StreamPartitionMsgOffset nextOffset,
      int recordSerializedSize, @Nullable GenericRow headers, @Nullable Map<String, String> metadata) {
    _recordIngestionTimeMs = recordIngestionTimeMs;
    _firstStreamRecordIngestionTimeMs = firstStreamRecordIngestionTimeMs;
    _offset = offset;
    _nextOffset = nextOffset;
    _recordSerializedSize = recordSerializedSize;
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
  public int getRecordSerializedSize() {
    return _recordSerializedSize;
  }

  @Nullable
  @Override
  public StreamPartitionMsgOffset getOffset() {
    return _offset;
  }

  @Nullable
  @Override
  public StreamPartitionMsgOffset getNextOffset() {
    return _nextOffset;
  }

  @Nullable
  @Override
  public GenericRow getHeaders() {
    return _headers;
  }

  @Nullable
  @Override
  public Map<String, String> getRecordMetadata() {
    return _metadata;
  }

  public static class Builder {
    private long _recordIngestionTimeMs = Long.MIN_VALUE;
    private int _recordSerializedSize = Integer.MIN_VALUE;
    private long _firstStreamRecordIngestionTimeMs = Long.MIN_VALUE;
    private StreamPartitionMsgOffset _offset;
    private StreamPartitionMsgOffset _nextOffset;
    private GenericRow _headers;
    private Map<String, String> _metadata;

    public Builder setRecordIngestionTimeMs(long recordIngestionTimeMs) {
      _recordIngestionTimeMs = recordIngestionTimeMs;
      return this;
    }

    public Builder setFirstStreamRecordIngestionTimeMs(long firstStreamRecordIngestionTimeMs) {
      _firstStreamRecordIngestionTimeMs = firstStreamRecordIngestionTimeMs;
      return this;
    }

    public Builder setOffset(StreamPartitionMsgOffset offset, StreamPartitionMsgOffset nextOffset) {
      _offset = offset;
      _nextOffset = nextOffset;
      return this;
    }

    public Builder setHeaders(GenericRow headers) {
      _headers = headers;
      return this;
    }

    public Builder setMetadata(Map<String, String> metadata) {
      _metadata = metadata;
      return this;
    }

    public Builder setSerializedValueSize(int recordSerializedSize) {
      _recordSerializedSize = recordSerializedSize;
      return this;
    }

    public StreamMessageMetadata build() {
      return new StreamMessageMetadata(_recordIngestionTimeMs, _firstStreamRecordIngestionTimeMs, _offset, _nextOffset,
          _recordSerializedSize, _headers, _metadata);
    }
  }
}
