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
package org.apache.pinot.plugin.stream.kafka20;

import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageMetadata;


public class KafkaStreamMessage extends StreamMessage {
  private static final GenericRow EMPTY_ROW_REUSE = new GenericRow();

  // should distinguish stream-specific record metadata in the table??
  private final long _offset;

  public KafkaStreamMessage(@Nullable byte[] key, byte[] value, long offset, @Nullable StreamMessageMetadata metadata) {
    super(key, value, metadata);
    _offset = offset;
  }

  public GenericRow getHeaders() {
    EMPTY_ROW_REUSE.clear();
    return getMetadata() != null ? getMetadata().getHeaders() : EMPTY_ROW_REUSE;
  }

  // for backward compatibility
  public byte[] getMessage() {
    return getValue();
  }

  public long getNextOffset() {
    return _offset < 0 ? -1 : _offset + 1;
  }
}
