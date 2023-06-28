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

import javax.annotation.Nullable;


/**
 * Represents a Stream message which includes the following components:
 * 1. record key (optional)
 * 2. record value (required)
 * 3. StreamMessageMetadata (optional) - encapsulates record headers and metadata associated with a stream message
 *  (such as a message identifier, publish timestamp, user-provided headers etc)
 *
 * Similar to value decoder, each implementing stream plugin can have a key decoder and header extractor.
 * If the key and header extractions are enabled for the table, the schema will automatically contain these fields as:
 * "__header$HEADER_KEY" or "__metadata$RECORD_TIMESTAMP"
 *
 * These columns can be treated similar to any other Pinot table column.
 *
 * Usability note: In order to achieve this, table configuration should enable "populate metadata" option.
 * Additionally, the pinot table schema should refer these fields. Otherwise, even though the fields are extracted,
 * they will not materialize in the pinot table.
 */
public class StreamMessage<T> {
  private final byte[] _key;
  private final T _value;
  protected final StreamMessageMetadata _metadata;
  int _length = -1;

  public StreamMessage(@Nullable byte[] key, T value, @Nullable StreamMessageMetadata metadata, int length) {
    _key = key;
    _value = value;
    _metadata = metadata;
    _length = length;
  }

  public StreamMessage(T value, int length) {
    this(value, length, null);
  }
  
  public StreamMessage(T value, int length, @Nullable StreamMessageMetadata metadata) {
    this(null, value, metadata, length);
  }

  public T getValue() {
    return _value;
  }

  public int getLength() {
    return _length;
  }

  @Nullable
  public StreamMessageMetadata getMetadata() {
    return _metadata;
  }

  @Nullable
  public byte[] getKey() {
    return _key;
  }
}
