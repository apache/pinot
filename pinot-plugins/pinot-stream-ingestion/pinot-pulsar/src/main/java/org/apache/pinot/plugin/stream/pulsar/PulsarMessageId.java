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
package org.apache.pinot.plugin.stream.pulsar;

import java.util.Arrays;
import org.apache.pulsar.client.api.MessageIdAdv;


/// [MessageIdAdv] built from its components, for message ids computed by Pinot (e.g. the id of the next message in a
/// partition). The Pulsar client keeps its message id implementations internal, and offers no public factory for them.
///
/// Equality, hash code, ordering and string form follow the built-in Pulsar message ids, so instances can be compared
/// with the message ids returned by the client. [#toByteArray] produces the same `MessageIdData` protobuf bytes as the
/// built-in message ids, which can be deserialized with `MessageId.fromByteArray()`. The ack set used to acknowledge
/// batched messages is not tracked, as these message ids are not acknowledged.
///
/// This class is immutable and thread-safe.
final class PulsarMessageId implements MessageIdAdv {
  // Protobuf tags (field number << 3 | wire type VARINT) of the `MessageIdData` fields
  private static final byte LEDGER_ID_TAG = 1 << 3;
  private static final byte ENTRY_ID_TAG = 2 << 3;
  private static final byte PARTITION_TAG = 3 << 3;
  private static final byte BATCH_INDEX_TAG = 4 << 3;
  private static final byte BATCH_SIZE_TAG = 6 << 3;
  // Up to 5 fields, each with a 1-byte tag and a varint of at most 10 bytes
  private static final int MAX_SERIALIZED_SIZE = 55;

  private final long _ledgerId;
  private final long _entryId;
  private final int _partitionIndex;
  private final int _batchIndex;
  private final int _batchSize;

  /// Creates the message id of a non-batched message.
  PulsarMessageId(long ledgerId, long entryId, int partitionIndex) {
    this(ledgerId, entryId, partitionIndex, -1, 0);
  }

  /// Creates the message id of a message at `batchIndex` in a batch of `batchSize` messages.
  PulsarMessageId(long ledgerId, long entryId, int partitionIndex, int batchIndex, int batchSize) {
    _ledgerId = ledgerId;
    _entryId = entryId;
    _partitionIndex = partitionIndex;
    _batchIndex = batchIndex;
    _batchSize = batchSize;
  }

  @Override
  public long getLedgerId() {
    return _ledgerId;
  }

  @Override
  public long getEntryId() {
    return _entryId;
  }

  @Override
  public int getPartitionIndex() {
    return _partitionIndex;
  }

  @Override
  public int getBatchIndex() {
    return _batchIndex;
  }

  @Override
  public int getBatchSize() {
    return _batchSize;
  }

  @Override
  public byte[] toByteArray() {
    byte[] buffer = new byte[MAX_SERIALIZED_SIZE];
    int size = writeField(buffer, 0, LEDGER_ID_TAG, _ledgerId);
    size = writeField(buffer, size, ENTRY_ID_TAG, _entryId);
    if (_partitionIndex >= 0) {
      size = writeField(buffer, size, PARTITION_TAG, _partitionIndex);
    }
    if (_batchIndex != -1) {
      size = writeField(buffer, size, BATCH_INDEX_TAG, _batchIndex);
    }
    size = writeField(buffer, size, BATCH_SIZE_TAG, _batchSize);
    return Arrays.copyOf(buffer, size);
  }

  /// Writes the tag and the varint encoded value of a field at `offset`, and returns the offset after it. `int32`
  /// values are sign-extended to 64 bits like protobuf does, so negative values take 10 bytes.
  private static int writeField(byte[] buffer, int offset, byte tag, long value) {
    buffer[offset++] = tag;
    while ((value & ~0x7FL) != 0) {
      buffer[offset++] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }
    buffer[offset++] = (byte) value;
    return offset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MessageIdAdv that)) {
      return false;
    }
    return _ledgerId == that.getLedgerId() && _entryId == that.getEntryId()
        && _partitionIndex == that.getPartitionIndex() && _batchIndex == that.getBatchIndex();
  }

  @Override
  public int hashCode() {
    return (int) (31 * (_ledgerId + 31 * _entryId) + (31 * (long) _partitionIndex) + _batchIndex);
  }

  @Override
  public String toString() {
    String str = _ledgerId + ":" + _entryId + ":" + _partitionIndex;
    return _batchIndex != -1 ? str + ":" + _batchIndex : str;
  }
}
