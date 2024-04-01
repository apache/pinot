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

import java.nio.ByteBuffer;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;


public class PulsarUtils {
  private PulsarUtils() {
  }

  public static SubscriptionInitialPosition offsetCriteriaToSubscription(OffsetCriteria offsetCriteria) {
    if (offsetCriteria.isLargest()) {
      return SubscriptionInitialPosition.Latest;
    }
    if (offsetCriteria.isSmallest()) {
      return SubscriptionInitialPosition.Earliest;
    }
    throw new IllegalArgumentException("Unsupported offset criteria: " + offsetCriteria);
  }

  public static MessageId offsetCriteriaToMessageId(OffsetCriteria offsetCriteria) {
    if (offsetCriteria.isLargest()) {
      return MessageId.latest;
    }
    if (offsetCriteria.isSmallest()) {
      return MessageId.earliest;
    }
    throw new IllegalArgumentException("Unsupported offset criteria: " + offsetCriteria);
  }

  /**
   * Stitch key and value bytes together using a simple format:
   * 4 bytes for key length + key bytes + 4 bytes for value length + value bytes
   */
  public static byte[] stitchKeyValue(byte[] keyBytes, byte[] valueBytes) {
    byte[] stitchedBytes = new byte[8 + keyBytes.length + valueBytes.length];
    ByteBuffer buffer = ByteBuffer.wrap(stitchedBytes);
    buffer.putInt(keyBytes.length);
    buffer.put(keyBytes);
    buffer.putInt(valueBytes.length);
    buffer.put(valueBytes);
    return stitchedBytes;
  }

  public static PulsarStreamMessage buildPulsarStreamMessage(Message<byte[]> message, boolean enableKeyValueStitch,
      PulsarMetadataExtractor pulsarMetadataExtractor) {
    byte[] key = message.getKeyBytes();
    byte[] value = message.getData();
    if (enableKeyValueStitch) {
      value = stitchKeyValue(key, value);
    }
    return new PulsarStreamMessage(key, value, message.getMessageId(),
        (PulsarStreamMessageMetadata) pulsarMetadataExtractor.extract(message), value.length);
  }
}
