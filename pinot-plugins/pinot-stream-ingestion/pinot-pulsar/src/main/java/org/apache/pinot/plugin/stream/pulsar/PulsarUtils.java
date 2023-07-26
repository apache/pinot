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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarUtils.class);

  private static final ByteBuffer LENGTH_BUF = ByteBuffer.allocate(4);

  private PulsarUtils() {
  }

  public static SubscriptionInitialPosition offsetCriteriaToSubscription(OffsetCriteria offsetCriteria)
      throws IllegalArgumentException {
    if (offsetCriteria.isLargest()) {
      return SubscriptionInitialPosition.Latest;
    }
    if (offsetCriteria.isSmallest()) {
      return SubscriptionInitialPosition.Earliest;
    }

    throw new IllegalArgumentException("Unknown initial offset value " + offsetCriteria);
  }

  public static MessageId offsetCriteriaToMessageId(OffsetCriteria offsetCriteria)
      throws IllegalArgumentException {
    if (offsetCriteria.isLargest()) {
      return MessageId.latest;
    }
    if (offsetCriteria.isSmallest()) {
      return MessageId.earliest;
    }

    throw new IllegalArgumentException("Unknown initial offset value " + offsetCriteria);
  }

  /**
   * Stitch key and value bytes together using a simple format:
   * 4 bytes for key length + key bytes + 4 bytes for value length + value bytes
   */
  protected static byte[] stitchKeyValue(byte[] keyBytes, byte[] valueBytes) {
    int keyLen = keyBytes.length;
    int valueLen = valueBytes.length;
    int totalByteArrayLength = 8 + keyLen + valueLen;
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(totalByteArrayLength)) {
      LENGTH_BUF.clear();
      bos.write(LENGTH_BUF.putInt(keyLen).array());
      bos.write(keyBytes);
      LENGTH_BUF.clear();
      bos.write(LENGTH_BUF.putInt(valueLen).array());
      bos.write(valueBytes);
      return bos.toByteArray();
    } catch (Exception e) {
      LOGGER.error("Unable to stitch key and value bytes together", e);
    }
    return null;
  }

  protected static PulsarStreamMessage buildPulsarStreamMessage(Message<byte[]> message, boolean enableKeyValueStitch,
      PulsarMetadataExtractor pulsarMetadataExtractor) {
    byte[] key = message.getKeyBytes();
    byte[] data = enableKeyValueStitch ? stitchKeyValue(key, message.getData()) : message.getData();
    int dataLength = (data != null) ? data.length : 0;
    return new PulsarStreamMessage(key, data, message.getMessageId(),
        (PulsarStreamMessageMetadata) pulsarMetadataExtractor.extract(message), dataLength);
  }
}
