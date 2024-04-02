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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.EncryptionContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class PulsarMessageBatchTest {
  private static final Random RANDOM = new Random();

  private byte[] _expectedValueBytes;
  private byte[] _expectedKeyBytes;
  private DummyPulsarMessage _message;

  public static class DummyPulsarMessage implements Message<byte[]> {
    private final byte[] _keyData;
    private final byte[] _valueData;
    private final Map<String, String> _properties;

    public DummyPulsarMessage(byte[] key, byte[] value) {
      _keyData = key;
      _valueData = value;
      _properties = new HashMap<>();
    }

    @Override
    public Map<String, String> getProperties() {
      return _properties;
    }

    @Override
    public boolean hasProperty(String name) {
      return _properties.containsKey(name);
    }

    @Override
    public String getProperty(String name) {
      return _properties.get(name);
    }

    @Override
    public byte[] getData() {
      return _valueData;
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public byte[] getValue() {
      return _valueData;
    }

    @Override
    public MessageId getMessageId() {
      return MessageId.earliest;
    }

    @Override
    public long getPublishTime() {
      return 0;
    }

    @Override
    public long getEventTime() {
      return 0;
    }

    @Override
    public long getSequenceId() {
      return 0;
    }

    @Override
    public String getProducerName() {
      return null;
    }

    @Override
    public boolean hasKey() {
      return _keyData != null;
    }

    @Override
    public String getKey() {
      return new String(_keyData);
    }

    @Override
    public boolean hasBase64EncodedKey() {
      return false;
    }

    @Override
    public byte[] getKeyBytes() {
      return _keyData;
    }

    @Override
    public boolean hasOrderingKey() {
      return false;
    }

    @Override
    public byte[] getOrderingKey() {
      return new byte[0];
    }

    @Override
    public String getTopicName() {
      return null;
    }

    @Override
    public Optional<EncryptionContext> getEncryptionCtx() {
      return Optional.empty();
    }

    @Override
    public int getRedeliveryCount() {
      return 0;
    }

    @Override
    public byte[] getSchemaVersion() {
      return new byte[0];
    }

    @Override
    public boolean isReplicated() {
      return false;
    }

    @Override
    public String getReplicatedFrom() {
      return null;
    }

    @Override
    public void release() {
    }

    @Override
    public boolean hasBrokerPublishTime() {
      return false;
    }

    @Override
    public Optional<Long> getBrokerPublishTime() {
      return Optional.empty();
    }

    @Override
    public boolean hasIndex() {
      return false;
    }

    @Override
    public Optional<Long> getIndex() {
      return Optional.empty();
    }
  }

  @BeforeClass
  public void setup() {
    _expectedValueBytes = new byte[10];
    _expectedKeyBytes = new byte[10];
    RANDOM.nextBytes(_expectedValueBytes);
    RANDOM.nextBytes(_expectedKeyBytes);
    _message = new DummyPulsarMessage(_expectedKeyBytes, _expectedValueBytes);
  }

  @Test
  public void testMessageBatchNoStitching() {
    PulsarConfig config = mock(PulsarConfig.class);
    when(config.getEnableKeyValueStitch()).thenReturn(false);
    List<BytesStreamMessage> streamMessages = List.of(PulsarUtils.buildPulsarStreamMessage(_message, config));
    PulsarMessageBatch messageBatch = new PulsarMessageBatch(streamMessages, mock(MessageIdStreamOffset.class), false);
    byte[] valueBytes = messageBatch.getStreamMessage(0).getValue();
    assertEquals(valueBytes, _expectedValueBytes);
  }

  @Test
  public void testMessageBatchWithStitching() {
    PulsarConfig config = mock(PulsarConfig.class);
    when(config.getEnableKeyValueStitch()).thenReturn(true);
    List<BytesStreamMessage> streamMessages = List.of(PulsarUtils.buildPulsarStreamMessage(_message, config));
    PulsarMessageBatch messageBatch = new PulsarMessageBatch(streamMessages, mock(MessageIdStreamOffset.class), false);
    BytesStreamMessage streamMessage = messageBatch.getStreamMessage(0);
    byte[] keyValueBytes = streamMessage.getValue();
    assertNotNull(keyValueBytes);
    assertEquals(keyValueBytes.length, 8 + _expectedKeyBytes.length + _expectedValueBytes.length);
    ByteBuffer byteBuffer = ByteBuffer.wrap(keyValueBytes);
    int keyLength = byteBuffer.getInt();
    byte[] keyBytes = new byte[keyLength];
    byteBuffer.get(keyBytes);
    assertEquals(keyBytes, _expectedKeyBytes);
    int valueLength = byteBuffer.getInt();
    byte[] valueBytes = new byte[valueLength];
    byteBuffer.get(valueBytes);
    assertEquals(valueBytes, _expectedValueBytes);
  }
}
