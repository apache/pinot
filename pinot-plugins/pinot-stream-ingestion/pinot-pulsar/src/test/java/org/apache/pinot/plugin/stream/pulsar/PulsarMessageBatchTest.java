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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.EncryptionContext;
import org.junit.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PulsarMessageBatchTest {
  private Random _random = new Random();
  private DummyPulsarMessage _msgWithKeyAndValue;
  private byte[] _expectedValueBytes;
  private byte[] _expectedKeyBytes;
  private List<DummyPulsarMessage> _messageList;
  private PulsarMetadataExtractor _metadataExtractor;

  public static class DummyPulsarMessage implements Message<byte[]> {
    private final byte[] _keyData;
    private final byte[] _valueData;
    private Map<String, String> _properties;

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
      return _keyData == null ? false : true;
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
    _random.nextBytes(_expectedValueBytes);
    _random.nextBytes(_expectedKeyBytes);
    _msgWithKeyAndValue = new DummyPulsarMessage(_expectedKeyBytes, _expectedValueBytes);
    _messageList = new ArrayList<>();
    _metadataExtractor = PulsarMetadataExtractor.build(true);
    _messageList.add(_msgWithKeyAndValue);
  }

  @Test
  public void testMessageBatchNoStitching() {
    List<PulsarStreamMessage> streamMessages = _messageList.stream().map(message ->
            PulsarUtils.buildPulsarStreamMessage(message, false, _metadataExtractor))
            .collect(Collectors.toList());
    PulsarMessageBatch messageBatch = new PulsarMessageBatch(streamMessages, false);
    byte[] valueBytes = messageBatch.getMessageAtIndex(0).getValue();
    Assert.assertArrayEquals(_expectedValueBytes, valueBytes);
  }

  @Test
  public void testMessageBatchWithStitching() {
    List<PulsarStreamMessage> streamMessages = _messageList.stream().map(message ->
                    PulsarUtils.buildPulsarStreamMessage(message, true, _metadataExtractor))
            .collect(Collectors.toList());
    PulsarMessageBatch messageBatch = new PulsarMessageBatch(streamMessages, true);
    byte[] keyValueBytes = messageBatch.getMessageAtIndex(0).getValue();
    Assert.assertEquals(keyValueBytes.length, 8 + _expectedKeyBytes.length + _expectedValueBytes.length);
    try {
      ByteBuffer byteBuffer = ByteBuffer.wrap(keyValueBytes);
      int keyLength = byteBuffer.getInt();
      byte[] keyBytes = new byte[keyLength];
      byteBuffer.get(keyBytes);
      Assert.assertArrayEquals(_expectedKeyBytes, keyBytes);
      int valueLength = byteBuffer.getInt();
      byte[] valueBytes = new byte[valueLength];
      byteBuffer.get(valueBytes);
      Assert.assertArrayEquals(_expectedValueBytes, valueBytes);
    } catch (Exception e) {
      Assert.fail("Could not parse key and value bytes because of exception: " + e.getMessage());
    }
  }
}
