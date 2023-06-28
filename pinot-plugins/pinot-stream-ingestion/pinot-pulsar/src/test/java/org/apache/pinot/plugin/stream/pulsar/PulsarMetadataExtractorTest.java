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

import java.nio.charset.StandardCharsets;
import org.apache.pulsar.client.api.MessageId;
import org.bouncycastle.util.encoders.Base64;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.stream.pulsar.PulsarMessageBatchTest.DummyPulsarMessage;
import static org.apache.pinot.plugin.stream.pulsar.PulsarStreamMessageMetadata.MESSAGE_ID_BYTES_B64_KEY;
import static org.apache.pinot.plugin.stream.pulsar.PulsarStreamMessageMetadata.MESSAGE_ID_KEY;
import static org.apache.pinot.plugin.stream.pulsar.PulsarStreamMessageMetadata.MESSAGE_KEY_KEY;
import static org.testng.Assert.assertEquals;


public class PulsarMetadataExtractorTest {

  private PulsarMetadataExtractor _metadataExtractor;

  @BeforeClass
  public void setup() {
    _metadataExtractor = PulsarMetadataExtractor.build(true, false);
  }

  @Test
  public void testExtractProperty()
      throws Exception {
    DummyPulsarMessage pulsarMessage =
        new DummyPulsarMessage("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8));
    pulsarMessage.getProperties().put("test_key", "test_value");
    pulsarMessage.getProperties().put("test_key2", "2");
    PulsarStreamMessageMetadata metadata = (PulsarStreamMessageMetadata) _metadataExtractor.extract(pulsarMessage);
    assertEquals("test_value", metadata.getHeaders().getValue("test_key"));
    assertEquals("2", metadata.getHeaders().getValue("test_key2"));
    assertEquals("key", metadata.getRecordMetadata().get(MESSAGE_KEY_KEY));
    String messageIdStr = metadata.getRecordMetadata().get(MESSAGE_ID_KEY);
    assertEquals(pulsarMessage.getMessageId().toString(), messageIdStr);

    byte[] messageIdBytes = Base64.decode(metadata.getRecordMetadata().get(MESSAGE_ID_BYTES_B64_KEY));
    MessageId messageId = MessageId.fromByteArray(messageIdBytes);
    assertEquals(MessageId.earliest, messageId);
  }

  @Test
  public void testPulsarSteamMessageUnstitched() {
    String key = "key";
    String value = "value";
    DummyPulsarMessage dummyPulsarMessage =
        new DummyPulsarMessage(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    PulsarStreamMessage streamMessage =
        PulsarUtils.buildPulsarStreamMessage(dummyPulsarMessage, false, _metadataExtractor);
    assertEquals(key.getBytes(StandardCharsets.UTF_8), streamMessage.getKey());
    assertEquals(value.getBytes(StandardCharsets.UTF_8), streamMessage.getValue());
    assertEquals(key.getBytes(StandardCharsets.UTF_8).length, streamMessage.getKeyLength());
    assertEquals(value.getBytes(StandardCharsets.UTF_8).length, streamMessage.getValueLength());
  }

  @Test
  public void testPulsarSteamMessageStitched() {
    String key = "key";
    String value = "value";
    byte[] stitchedValueBytes =
        PulsarUtils.stitchKeyValue(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    DummyPulsarMessage dummyPulsarMessage =
        new DummyPulsarMessage(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    PulsarStreamMessage streamMessage =
        PulsarUtils.buildPulsarStreamMessage(dummyPulsarMessage, true, _metadataExtractor);
    assertEquals(key.getBytes(StandardCharsets.UTF_8), streamMessage.getKey());
    assertEquals(stitchedValueBytes, streamMessage.getValue());
    assertEquals(key.getBytes(StandardCharsets.UTF_8).length, streamMessage.getKeyLength());
    assertEquals(stitchedValueBytes.length, streamMessage.getValueLength());
  }
}
