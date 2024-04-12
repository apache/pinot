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
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pulsar.client.api.MessageId;
import org.bouncycastle.util.encoders.Base64;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.stream.pulsar.PulsarMessageBatchTest.DummyPulsarMessage;
import static org.apache.pinot.plugin.stream.pulsar.PulsarStreamMessageMetadata.PulsarMessageMetadataValue.MESSAGE_ID;
import static org.apache.pinot.plugin.stream.pulsar.PulsarStreamMessageMetadata.PulsarMessageMetadataValue.MESSAGE_ID_BYTES_B64;
import static org.apache.pinot.plugin.stream.pulsar.PulsarStreamMessageMetadata.PulsarMessageMetadataValue.MESSAGE_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class PulsarUtilsTest {

  @Test
  public void testExtractProperty()
      throws Exception {
    PulsarConfig config = mock(PulsarConfig.class);
    when(config.isPopulateMetadata()).thenReturn(true);
    when(config.getMetadataFields()).thenReturn(Set.of(MESSAGE_ID, MESSAGE_ID_BYTES_B64, MESSAGE_KEY));
    DummyPulsarMessage pulsarMessage =
        new DummyPulsarMessage("key".getBytes(StandardCharsets.UTF_8), "value".getBytes(StandardCharsets.UTF_8));
    pulsarMessage.getProperties().put("test_key", "test_value");
    pulsarMessage.getProperties().put("test_key2", "2");
    StreamMessageMetadata metadata = PulsarUtils.extractMessageMetadata(pulsarMessage, config);
    GenericRow headers = metadata.getHeaders();
    assertNotNull(headers);
    assertEquals(headers.getValue("test_key"), "test_value");
    assertEquals(headers.getValue("test_key2"), "2");
    Map<String, String> recordMetadata = metadata.getRecordMetadata();
    assertNotNull(recordMetadata);
    assertEquals(recordMetadata.get(MESSAGE_KEY.getKey()), "key");
    assertEquals(recordMetadata.get(MESSAGE_ID.getKey()), pulsarMessage.getMessageId().toString());

    byte[] messageIdBytes = Base64.decode(recordMetadata.get(MESSAGE_ID_BYTES_B64.getKey()));
    MessageId messageId = MessageId.fromByteArray(messageIdBytes);
    assertEquals(MessageId.earliest, messageId);
  }

  @Test
  public void testPulsarSteamMessageUnstitched() {
    PulsarConfig config = mock(PulsarConfig.class);
    when(config.getEnableKeyValueStitch()).thenReturn(false);
    String key = "key";
    String value = "value";
    DummyPulsarMessage dummyPulsarMessage =
        new DummyPulsarMessage(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    BytesStreamMessage streamMessage = PulsarUtils.buildPulsarStreamMessage(dummyPulsarMessage, config);
    assertEquals(streamMessage.getKey(), key.getBytes(StandardCharsets.UTF_8));
    assertEquals(streamMessage.getValue(), value.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testPulsarSteamMessageStitched() {
    PulsarConfig config = mock(PulsarConfig.class);
    when(config.getEnableKeyValueStitch()).thenReturn(true);
    String key = "key";
    String value = "value";
    byte[] stitchedValueBytes =
        PulsarUtils.stitchKeyValue(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    DummyPulsarMessage dummyPulsarMessage =
        new DummyPulsarMessage(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    BytesStreamMessage streamMessage = PulsarUtils.buildPulsarStreamMessage(dummyPulsarMessage, config);
    assertEquals(streamMessage.getKey(), key.getBytes(StandardCharsets.UTF_8));
    assertEquals(streamMessage.getValue(), stitchedValueBytes);
  }
}
