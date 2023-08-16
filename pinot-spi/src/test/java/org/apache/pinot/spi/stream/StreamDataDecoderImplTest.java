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

import com.google.common.collect.ImmutableSet;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StreamDataDecoderImplTest {
  private static final String NAME_FIELD = "name";
  private static final String AGE_HEADER_KEY = "age";
  private static final String SEQNO_RECORD_METADATA = "seqNo";

  @Test
  public void testDecodeValueOnly()
      throws Exception {
    TestDecoder messageDecoder = new TestDecoder();
    messageDecoder.init(Collections.emptyMap(), ImmutableSet.of(NAME_FIELD), "");
    String value = "Alice";
    StreamMessage<byte[]> message = new StreamMessage(value.getBytes(StandardCharsets.UTF_8),
        value.getBytes(StandardCharsets.UTF_8).length, null);
    StreamDataDecoderResult result = new StreamDataDecoderImpl(messageDecoder).decode(message);
    Assert.assertNotNull(result);
    Assert.assertNull(result.getException());
    Assert.assertNotNull(result.getResult());

    GenericRow row = result.getResult();
    Assert.assertEquals(row.getFieldToValueMap().size(), 1);
    Assert.assertEquals(String.valueOf(row.getValue(NAME_FIELD)), value);
  }

  @Test
  public void testDecodeKeyAndHeaders()
      throws Exception {
    TestDecoder messageDecoder = new TestDecoder();
    messageDecoder.init(Collections.emptyMap(), ImmutableSet.of(NAME_FIELD), "");
    String value = "Alice";
    String key = "id-1";
    GenericRow headers = new GenericRow();
    headers.putValue(AGE_HEADER_KEY, 3);
    Map<String, String> recordMetadata = Collections.singletonMap(SEQNO_RECORD_METADATA, "1");
    StreamMessageMetadata metadata = new StreamMessageMetadata(1234L, headers, recordMetadata);
    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
    StreamMessage<byte[]> message =
        new StreamMessage(key.getBytes(StandardCharsets.UTF_8), valueBytes, metadata, value.length());

    StreamDataDecoderResult result = new StreamDataDecoderImpl(messageDecoder).decode(message);
    Assert.assertNotNull(result);
    Assert.assertNull(result.getException());
    Assert.assertNotNull(result.getResult());

    GenericRow row = result.getResult();
    Assert.assertEquals(row.getFieldToValueMap().size(), 4);
    Assert.assertEquals(row.getValue(NAME_FIELD), value);
    Assert.assertEquals(row.getValue(StreamDataDecoderImpl.KEY), key, "Failed to decode record key");
    Assert.assertEquals(row.getValue(StreamDataDecoderImpl.HEADER_KEY_PREFIX + AGE_HEADER_KEY), 3);
    Assert.assertEquals(row.getValue(StreamDataDecoderImpl.METADATA_KEY_PREFIX + SEQNO_RECORD_METADATA), "1");
  }

  @Test
  public void testNoExceptionIsThrown()
      throws Exception {
    ThrowingDecoder messageDecoder = new ThrowingDecoder();
    messageDecoder.init(Collections.emptyMap(), ImmutableSet.of(NAME_FIELD), "");
    String value = "Alice";
    StreamMessage<byte[]> message = new StreamMessage(value.getBytes(StandardCharsets.UTF_8),
        value.getBytes(StandardCharsets.UTF_8).length, null);
    StreamDataDecoderResult result = new StreamDataDecoderImpl(messageDecoder).decode(message);
    Assert.assertNotNull(result);
    Assert.assertNotNull(result.getException());
    Assert.assertNull(result.getResult());
  }

  class ThrowingDecoder implements StreamMessageDecoder<byte[]> {

    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
        throws Exception { }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      throw new RuntimeException("something failed during decoding");
    }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      return decode(payload, destination);
    }
  }


  class TestDecoder implements StreamMessageDecoder<byte[]> {
    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
        throws Exception { }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, GenericRow destination) {
      destination.putValue(NAME_FIELD, new String(payload, StandardCharsets.UTF_8));
      return destination;
    }

    @Nullable
    @Override
    public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
      return decode(payload, destination);
    }
  }
}
