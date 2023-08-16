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

import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StreamMessageTest {

  @Test
  public void testAllowNullKeyAndMetadata() {
    String value = "hello";
    byte[] valBytes = value.getBytes(StandardCharsets.UTF_8);
    StreamMessage<byte[]> msg = new StreamMessage(valBytes, valBytes.length, null);
    Assert.assertNull(msg.getKey());
    Assert.assertNull(msg.getMetadata());
    Assert.assertEquals(new String(msg.getValue()), value);

    value = "value";
    valBytes = value.getBytes(StandardCharsets.UTF_8);
    StreamMessage<byte[]> msg1 =
        new StreamMessage("key".getBytes(StandardCharsets.UTF_8), valBytes, null, valBytes.length);
    Assert.assertNotNull(msg1.getKey());
    Assert.assertEquals(new String(msg1.getKey()), "key");
    Assert.assertNotNull(msg1.getValue());
    Assert.assertEquals(new String(msg1.getValue()), value);
    Assert.assertNull(msg1.getMetadata());

    StreamMessage<String> msg2 = new StreamMessage<>("key".getBytes(StandardCharsets.UTF_8), value, null,
        value.length());
    Assert.assertNotNull(msg2.getKey());
    Assert.assertEquals(new String(msg2.getKey()), "key");
    Assert.assertNotNull(msg2.getValue());
    Assert.assertEquals(msg2.getValue(), value);
    Assert.assertNull(msg2.getMetadata());
  }
}
