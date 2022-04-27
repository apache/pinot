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


public class StreamDataProducerTest {

  @Test
  public void testRowWithKeyEquals() {
    byte[] b1 = new byte[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
    byte[] b2 = new byte[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
    byte[] b3 = new byte[]{1, 2, 3, 4, 5, 6, 7, 8};
    byte[] k1 = "somekey".getBytes(StandardCharsets.UTF_8);
    byte[] k3 = "anotherkey".getBytes(StandardCharsets.UTF_8);
    StreamDataProducer.RowWithKey nullKey1 = new StreamDataProducer.RowWithKey(null, b1);
    StreamDataProducer.RowWithKey nullKey2 = new StreamDataProducer.RowWithKey(null, b2);
    StreamDataProducer.RowWithKey nullKey3 = new StreamDataProducer.RowWithKey(null, b3);
    Assert.assertEquals(nullKey2, nullKey1);
    Assert.assertEquals(nullKey1.hashCode(), nullKey2.hashCode());
    Assert.assertNotEquals(nullKey3, nullKey1);
    Assert.assertNotEquals(nullKey3.hashCode(), nullKey1.hashCode());

    Assert.assertEquals(nullKey1, nullKey1);

    StreamDataProducer.RowWithKey b2WithKey = new StreamDataProducer.RowWithKey(k1, b2);
    Assert.assertNotEquals(nullKey2, b2WithKey);;
    StreamDataProducer.RowWithKey b1WithKey = new StreamDataProducer.RowWithKey(k1, b1);
    Assert.assertEquals(b1WithKey, b2WithKey);
    Assert.assertEquals(b1WithKey.hashCode(), b2WithKey.hashCode());

    StreamDataProducer.RowWithKey b2WithDifferentKey = new StreamDataProducer.RowWithKey(k3, b2);
    Assert.assertNotEquals(b2WithKey, b2WithDifferentKey);
  }
}
