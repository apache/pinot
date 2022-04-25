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

package org.apache.pinot.tools.streams;

import java.util.concurrent.ExecutorService;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotRealtimeSourceTest {

  @Test
  public void testBuilder() {
    StreamDataProducer producer = Mockito.mock(StreamDataProducer.class);
    PinotSourceDataGenerator generator = Mockito.mock(PinotSourceDataGenerator.class);
    PinotRealtimeSource realtimeSource =
        PinotRealtimeSource.builder().setTopic("mytopic").setProducer(producer).setGenerator(generator).build();
    Assert.assertNotNull(realtimeSource);

    PinotStreamRateLimiter limiter = Mockito.mock(PinotStreamRateLimiter.class);
    ExecutorService executorService = Mockito.mock(ExecutorService.class);
    realtimeSource = PinotRealtimeSource.builder().setRateLimiter(limiter).setProducer(producer).setGenerator(generator)
        .setTopic("mytopic").setExecutor(executorService).setMaxMessagePerSecond(9527).build();
    Assert.assertEquals(realtimeSource._executor, executorService);
    Assert.assertEquals(realtimeSource._producer, producer);
    Assert.assertEquals(realtimeSource._topicName, "mytopic");
    String qps = realtimeSource._properties.get(PinotRealtimeSource.KEY_OF_MAX_MESSAGE_PER_SECOND).toString();
    Assert.assertNotNull(qps);
    Assert.assertEquals(qps, "9527");
    Assert.assertEquals(realtimeSource._rateLimiter, limiter);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testBuilderNoNullProducerThrowExceptions() {
    PinotSourceDataGenerator generator = Mockito.mock(PinotSourceDataGenerator.class);
    PinotRealtimeSource realtimeSource =
        PinotRealtimeSource.builder().setTopic("mytopic").setGenerator(generator).build();
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testBuilderNoNullGeneratorThrowExceptions() {
    StreamDataProducer producer = Mockito.mock(StreamDataProducer.class);
    PinotRealtimeSource realtimeSource =
        PinotRealtimeSource.builder().setTopic("mytopic").setProducer(producer).build();
  }
}
