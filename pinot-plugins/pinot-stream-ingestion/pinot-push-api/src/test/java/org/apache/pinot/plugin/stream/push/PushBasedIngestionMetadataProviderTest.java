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
package org.apache.pinot.plugin.stream.push;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class PushBasedIngestionMetadataProviderTest {
  private static final String STREAM_NAME = "kinesis-test";
  private static final String CLIENT_ID = "dummy";
  private static final int TIMEOUT = 1000;

  private PushBasedIngestionMetadataProvider _metadataProvider;
  private StreamConsumerFactory _streamConsumerFactory;
  private PartitionGroupConsumer _partitionGroupConsumer;

  private StreamConfig getStreamConfig() {
    Map<String, String> props = new HashMap<>();
    props.put(StreamConfigProperties.STREAM_TYPE, "push");
    props.put("stream.push.consumer.type", "lowLevel");
    props.put("stream.push.topic.name", STREAM_NAME);
    props.put("stream.push.decoder.class.name", "ABCD");
    props.put("stream.push.consumer.factory.class.name",
        "org.apache.pinot.plugin.stream.push.PushBasedIngestionConsumerFactory");
    return new StreamConfig("pushTest", props);
  }

  @BeforeMethod
  public void setupTest() {
    _streamConsumerFactory = mock(StreamConsumerFactory.class);
    _partitionGroupConsumer = mock(PartitionGroupConsumer.class);
    _metadataProvider = new PushBasedIngestionMetadataProvider(CLIENT_ID, getStreamConfig(), _streamConsumerFactory);
  }

  @Test
  public void getPartitionsGroupInfoListTest()
      throws Exception {
    List<PartitionGroupMetadata> result =
        _metadataProvider.computePartitionGroupMetadata(CLIENT_ID, getStreamConfig(), new ArrayList<>(), TIMEOUT);

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).getPartitionGroupId(), 0);
  }
}
