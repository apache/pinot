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
package org.apache.pinot.plugin.stream.kafka30.server;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.pinot.spi.utils.NetUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaServerStartableTest {
  @Test
  public void testShouldStartManagedKafkaForDefaultManagedBroker() {
    KafkaServerStartable kafkaServerStartable = new KafkaServerStartable();
    kafkaServerStartable.init(new Properties());

    Assert.assertTrue(kafkaServerStartable.shouldStartManagedKafka("localhost:19092"));
    Assert.assertFalse(kafkaServerStartable.shouldStartManagedKafka("localhost:19093"));
  }

  @Test
  public void testShouldStartManagedKafkaForConfiguredLocalBrokerWhenEnabled() {
    KafkaServerStartable kafkaServerStartable = new KafkaServerStartable();
    Properties serverProperties = new Properties();
    serverProperties.put("kafka.server.allow.managed.for.configured.broker", "true");
    kafkaServerStartable.init(serverProperties);

    Assert.assertTrue(kafkaServerStartable.shouldStartManagedKafka("localhost:19093"));
    Assert.assertTrue(kafkaServerStartable.shouldStartManagedKafka("127.0.0.1:19093"));
    Assert.assertFalse(kafkaServerStartable.shouldStartManagedKafka("kafka-broker:19093"));
  }

  @Test
  public void testResolveKafkaBrokerListFallsBackToDefault() {
    TestableKafkaServerStartable kafkaServerStartable = new TestableKafkaServerStartable();
    kafkaServerStartable.setAvailableBrokers("localhost:9092");

    Assert.assertEquals(kafkaServerStartable.resolveKafkaBrokerList("localhost:19092"), "localhost:9092");
  }

  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = "Kafka broker list is not reachable: .*")
  public void testResolveKafkaBrokerListThrowsWhenNoBrokerAvailable() {
    TestableKafkaServerStartable kafkaServerStartable = new TestableKafkaServerStartable();
    kafkaServerStartable.resolveKafkaBrokerList("localhost:19092");
  }

  @Test
  public void testStopReleasesBrokerPort()
      throws Exception {
    int kafkaServerPort = NetUtils.findOpenPort();
    Properties serverProperties = new Properties();
    serverProperties.put("kafka.server.bootstrap.servers", "localhost:" + kafkaServerPort);
    serverProperties.put("kafka.server.port", Integer.toString(kafkaServerPort));
    serverProperties.put("kafka.server.broker.id", "0");
    serverProperties.put("kafka.server.owner.name", getClass().getSimpleName());
    serverProperties.put("kafka.server.allow.managed.for.configured.broker", "true");

    KafkaServerStartable kafkaServerStartable = new KafkaServerStartable();
    kafkaServerStartable.init(serverProperties);
    kafkaServerStartable.start();
    try {
      Assert.assertFalse(NetUtils.available(kafkaServerPort), "Kafka port should be in use while broker is running");
    } finally {
      kafkaServerStartable.stop();
    }
    Assert.assertTrue(NetUtils.available(kafkaServerPort), "Kafka port should be released after broker stop");
  }

  private static final class TestableKafkaServerStartable extends KafkaServerStartable {
    private final Set<String> _availableBrokers = new HashSet<>();

    void setAvailableBrokers(String... availableBrokers) {
      _availableBrokers.clear();
      _availableBrokers.addAll(Arrays.asList(availableBrokers));
    }

    @Override
    boolean isKafkaAvailable(String brokerList) {
      return _availableBrokers.contains(brokerList);
    }
  }
}
