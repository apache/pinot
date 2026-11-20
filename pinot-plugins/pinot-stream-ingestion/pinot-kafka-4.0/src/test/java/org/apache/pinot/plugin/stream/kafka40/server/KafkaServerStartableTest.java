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
package org.apache.pinot.plugin.stream.kafka40.server;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import org.apache.pinot.spi.utils.NetUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Verifies managed quickstart broker selection and lifecycle.
public class KafkaServerStartableTest {
  @Test
  public void testShouldStartManagedKafkaForDefaultManagedBroker() {
    KafkaServerStartable kafkaServerStartable = new KafkaServerStartable();
    kafkaServerStartable.init(new Properties());

    assertTrue(kafkaServerStartable.shouldStartManagedKafka("localhost:19092"));
    assertFalse(kafkaServerStartable.shouldStartManagedKafka("localhost:19093"));
  }

  @Test
  public void testShouldStartManagedKafkaForConfiguredLocalBrokerWhenEnabled() {
    KafkaServerStartable kafkaServerStartable = new KafkaServerStartable();
    Properties serverProperties = new Properties();
    serverProperties.put("kafka.server.allow.managed.for.configured.broker", "true");
    kafkaServerStartable.init(serverProperties);

    assertTrue(kafkaServerStartable.shouldStartManagedKafka("localhost:19093"));
    assertTrue(kafkaServerStartable.shouldStartManagedKafka("127.0.0.1:19093"));
    assertFalse(kafkaServerStartable.shouldStartManagedKafka("kafka-broker:19093"));
  }

  @Test
  public void testResolveKafkaBrokerListFallsBackToDefault() {
    TestableKafkaServerStartable kafkaServerStartable = new TestableKafkaServerStartable();
    kafkaServerStartable.setAvailableBrokers("localhost:9092");

    assertEquals(kafkaServerStartable.resolveKafkaBrokerList("localhost:19092"), "localhost:9092");
  }

  @Test(expectedExceptions = IllegalStateException.class,
      expectedExceptionsMessageRegExp = "Kafka broker list is not reachable: .*")
  public void testResolveKafkaBrokerListThrowsWhenNoBrokerAvailable() {
    TestableKafkaServerStartable kafkaServerStartable = new TestableKafkaServerStartable();
    kafkaServerStartable.resolveKafkaBrokerList("localhost:19092");
  }

  @Test
  public void testStopMakesBrokerUnavailable()
      throws Exception {
    int kafkaServerPort = NetUtils.findOpenPort();
    String brokerList = "localhost:" + kafkaServerPort;
    Properties serverProperties = new Properties();
    serverProperties.put("kafka.server.bootstrap.servers", brokerList);
    serverProperties.put("kafka.server.port", Integer.toString(kafkaServerPort));
    serverProperties.put("kafka.server.broker.id", "0");
    serverProperties.put("kafka.server.owner.name", getClass().getSimpleName());
    serverProperties.put("kafka.server.allow.managed.for.configured.broker", "true");

    KafkaServerStartable kafkaServerStartable = new KafkaServerStartable();
    kafkaServerStartable.init(serverProperties);
    kafkaServerStartable.start();
    try {
      assertTrue(kafkaServerStartable.isKafkaAvailable(brokerList),
          "Kafka broker should be reachable while running");
    } finally {
      kafkaServerStartable.stop();
    }
    assertFalse(kafkaServerStartable.isKafkaAvailable(brokerList),
        "Kafka broker should be unavailable after stop");
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
