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
package org.apache.pinot.plugin.stream.kafka20;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manager for Kafka consumers that reuses consumers and delays their shutdown.
 *
 * This is a workaround for the current realtime design flaw where any issue while flushing/committing offsets causes
 * duplicate or dropped events. Kafka consumption is driven by the controller, which assigns a realtime segment to the
 * servers; when a server is assigned a new realtime segment, it creates a Kafka consumer, consumes until it reaches a
 * threshold then flushes to disk, writes metadata to helix indicating the segment is completed, commits Kafka offsets
 * to ZK and then shuts down the consumer. The controller notices the metadata write and reassigns a segment to the
 * server, so that it can keep on consuming.
 *
 * This logic is flawed if committing Kafka offsets fails, at which time the committed state is unknown. The proper fix
 * would be to just keep on using that consumer and try committing our offsets later, but we recreate a new Kafka
 * consumer whenever we get a new segment and also keep the old consumer around, leading to half the events being
 * assigned, due to Kafka rebalancing the partitions between the two consumers (one of which is not actually reading
 * anything anymore). Because that logic is stateless and driven by Helix, there's no real clean way to keep the
 * consumer alive and pass it to the next segment.
 *
 * This class and long comment is to work around this issue by keeping the consumer alive for a little bit instead of
 * shutting it down immediately, so that the next segment assignment can pick up the same consumer. This way, even if
 * committing the offsets fails, we can still pick up the same consumer the next time we get a segment assigned to us
 * by the controller and hopefully commit our offsets the next time we flush to disk.
 *
 * This temporary code should be completely removed by the time we redesign the consumption to use the lower level
 * Kafka APIs.
 */
public class KafkaStreamLevelConsumerManager {
  private KafkaStreamLevelConsumerManager() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamLevelConsumerManager.class);
  private static final Long IN_USE = -1L;
  private static final long CONSUMER_SHUTDOWN_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(60); // One minute
  private static final Map<ImmutableTriple<String, String, String>, KafkaConsumer> CONSUMER_FOR_CONFIG_KEY =
      new HashMap<>();
  private static final IdentityHashMap<KafkaConsumer, Long> CONSUMER_RELEASE_TIME = new IdentityHashMap<>();

  public static KafkaConsumer acquireKafkaConsumerForConfig(KafkaStreamLevelStreamConfig kafkaStreamLevelStreamConfig) {
    final ImmutableTriple<String, String, String> configKey =
        new ImmutableTriple<>(kafkaStreamLevelStreamConfig.getKafkaTopicName(), kafkaStreamLevelStreamConfig.getGroupId(),
            kafkaStreamLevelStreamConfig.getBootstrapServers());

    synchronized (KafkaStreamLevelConsumerManager.class) {
      // If we have the consumer and it's not already acquired, return it, otherwise error out if it's already acquired
      if (CONSUMER_FOR_CONFIG_KEY.containsKey(configKey)) {
        KafkaConsumer kafkaConsumer = CONSUMER_FOR_CONFIG_KEY.get(configKey);
        if (CONSUMER_RELEASE_TIME.get(kafkaConsumer).equals(IN_USE)) {
          throw new RuntimeException("Consumer " + kafkaConsumer + " already in use!");
        } else {
          LOGGER.info("Reusing kafka consumer with id {}", kafkaConsumer);
          CONSUMER_RELEASE_TIME.put(kafkaConsumer, IN_USE);
          return kafkaConsumer;
        }
      }

      LOGGER.info("Creating new kafka consumer and iterator for topic {}",
          kafkaStreamLevelStreamConfig.getKafkaTopicName());

      // Create the consumer

      Properties consumerProp = new Properties();
      consumerProp.putAll(kafkaStreamLevelStreamConfig.getKafkaConsumerProperties());
      consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaStreamLevelStreamConfig.getBootstrapServers());
      consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());

      if (consumerProp.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG) && consumerProp
          .getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).equals("smallest")) {
        consumerProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      }
      KafkaConsumer consumer = new KafkaConsumer<>(consumerProp);
      consumer.subscribe(Collections.singletonList(kafkaStreamLevelStreamConfig.getKafkaTopicName()));

      // Mark both the consumer and iterator as acquired
      CONSUMER_FOR_CONFIG_KEY.put(configKey, consumer);
      CONSUMER_RELEASE_TIME.put(consumer, IN_USE);

      LOGGER.info("Created consumer with id {} for topic {}", consumer, kafkaStreamLevelStreamConfig.getKafkaTopicName());

      return consumer;
    }
  }

  public static void releaseKafkaConsumer(final KafkaConsumer kafkaConsumer) {
    synchronized (KafkaStreamLevelConsumerManager.class) {
      // Release the consumer, mark it for shutdown in the future
      final long releaseTime = System.currentTimeMillis() + CONSUMER_SHUTDOWN_DELAY_MILLIS;
      CONSUMER_RELEASE_TIME.put(kafkaConsumer, releaseTime);

      LOGGER.info("Marking consumer with id {} for release at {}", kafkaConsumer, releaseTime);

      // Schedule the shutdown of the consumer
      new Thread() {
        @Override
        public void run() {
          try {
            // Await the shutdown time
            Uninterruptibles.sleepUninterruptibly(CONSUMER_SHUTDOWN_DELAY_MILLIS, TimeUnit.MILLISECONDS);

            // Shutdown all consumers that have not been re-acquired
            synchronized (KafkaStreamLevelConsumerManager.class) {
              LOGGER.info("Executing release check for consumer {} at {}, scheduled at {}", kafkaConsumer,
                  System.currentTimeMillis(), releaseTime);

              Iterator<Map.Entry<ImmutableTriple<String, String, String>, KafkaConsumer>> configIterator =
                  CONSUMER_FOR_CONFIG_KEY.entrySet().iterator();

              while (configIterator.hasNext()) {
                Map.Entry<ImmutableTriple<String, String, String>, KafkaConsumer> entry = configIterator.next();
                KafkaConsumer kafkaConsumer = entry.getValue();

                final Long releaseTime = CONSUMER_RELEASE_TIME.get(kafkaConsumer);
                if (!releaseTime.equals(IN_USE) && releaseTime < System.currentTimeMillis()) {
                  LOGGER.info("Releasing consumer {}", kafkaConsumer);

                  try {
                    kafkaConsumer.close();
                  } catch (Exception e) {
                    LOGGER.warn("Caught exception while shutting down Kafka consumer with id {}", kafkaConsumer, e);
                  }

                  configIterator.remove();
                  CONSUMER_RELEASE_TIME.remove(kafkaConsumer);
                } else {
                  LOGGER.info("Not releasing consumer {}, it has been reacquired", kafkaConsumer);
                }
              }
            }
          } catch (Exception e) {
            LOGGER.warn("Caught exception in release of consumer {}", kafkaConsumer, e);
          }
        }
      }.start();
    }
  }

  public static void closeAllConsumers() {
    try {
      // Shutdown all consumers
      synchronized (KafkaStreamLevelConsumerManager.class) {
        LOGGER.info("Trying to shutdown all the kafka consumers");
        Iterator<KafkaConsumer> consumerIterator = CONSUMER_FOR_CONFIG_KEY.values().iterator();

        while (consumerIterator.hasNext()) {
          KafkaConsumer kafkaConsumer = consumerIterator.next();
          LOGGER.info("Trying to shutdown consumer {}", kafkaConsumer);
          try {
            kafkaConsumer.close();
          } catch (Exception e) {
            LOGGER.warn("Caught exception while shutting down Kafka consumer with id {}", kafkaConsumer, e);
          }
          consumerIterator.remove();
        }
        CONSUMER_FOR_CONFIG_KEY.clear();
        CONSUMER_RELEASE_TIME.clear();
      }
    } catch (Exception e) {
      LOGGER.warn("Caught exception during shutting down all kafka consumers", e);
    }
  }
}
