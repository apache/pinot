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
package org.apache.pinot.plugin.stream.kafka09;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.lang3.tuple.ImmutableTriple;
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
public class KafkaConsumerManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerManager.class);
  private static final Long IN_USE = -1L;
  private static final long CONSUMER_SHUTDOWN_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(60); // One minute
  private static final Map<ImmutableTriple<String, String, String>, ConsumerAndIterator>
      CONSUMER_AND_ITERATOR_FOR_CONFIG_KEY = new HashMap<>();
  private static final IdentityHashMap<ConsumerAndIterator, Long> CONSUMER_RELEASE_TIME = new IdentityHashMap<>();

  public static ConsumerAndIterator acquireConsumerAndIteratorForConfig(
      KafkaHighLevelStreamConfig kafkaHighLevelStreamConfig) {
    final ImmutableTriple<String, String, String> configKey =
        new ImmutableTriple<>(kafkaHighLevelStreamConfig.getKafkaTopicName(), kafkaHighLevelStreamConfig.getGroupId(),
            kafkaHighLevelStreamConfig.getZkBrokerUrl());

    synchronized (KafkaConsumerManager.class) {
      // If we have the consumer and it's not already acquired, return it, otherwise error out if it's already acquired
      if (CONSUMER_AND_ITERATOR_FOR_CONFIG_KEY.containsKey(configKey)) {
        ConsumerAndIterator consumerAndIterator = CONSUMER_AND_ITERATOR_FOR_CONFIG_KEY.get(configKey);
        if (CONSUMER_RELEASE_TIME.get(consumerAndIterator).equals(IN_USE)) {
          throw new RuntimeException("Consumer/iterator " + consumerAndIterator.getId() + " already in use!");
        } else {
          LOGGER.info("Reusing kafka consumer/iterator with id {}", consumerAndIterator.getId());
          CONSUMER_RELEASE_TIME.put(consumerAndIterator, IN_USE);
          return consumerAndIterator;
        }
      }

      LOGGER.info("Creating new kafka consumer and iterator for topic {}",
          kafkaHighLevelStreamConfig.getKafkaTopicName());

      // Create the consumer
      ConsumerConnector consumer =
          kafka.consumer.Consumer.createJavaConsumerConnector(kafkaHighLevelStreamConfig.getKafkaConsumerConfig());

      // Create the iterator (can only be done once per consumer)
      ConsumerIterator<byte[], byte[]> iterator =
          consumer.createMessageStreams(kafkaHighLevelStreamConfig.getTopicMap(1)).
              get(kafkaHighLevelStreamConfig.getKafkaTopicName()).get(0).iterator();

      // Mark both the consumer and iterator as acquired
      ConsumerAndIterator consumerAndIterator = new ConsumerAndIterator(consumer, iterator);
      CONSUMER_AND_ITERATOR_FOR_CONFIG_KEY.put(configKey, consumerAndIterator);
      CONSUMER_RELEASE_TIME.put(consumerAndIterator, IN_USE);

      LOGGER.info("Created consumer/iterator with id {} for topic {}", consumerAndIterator.getId(),
          kafkaHighLevelStreamConfig.getKafkaTopicName());

      return consumerAndIterator;
    }
  }

  public static void releaseConsumerAndIterator(final ConsumerAndIterator consumerAndIterator) {
    synchronized (KafkaConsumerManager.class) {
      // Release the consumer, mark it for shutdown in the future
      final long releaseTime = System.currentTimeMillis() + CONSUMER_SHUTDOWN_DELAY_MILLIS;
      CONSUMER_RELEASE_TIME.put(consumerAndIterator, releaseTime);

      LOGGER.info("Marking consumer/iterator with id {} for release at {}", consumerAndIterator.getId(), releaseTime);

      // Schedule the shutdown of the consumer
      new Thread() {
        @Override
        public void run() {
          try {
            // Await the shutdown time
            Uninterruptibles.sleepUninterruptibly(CONSUMER_SHUTDOWN_DELAY_MILLIS, TimeUnit.MILLISECONDS);

            // Shutdown all consumers that have not been re-acquired
            synchronized (KafkaConsumerManager.class) {
              LOGGER.info("Executing release check for consumer/iterator {} at {}, scheduled at ",
                  consumerAndIterator.getId(), System.currentTimeMillis(), releaseTime);

              Iterator<Map.Entry<ImmutableTriple<String, String, String>, ConsumerAndIterator>> configIterator =
                  CONSUMER_AND_ITERATOR_FOR_CONFIG_KEY.entrySet().iterator();

              while (configIterator.hasNext()) {
                Map.Entry<ImmutableTriple<String, String, String>, ConsumerAndIterator> entry = configIterator.next();
                ConsumerAndIterator consumerAndIterator = entry.getValue();

                final Long releaseTime = CONSUMER_RELEASE_TIME.get(consumerAndIterator);
                if (!releaseTime.equals(IN_USE) && releaseTime < System.currentTimeMillis()) {
                  LOGGER.info("Releasing consumer/iterator {}", consumerAndIterator.getId());

                  try {
                    consumerAndIterator.getConsumer().shutdown();
                  } catch (Exception e) {
                    LOGGER.warn("Caught exception while shutting down Kafka consumer with id {}",
                        consumerAndIterator.getId(), e);
                  }

                  configIterator.remove();
                  CONSUMER_RELEASE_TIME.remove(consumerAndIterator);
                } else {
                  LOGGER
                      .info("Not releasing consumer/iterator {}, it has been reacquired", consumerAndIterator.getId());
                }
              }
            }
          } catch (Exception e) {
            LOGGER.warn("Caught exception in release of consumer/iterator {}", e, consumerAndIterator);
          }
        }
      }.start();
    }
  }

  public static void closeAllConsumers() {
    try {
      // Shutdown all consumers
      synchronized (KafkaConsumerManager.class) {
        LOGGER.info("Trying to shutdown all the kafka consumers");
        Iterator<ConsumerAndIterator> consumerIterator = CONSUMER_AND_ITERATOR_FOR_CONFIG_KEY.values().iterator();

        while (consumerIterator.hasNext()) {
          ConsumerAndIterator consumerAndIterator = consumerIterator.next();
          LOGGER.info("Trying to shutdown consumer/iterator {}", consumerAndIterator.getId());
          try {
            consumerAndIterator.getConsumer().shutdown();
          } catch (Exception e) {
            LOGGER
                .warn("Caught exception while shutting down Kafka consumer with id {}", consumerAndIterator.getId(), e);
          }
          consumerIterator.remove();
        }
        CONSUMER_AND_ITERATOR_FOR_CONFIG_KEY.clear();
        CONSUMER_RELEASE_TIME.clear();
      }
    } catch (Exception e) {
      LOGGER.warn("Caught exception during shutting down all kafka consumers", e);
    }
  }
}
