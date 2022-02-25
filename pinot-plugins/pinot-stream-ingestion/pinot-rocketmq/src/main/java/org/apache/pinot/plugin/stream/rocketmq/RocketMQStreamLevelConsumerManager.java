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
package org.apache.pinot.plugin.stream.rocketmq;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implements RocketMQ high level connection manager.
 */
public class RocketMQStreamLevelConsumerManager {
  private RocketMQStreamLevelConsumerManager() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(RocketMQStreamLevelConsumerManager.class);
  private static final Long IN_USE = -1L;
  private static final long CONSUMER_SHUTDOWN_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(60); // One minute
  private static final Map<ImmutableTriple<String, String, String>, DefaultLitePullConsumer> CONSUMER_FOR_CONFIG_KEY =
      new HashMap<>();
  private static final IdentityHashMap<DefaultLitePullConsumer, Long> CONSUMER_RELEASE_TIME = new IdentityHashMap<>();

  /**
   * Get {@link DefaultLitePullConsumer} for {@link RocketMQConfig}. If the reader is already created we return the
   * instance, otherwise
   * a new reader is created.
   */
  public static DefaultLitePullConsumer acquireRocketMQConsumerForConfig(
      RocketMQConfig rocketmqStreamLevelStreamConfig) {
    final ImmutableTriple<String, String, String> configKey =
        new ImmutableTriple<>(rocketmqStreamLevelStreamConfig.getRocketMQTopicName(),
            rocketmqStreamLevelStreamConfig.getConsumerGroupId(), rocketmqStreamLevelStreamConfig.getNameServer());

    synchronized (RocketMQStreamLevelConsumerManager.class) {
      // If we have the consumer and it's not already acquired, return it, otherwise error out if it's already acquired
      if (CONSUMER_FOR_CONFIG_KEY.containsKey(configKey)) {
        DefaultLitePullConsumer rocketmqConsumer = CONSUMER_FOR_CONFIG_KEY.get(configKey);
        if (CONSUMER_RELEASE_TIME.get(rocketmqConsumer).equals(IN_USE)) {
          throw new RuntimeException("Consumer " + rocketmqConsumer + " already in use!");
        } else {
          LOGGER.info("Reusing rocketmq consumer with id {}", rocketmqConsumer);
          CONSUMER_RELEASE_TIME.put(rocketmqConsumer, IN_USE);
          return rocketmqConsumer;
        }
      }

      LOGGER.info("Creating new rocketmq consumer and iterator for topic {}",
          rocketmqStreamLevelStreamConfig.getRocketMQTopicName());

      // Create the consumer
      try {
        DefaultLitePullConsumer
            consumer = new DefaultLitePullConsumer(rocketmqStreamLevelStreamConfig.getConsumerNamespace(),
            rocketmqStreamLevelStreamConfig.getConsumerGroupId(), null);
        consumer.setNamesrvAddr(rocketmqStreamLevelStreamConfig.getNameServer());
        consumer.setAutoCommit(false);
        consumer.subscribe(rocketmqStreamLevelStreamConfig.getRocketMQTopicName(), "*");
        consumer.setConsumeFromWhere(rocketmqStreamLevelStreamConfig.getConsumerFromWhere());
        if (ConsumeFromWhere.CONSUME_FROM_TIMESTAMP.equals(rocketmqStreamLevelStreamConfig.getConsumerFromWhere())) {
          consumer.setConsumeTimestamp(rocketmqStreamLevelStreamConfig.getConsumeTimestamp());
        }
        consumer.start();

        // Mark both the consumer and iterator as acquired
        CONSUMER_FOR_CONFIG_KEY.put(configKey, consumer);
        CONSUMER_RELEASE_TIME.put(consumer, IN_USE);

        LOGGER.info("Created consumer with id {} for topic {}", consumer,
            rocketmqStreamLevelStreamConfig.getRocketMQTopicName());

        return consumer;
      } catch (MQClientException e) {
        LOGGER.error("Could not create rocketmq consumer", e);
        return null;
      }
    }
  }

  /**
   *  remove the {@link DefaultLitePullConsumer} from consumer pool after closing it.
   */
  public static void releaseRocketMQConsumer(final DefaultLitePullConsumer rocketmqConsumer) {
    synchronized (RocketMQStreamLevelConsumerManager.class) {
      // Release the consumer, mark it for shutdown in the future
      final long releaseTime = System.currentTimeMillis() + CONSUMER_SHUTDOWN_DELAY_MILLIS;
      CONSUMER_RELEASE_TIME.put(rocketmqConsumer, releaseTime);

      LOGGER.info("Marking consumer with id {} for release at {}", rocketmqConsumer, releaseTime);

      // Schedule the shutdown of the consumer
      new Thread() {
        @Override
        public void run() {
          try {
            // Await the shutdown time
            Uninterruptibles.sleepUninterruptibly(CONSUMER_SHUTDOWN_DELAY_MILLIS, TimeUnit.MILLISECONDS);

            // Shutdown all consumers that have not been re-acquired
            synchronized (RocketMQStreamLevelConsumerManager.class) {
              LOGGER.info("Executing release check for consumer {} at {}, scheduled at {}", rocketmqConsumer,
                  System.currentTimeMillis(), releaseTime);

              Iterator<Map.Entry<ImmutableTriple<String, String, String>, DefaultLitePullConsumer>> configIterator =
                  CONSUMER_FOR_CONFIG_KEY.entrySet().iterator();

              while (configIterator.hasNext()) {
                Map.Entry<ImmutableTriple<String, String, String>, DefaultLitePullConsumer> entry =
                    configIterator.next();
                DefaultLitePullConsumer rocketmqConsumer = entry.getValue();

                final Long releaseTime = CONSUMER_RELEASE_TIME.get(rocketmqConsumer);
                if (!releaseTime.equals(IN_USE) && releaseTime < System.currentTimeMillis()) {
                  LOGGER.info("Releasing consumer {}", rocketmqConsumer);

                  try {
                    rocketmqConsumer.shutdown();
                  } catch (Exception e) {
                    LOGGER.warn("Caught exception while shutting down RocketMQ consumer with id {}", rocketmqConsumer,
                        e);
                  }

                  configIterator.remove();
                  CONSUMER_RELEASE_TIME.remove(rocketmqConsumer);
                } else {
                  LOGGER.info("Not releasing consumer {}, it has been reacquired", rocketmqConsumer);
                }
              }
            }
          } catch (Exception e) {
            LOGGER.warn("Caught exception in release of consumer {}", rocketmqConsumer, e);
          }
        }
      }.start();
    }
  }

  public static void closeAllConsumers() {
    try {
      // Shutdown all consumers
      synchronized (RocketMQStreamLevelConsumerManager.class) {
        LOGGER.info("Trying to shutdown all the rocketmq consumers");
        Iterator<DefaultLitePullConsumer> consumerIterator = CONSUMER_FOR_CONFIG_KEY.values().iterator();

        while (consumerIterator.hasNext()) {
          DefaultLitePullConsumer rocketmqConsumer = consumerIterator.next();
          LOGGER.info("Trying to shutdown consumer {}", rocketmqConsumer);
          try {
            rocketmqConsumer.shutdown();
          } catch (Exception e) {
            LOGGER.warn("Caught exception while shutting down RocketMQ consumer with id {}", rocketmqConsumer, e);
          }
          consumerIterator.remove();
        }
        CONSUMER_FOR_CONFIG_KEY.clear();
        CONSUMER_RELEASE_TIME.clear();
      }
    } catch (Exception e) {
      LOGGER.warn("Caught exception during shutting down all rocketmq consumers", e);
    }
  }
}
