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
package org.apache.pinot.plugin.stream.pulsar;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarStreamLevelConsumerManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarStreamLevelConsumerManager.class);
  private static final Long IN_USE = -1L;
  private static final long CONSUMER_SHUTDOWN_DELAY_MILLIS = TimeUnit.SECONDS.toMillis(60); // One minute
  private static final Map<ImmutableTriple<String, String, String>, Reader<byte[]>> CONSUMER_FOR_CONFIG_KEY =
      new HashMap<>();
  private static final IdentityHashMap<Reader<byte[]>, Long> CONSUMER_RELEASE_TIME = new IdentityHashMap<>();
  protected static PulsarClient _pulsarClient;
  protected static Reader<byte[]> _reader;

  public static Reader<byte[]> acquirePulsarConsumerForConfig(PulsarConfig pulsarStreamLevelStreamConfig) {
    final ImmutableTriple<String, String, String> configKey =
        new ImmutableTriple<>(pulsarStreamLevelStreamConfig.getPulsarTopicName(),
            pulsarStreamLevelStreamConfig.getSubscriberId(), pulsarStreamLevelStreamConfig.getBootstrapServers());

    synchronized (PulsarStreamLevelConsumerManager.class) {
      // If we have the consumer and it's not already acquired, return it, otherwise error out if it's already acquired
      if (CONSUMER_FOR_CONFIG_KEY.containsKey(configKey)) {
        Reader<byte[]> pulsarConsumer = CONSUMER_FOR_CONFIG_KEY.get(configKey);
        if (CONSUMER_RELEASE_TIME.get(pulsarConsumer).equals(IN_USE)) {
          throw new RuntimeException("Consumer " + pulsarConsumer + " already in use!");
        } else {
          LOGGER.info("Reusing pulsar consumer with id {}", pulsarConsumer);
          CONSUMER_RELEASE_TIME.put(pulsarConsumer, IN_USE);
          return pulsarConsumer;
        }
      }

      LOGGER.info("Creating new pulsar consumer and iterator for topic {}",
          pulsarStreamLevelStreamConfig.getPulsarTopicName());

      // Create the consumer

      Properties consumerProp = new Properties();
      consumerProp.putAll(pulsarStreamLevelStreamConfig.getPulsarConsumerProperties());

      try {
        _pulsarClient = PulsarClient.builder().serviceUrl(pulsarStreamLevelStreamConfig.getBootstrapServers()).build();

        _reader =
            _pulsarClient.newReader().topic(pulsarStreamLevelStreamConfig.getPulsarTopicName())
                .startMessageId(pulsarStreamLevelStreamConfig.getInitialMessageId()).create();

        // Mark both the consumer and iterator as acquired
        CONSUMER_FOR_CONFIG_KEY.put(configKey, _reader);
        CONSUMER_RELEASE_TIME.put(_reader, IN_USE);

        LOGGER.info("Created consumer with id {} for topic {}", _reader,
            pulsarStreamLevelStreamConfig.getPulsarTopicName());

        return _reader;
      } catch (PulsarClientException e) {
        LOGGER.error("Could not create pulsar consumer", e);
        return null;
      }
    }
  }

  public static void releasePulsarConsumer(final Reader<byte[]> pulsarConsumer) {
    synchronized (PulsarStreamLevelConsumerManager.class) {
      // Release the consumer, mark it for shutdown in the future
      final long releaseTime = System.currentTimeMillis() + CONSUMER_SHUTDOWN_DELAY_MILLIS;
      CONSUMER_RELEASE_TIME.put(pulsarConsumer, releaseTime);

      LOGGER.info("Marking consumer with id {} for release at {}", pulsarConsumer, releaseTime);

      // Schedule the shutdown of the consumer
      new Thread() {
        @Override
        public void run() {
          try {
            // Await the shutdown time
            Uninterruptibles.sleepUninterruptibly(CONSUMER_SHUTDOWN_DELAY_MILLIS, TimeUnit.MILLISECONDS);

            // Shutdown all consumers that have not been re-acquired
            synchronized (PulsarStreamLevelConsumerManager.class) {
              LOGGER.info("Executing release check for consumer {} at {}, scheduled at {}", pulsarConsumer,
                  System.currentTimeMillis(), releaseTime);

              Iterator<Map.Entry<ImmutableTriple<String, String, String>, Reader<byte[]>>> configIterator =
                  CONSUMER_FOR_CONFIG_KEY.entrySet().iterator();

              while (configIterator.hasNext()) {
                Map.Entry<ImmutableTriple<String, String, String>, Reader<byte[]>> entry = configIterator.next();
                Reader<byte[]> pulsarConsumer = entry.getValue();

                final Long releaseTime = CONSUMER_RELEASE_TIME.get(pulsarConsumer);
                if (!releaseTime.equals(IN_USE) && releaseTime < System.currentTimeMillis()) {
                  LOGGER.info("Releasing consumer {}", pulsarConsumer);

                  try {
                    pulsarConsumer.close();
                  } catch (Exception e) {
                    LOGGER.warn("Caught exception while shutting down Pulsar consumer with id {}", pulsarConsumer, e);
                  }

                  configIterator.remove();
                  CONSUMER_RELEASE_TIME.remove(pulsarConsumer);
                } else {
                  LOGGER.info("Not releasing consumer {}, it has been reacquired", pulsarConsumer);
                }
              }
            }
          } catch (Exception e) {
            LOGGER.warn("Caught exception in release of consumer {}", pulsarConsumer, e);
          }
        }
      }.start();
    }
  }

  public static void closeAllConsumers() {
    try {
      // Shutdown all consumers
      synchronized (PulsarStreamLevelConsumerManager.class) {
        LOGGER.info("Trying to shutdown all the pulsar consumers");
        Iterator<Reader<byte[]>> consumerIterator = CONSUMER_FOR_CONFIG_KEY.values().iterator();

        while (consumerIterator.hasNext()) {
          Reader<byte[]> pulsarConsumer = consumerIterator.next();
          LOGGER.info("Trying to shutdown consumer {}", pulsarConsumer);
          try {
            pulsarConsumer.close();
          } catch (Exception e) {
            LOGGER.warn("Caught exception while shutting down Pulsar consumer with id {}", pulsarConsumer, e);
          }
          consumerIterator.remove();
        }
        CONSUMER_FOR_CONFIG_KEY.clear();
        CONSUMER_RELEASE_TIME.clear();
      }
    } catch (Exception e) {
      LOGGER.warn("Caught exception during shutting down all pulsar consumers", e);
    }
  }
}
