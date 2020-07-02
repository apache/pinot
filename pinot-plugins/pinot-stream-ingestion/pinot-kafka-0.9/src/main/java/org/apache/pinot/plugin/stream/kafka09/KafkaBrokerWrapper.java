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

import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a work around for kafka 8 and 9.
 *
 * In order to consume data from kafka using simple consumer, we need to know the leader kafka
 * broker for each kafka partition from {@link kafka.javaapi.PartitionMetadata}. But
 * {@link kafka.javaapi.PartitionMetadata} will give two different type of objects in kafka8 and kafka9.
 *
 * In kafka8, it will give {@link kafka.cluster.Broker}.
 * In kafka9, it will give {@link kafka.cluster.BrokerEndPoint}.
 *
 * In Pinot, we need to get host and port from this leader broker object, so this wrapper is made
 * to treat {@link kafka.cluster.Broker} and {@link kafka.cluster.BrokerEndPoint} as object, and
 * underlying use java reflection to provide the method access to host() method for
 * {@link kafka.cluster.Broker#host()} and {@link kafka.cluster.BrokerEndPoint#host()},
 * as long as port method: {@link kafka.cluster.Broker#port()} and {@link kafka.cluster.BrokerEndPoint#port()}.
 *
 */
public class KafkaBrokerWrapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaBrokerWrapper.class);

  private final Object _kafkaBroker;

  public KafkaBrokerWrapper(Object kafkaBroker) {
    this._kafkaBroker = kafkaBroker;
  }

  public String host() {
    if (_kafkaBroker != null) {
      try {
        Method method = _kafkaBroker.getClass().getMethod("host", new Class<?>[0]);
        return (String) method.invoke(_kafkaBroker);
      } catch (Exception e) {
        LOGGER.error("Failed to get host() method from KafkaBroker Object: {}", _kafkaBroker, e);
      }
    }
    return "";
  }

  public int port() {
    if (_kafkaBroker != null) {
      try {
        Method method = _kafkaBroker.getClass().getMethod("port", new Class<?>[0]);
        return (int) method.invoke(_kafkaBroker);
      } catch (Exception e) {
        LOGGER.error("Failed to get port() method from KafkaBroker Object: {}", _kafkaBroker, e);
      }
    }
    return 0;
  }

  @Override
  public String toString() {
    return host() + ":" + port();
  }
}
