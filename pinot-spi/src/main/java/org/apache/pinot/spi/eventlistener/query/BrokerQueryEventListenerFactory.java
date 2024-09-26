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
package org.apache.pinot.spi.eventlistener.query;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_BROKER_EVENT_LISTENER_CLASS_NAME;
import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_REQUEST_CONTEXT_TRACKED_HEADER_KEYS;
import static org.apache.pinot.spi.utils.CommonConstants.DEFAULT_BROKER_EVENT_LISTENER_CLASS_NAME;


public class BrokerQueryEventListenerFactory {
  private BrokerQueryEventListenerFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerQueryEventListenerFactory.class);
  private static BrokerQueryEventListener _brokerQueryEventListener;
  private static Set<String> _trackedHeaders;

  /**
   * Initializes BrokerQueryEventListener and tracked headers.
   */
  public static void init(PinotConfiguration config) {
    // Initialize BrokerQueryEventListener
    initializeBrokerQueryEventListener(config);
    // Initialize tracked headers
    initializeTrackedHeaders(config);
  }

  /**
   * Initializes BrokerQueryEventListener with event-listener configurations.
   * @param config The subset of the configuration containing the event-listener-related keys
   */
  private static void initializeBrokerQueryEventListener(PinotConfiguration config) {
    String brokerQueryEventListenerClassName =
        config.getProperty(CONFIG_OF_BROKER_EVENT_LISTENER_CLASS_NAME, DEFAULT_BROKER_EVENT_LISTENER_CLASS_NAME);
    LOGGER.info("Initializing BrokerQueryEventListener with class name: {}", brokerQueryEventListenerClassName);
    try {
      _brokerQueryEventListener =
          (BrokerQueryEventListener) Class.forName(brokerQueryEventListenerClassName).getDeclaredConstructor()
              .newInstance();
      _brokerQueryEventListener.init(config);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    LOGGER.info("Initialized BrokerQueryEventListener: {}", _brokerQueryEventListener.getClass().getName());
  }

  /**
   * Initializes tracked request-headers to extract from query request.
   * @param config The subset of the configuration containing the event-listener-related keys
   */
  private static void initializeTrackedHeaders(PinotConfiguration config) {
    String trackedHeaders = config.getProperty(CONFIG_OF_REQUEST_CONTEXT_TRACKED_HEADER_KEYS);
    if (StringUtils.isNotEmpty(trackedHeaders)) {
      LOGGER.info("Initializing tracked headers with config: {}", trackedHeaders);
      String[] split = StringUtils.split(trackedHeaders, ',');
      _trackedHeaders = Sets.newHashSetWithExpectedSize(split.length);
      for (String header : split) {
        _trackedHeaders.add(header.trim().toLowerCase());
      }
      LOGGER.info("Initialized tracked headers: {}", _trackedHeaders);
    } else {
      LOGGER.info("No tracked headers configured");
      _trackedHeaders = Set.of();
    }
  }

  /**
   * Returns the BrokerQueryEventListener instance.
   */
  public static BrokerQueryEventListener getBrokerQueryEventListener() {
    return Preconditions.checkNotNull(_brokerQueryEventListener,
        "BrokerQueryEventListenerFactory has not been initialized");
  }

  /**
   * Returns the set of tracked headers.
   */
  public static Set<String> getTrackedHeaders() {
    return Preconditions.checkNotNull(_trackedHeaders, "BrokerQueryEventListenerFactory has not been initialized");
  }
}
