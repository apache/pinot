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
package org.apache.pinot.spi.queryeventlistener;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Optional;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_BROKER_EVENT_LISTENER_CLASS_NAME;
import static org.apache.pinot.spi.utils.CommonConstants.DEFAULT_BROKER_EVENT_LISTENER_CLASS_NAME;


public class PinotBrokerQueryEventListenerUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(PinotBrokerQueryEventListenerUtils.class);
    private static BrokerQueryEventListener _brokerQueryEventListener = null;

    private PinotBrokerQueryEventListenerUtils() {
    }

    /**
     * Initialize the BrokerQueryEventListener and registers the eventListener
     */
    @VisibleForTesting
    public synchronized static void init(PinotConfiguration eventListenerConfiguration) {
        // Initializes BrokerQueryEventListener.
        initializeBrokerQueryEventListener(eventListenerConfiguration);
    }

    /**
     * Initializes PinotBrokerQueryEventListener with event-listener configurations.
     * @param eventListenerConfiguration The subset of the configuration containing the event-listener-related keys
     */
    private static void initializeBrokerQueryEventListener(PinotConfiguration eventListenerConfiguration) {
        String brokerQueryEventListenerClassName = eventListenerConfiguration.getProperty(
                CONFIG_OF_BROKER_EVENT_LISTENER_CLASS_NAME, DEFAULT_BROKER_EVENT_LISTENER_CLASS_NAME);
        LOGGER.info("{} will be initialized as the PinotBrokerQueryEventListener",
                brokerQueryEventListenerClassName);

        Optional<Class<?>> clazzFound;
        try {
            clazzFound = Optional.of(Class.forName(brokerQueryEventListenerClassName));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to initialize BrokerQueryEventListener. "
                    + "Please check if any pinot-event-listener related jar is actually added to the classpath.");
        }

        clazzFound.ifPresent(clazz -> {
                    try {
                        BrokerQueryEventListener brokerQueryEventListener =
                                (BrokerQueryEventListener) clazz.newInstance();
                        registerBrokerEventListener(brokerQueryEventListener);
                    } catch (Exception e) {
                        LOGGER.error("Caught exception while initializing event listener registry: {}, skipping it",
                                clazz, e);
                    }
                }
        );

        Preconditions.checkState(_brokerQueryEventListener != null,
                "Failed to initialize BrokerQueryEventListener. "
                        + "Please check if any pinot-event-listener related jar is actually added to the classpath.");
    }

    /**
     * Registers an broker event listener.
     */
    private static void registerBrokerEventListener(
            BrokerQueryEventListener brokerQueryEventListener) {
        LOGGER.info("Registering broker event listener : {}",
                brokerQueryEventListener.getClass().getName());
        _brokerQueryEventListener = brokerQueryEventListener;
    }

    /**
     * Returns the brokerQueryEventListener. If the BrokerQueryEventListener is null,
     * first creates and initializes the BrokerQueryEventListener.
     * @param eventListenerConfiguration event-listener configs
     */
    public static synchronized BrokerQueryEventListener getBrokerQueryEventListener(
            PinotConfiguration eventListenerConfiguration) {
        if (_brokerQueryEventListener == null) {
            init(eventListenerConfiguration);
        }
        return _brokerQueryEventListener;
    }

    @VisibleForTesting
    public static BrokerQueryEventListener getBrokerQueryEventListener() {
        return getBrokerQueryEventListener(new PinotConfiguration(Collections.emptyMap()));
    }
}
