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
import java.util.Set;
import org.apache.pinot.spi.annotations.queryeventlistener.BrokerEventListenerFactory;
import org.apache.pinot.spi.annotations.queryeventlistener.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.*;

public class PinotBrokerQueryEventListenerUtils {
    private PinotBrokerQueryEventListenerUtils() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(PinotBrokerQueryEventListenerUtils.class);
    private static final String EVENT_LISTENER_PACKAGE_REGEX_PATTERN = ".*\\.plugin\\.query.event.listener\\..*";
    private static BrokerQueryEventListenerFactory _brokerQueryEventListenerFactory = null;

    /**
     * Initialize the metricsFactory ad registers the metricsRegistry
     */
    @VisibleForTesting
    public synchronized static void init(PinotConfiguration eventListenerConfiguration) {
        // Initializes PinotQueryEventListenerFactory.
        initializeBrokerQueryEventListenerFactory(eventListenerConfiguration);
    }

    /**
     * Initializes PinotQueryEventListenerFactory with metrics configurations.
     * @param eventListenerConfiguration The subset of the configuration containing the event-listener-related keys
     */
    private static void initializeBrokerQueryEventListenerFactory(PinotConfiguration eventListenerConfiguration) {
        Set<Class<?>> classes = getPinotBrokerQueryEventListenerFactoryClasses();
        if (classes.size() > 1) {
            LOGGER.warn("More than one BrokerQueryEventListenerFactory was found: {}", classes);
        }

        String brokerQueryEventFactoryClassName = eventListenerConfiguration.getProperty(
                CONFIG_OF_EVENT_LISTENER_FACTORY_CLASS_NAME, DEFAULT_EVENT_LISTENER_FACTORY_CLASS_NAME);
        LOGGER.info("{} will be initialized as the PinotBrokerQueryEventListenerFactory",
                brokerQueryEventFactoryClassName);

        Optional<Class<?>> clazzFound = classes.stream().filter(c -> c.getName()
                        .equals(brokerQueryEventFactoryClassName)).findFirst();

        clazzFound.ifPresent(clazz -> {
                    BrokerEventListenerFactory annotation = clazz.getAnnotation(BrokerEventListenerFactory.class);
                    LOGGER.info("Trying to init PinotBrokerQueryEventListenerFactory: {} "
                            + "and BrokerQueryEventListenerFactory: {}", clazz, annotation);
                    if (annotation.enabled()) {
                        try {
                            BrokerQueryEventListenerFactory brokerQueryEventListenerFactory =
                                    (BrokerQueryEventListenerFactory) clazz.newInstance();
                            brokerQueryEventListenerFactory.init(eventListenerConfiguration);
                            registerBrokerEventListenerFactory(brokerQueryEventListenerFactory);
                        } catch (Exception e) {
                            LOGGER.error("Caught exception while initializing event listener registry: {}, skipping it",
                                    clazz, e);
                        }
                    }
                }
        );

        Preconditions.checkState(_brokerQueryEventListenerFactory != null,
                "Failed to initialize BrokerQueryEventListenerFactory. "
                        + "Please check if any pinot-event-listener related jar is actually added to the classpath.");
    }

    /**
     * Registers an broker event listener factory.
     */
    private static void registerBrokerEventListenerFactory(
            BrokerQueryEventListenerFactory brokerQueryEventListenerFactory) {
        LOGGER.info("Registering broker event listener factory: {}",
                brokerQueryEventListenerFactory.getEventListenerFactoryName());
        _brokerQueryEventListenerFactory = brokerQueryEventListenerFactory;
    }

    /**
     * Returns the brokerQueryEventListener from the initialised BrokerQueryEventListenerFactory.
     * If the BrokerQueryEventListenerFactory is null, first creates and initializes the
     * BrokerQueryEventListenerFactory and registers the BrokerQueryEventListener.
     * @param eventListenerConfiguration event-listener configs
     */
    public static synchronized BrokerQueryEventListener getBrokerQueryEventListener(
            PinotConfiguration eventListenerConfiguration) {
        if (_brokerQueryEventListenerFactory == null) {
            init(eventListenerConfiguration);
        }
        return _brokerQueryEventListenerFactory.getBrokerQueryEventListener();
    }

    @VisibleForTesting
    public static BrokerQueryEventListener getBrokerQueryEventListener() {
        return getBrokerQueryEventListener(new PinotConfiguration(Collections.emptyMap()));
    }

    private static Set<Class<?>> getPinotBrokerQueryEventListenerFactoryClasses() {
        return PinotReflectionUtils.getClassesThroughReflection(EVENT_LISTENER_PACKAGE_REGEX_PATTERN,
                BrokerEventListenerFactory.class);
    }
}
