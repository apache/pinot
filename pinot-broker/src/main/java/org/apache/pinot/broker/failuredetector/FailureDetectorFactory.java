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
package org.apache.pinot.broker.failuredetector;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailureDetectorFactory {
  private FailureDetectorFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetectorFactory.class);
  private static final FailureDetector NO_OP_FAILURE_DETECTOR = new NoOpFailureDetector();

  public static FailureDetector getFailureDetector(PinotConfiguration config, BrokerMetrics brokerMetrics) {
    String typeStr = config.getProperty(Broker.FailureDetector.CONFIG_OF_TYPE, Broker.FailureDetector.DEFAULT_TYPE);
    Broker.FailureDetector.Type type;
    try {
      type = Broker.FailureDetector.Type.valueOf(typeStr.toUpperCase());
    } catch (Exception e) {
      throw new IllegalArgumentException("Illegal failure detector type: " + typeStr);
    }
    switch (type) {
      case NO_OP:
        return NO_OP_FAILURE_DETECTOR;
      case CONNECTION: {
        FailureDetector failureDetector = new ConnectionFailureDetector();
        failureDetector.init(config, brokerMetrics);
        return failureDetector;
      }
      case CUSTOM: {
        String className = config.getProperty(Broker.FailureDetector.CONFIG_OF_CLASS_NAME);
        Preconditions.checkArgument(!StringUtils.isEmpty(className),
            "Failure detector class name must be configured for CUSTOM type");
        LOGGER.info("Initializing CUSTOM failure detector with class: {}", className);
        try {
          FailureDetector failureDetector = PluginManager.get().createInstance(className);
          failureDetector.init(config, brokerMetrics);
          LOGGER.info("Initialized CUSTOM failure detector with class: {}", className);
          return failureDetector;
        } catch (Exception e) {
          throw new RuntimeException(
              "Caught exception while initializing CUSTOM failure detector with class: " + className, e);
        }
      }
      default:
        throw new IllegalStateException("Unsupported failure detector type: " + type);
    }
  }
}
