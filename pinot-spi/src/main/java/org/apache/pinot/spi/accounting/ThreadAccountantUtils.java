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
package org.apache.pinot.spi.accounting;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThreadAccountantUtils {
  private ThreadAccountantUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadAccountantUtils.class);

  /// Creates a thread accountant based on the factory class name in the config. If no factory class name is specified,
  /// or if any exception is thrown while loading the factory class, returns a no-op accountant.
  public static ThreadAccountant createAccountant(PinotConfiguration config, String instanceId,
      InstanceType instanceType) {
    String factoryName = config.getProperty(Accounting.CONFIG_OF_FACTORY_NAME);
    if (factoryName != null) {
      LOGGER.info("Initializing ThreadAccountant with factory: {}", factoryName);
      try {
        ThreadAccountantFactory threadAccountantFactory =
            (ThreadAccountantFactory) Class.forName(factoryName).getDeclaredConstructor().newInstance();
        return threadAccountantFactory.init(config, instanceId, instanceType);
      } catch (Throwable t) {
        LOGGER.error("Caught exception while initializing ThreadAccountant with factory: {}, "
            + "falling back to no-op accountant", factoryName, t);
      }
    }
    LOGGER.info("Using no-op accountant");
    return NoOpAccountant.INSTANCE;
  }

  public static ThreadAccountant getNoOpAccountant() {
    return NoOpAccountant.INSTANCE;
  }

  /// No-op implementation of [ThreadAccountant].
  private static class NoOpAccountant implements ThreadAccountant {
    static final NoOpAccountant INSTANCE = new NoOpAccountant();

    @Override
    public void setupTask(QueryThreadContext threadContext) {
    }

    @Override
    public void sampleUsage() {
    }

    @Override
    public void clear() {
    }

    @Override
    public void updateUntrackedResourceUsage(String queryId, long allocatedBytes, long cpuTimeNs,
        TrackingScope trackingScope) {
    }

    @Override
    public Collection<? extends ThreadResourceTracker> getThreadResources() {
      return List.of();
    }

    @Override
    public Map<String, ? extends QueryResourceTracker> getQueryResources() {
      return Map.of();
    }
  }
}
