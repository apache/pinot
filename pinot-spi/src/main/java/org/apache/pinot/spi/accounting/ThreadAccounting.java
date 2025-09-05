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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Global registration for thread accounting implementations.
 */
public class ThreadAccounting {
  private ThreadAccounting() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(ThreadAccounting.class);

  /// This is the registration point for broker side [ThreadResourceUsageAccountant] implementations. Only one
  /// accountant can be registered to avoid the overhead of polymorphic calls, and it must be registered before the
  /// first call to [#getBrokerAccountant()]. Keep separate registry for broker and server accountant to allow different
  /// implementations to be registered when both broker and server are in the same JVM.
  private static final AtomicReference<ThreadResourceUsageAccountant> BROKER_ACCOUNTANT_REGISTRY =
      new AtomicReference<>();

  /// User once registration point to allow customization of broker side thread accounting behaviour. Registration will
  /// be successful if this was the first attempt to register and registration happened before first call to
  /// [#getBrokerAccountant()].
  @VisibleForTesting
  public static boolean registerBrokerAccountant(ThreadResourceUsageAccountant accountant) {
    return BROKER_ACCOUNTANT_REGISTRY.compareAndSet(null, accountant);
  }

  private static final class BrokerAccountantHolder {
    static final ThreadResourceUsageAccountant ACCOUNTANT;

    static {
      ThreadResourceUsageAccountant accountant = BROKER_ACCOUNTANT_REGISTRY.get();
      ACCOUNTANT = accountant != null ? accountant : createDefaultThreadAccountant();
    }
  }

  /// Returns the registered broker side [ThreadResourceUsageAccountant].
  public static ThreadResourceUsageAccountant getBrokerAccountant() {
    return BrokerAccountantHolder.ACCOUNTANT;
  }

  /// This is the registration point for server side [ThreadResourceUsageAccountant] implementations. Only one
  /// accountant can be registered to avoid the overhead of polymorphic calls, and it must be registered before the
  /// first call to [#getServerAccountant()]. Keep separate registry for broker and server accountant to allow different
  /// implementations to be registered when both broker and server are in the same JVM.
  private static final AtomicReference<ThreadResourceUsageAccountant> SERVER_ACCOUNTANT_REGISTRY =
      new AtomicReference<>();

  /// User once registration point to allow customization of server side thread accounting behaviour. Registration will
  /// be successful if this was the first attempt to register and registration happened before first call to
  /// [#getServerAccountant()].
  @VisibleForTesting
  public static boolean registerServerAccountant(ThreadResourceUsageAccountant accountant) {
    return SERVER_ACCOUNTANT_REGISTRY.compareAndSet(null, accountant);
  }

  private static final class ServerAccountantHolder {
    static final ThreadResourceUsageAccountant ACCOUNTANT;

    static {
      ThreadResourceUsageAccountant accountant = SERVER_ACCOUNTANT_REGISTRY.get();
      ACCOUNTANT = accountant != null ? accountant : createDefaultThreadAccountant();
    }
  }

  /// Returns the registered server side [ThreadResourceUsageAccountant].
  public static ThreadResourceUsageAccountant getServerAccountant() {
    return ServerAccountantHolder.ACCOUNTANT;
  }

  /**
   * Create default thread accountant for query preemption hardening. Use when {@see register} not called and
   * {@see initializeThreadAccountant} not loading any class
   * @return default thread accountant that only tracks the corresponding runner thread of each worker thread
   */
  private static DefaultThreadResourceUsageAccountant createDefaultThreadAccountant() {
    LOGGER.info("Using no-op thread accountant");
    return DefaultThreadResourceUsageAccountant.INSTANCE;
  }

  /// Initializes the thread accountant. Should be called only once during instance startup.
  public static void init(PinotConfiguration config, String instanceId, InstanceType instanceType) {
    ThreadResourceUsageAccountant threadAccountant = createThreadAccountant(config, instanceId, instanceType);
    String threadAccountantClassName = threadAccountant.getClass().getName();
    switch (instanceType) {
      case BROKER:
        if (registerBrokerAccountant(threadAccountant)) {
          LOGGER.info("Registered broker ThreadResourceUsageAccountant: {}", threadAccountantClassName);
        } else {
          LOGGER.error(
              "Failed to register broker ThreadResourceUsageAccountant: {}, as another one is already registered: {}",
              threadAccountantClassName, getBrokerAccountant().getClass().getName());
        }
        break;
      case SERVER:
        if (registerServerAccountant(threadAccountant)) {
          LOGGER.info("Registered server ThreadResourceUsageAccountant: {}", threadAccountantClassName);
        } else {
          LOGGER.error(
              "Failed to register server ThreadResourceUsageAccountant: {}, as another one is already registered: {}",
              threadAccountantClassName, getServerAccountant().getClass().getName());
        }
        break;
      default:
        throw new IllegalStateException("Unsupported instance type: " + instanceType);
    }
  }

  public static ThreadResourceUsageAccountant createThreadAccountant(PinotConfiguration config, String instanceId,
      InstanceType instanceType) {
    String factoryName = config.getProperty(Accounting.CONFIG_OF_FACTORY_NAME);
    if (factoryName != null) {
      LOGGER.info("Initializing ThreadResourceUsageAccountant with factory: {}", factoryName);
      try {
        ThreadAccountantFactory threadAccountantFactory =
            (ThreadAccountantFactory) Class.forName(factoryName).getDeclaredConstructor().newInstance();
        return threadAccountantFactory.init(config, instanceId, instanceType);
      } catch (Throwable t) {
        LOGGER.error("Caught exception while initializing ThreadResourceUsageAccountant with factory: {}, "
            + "falling back to no-op accountant", factoryName, t);
      }
    }
    return createDefaultThreadAccountant();
  }

  /// Default no-op implementation of [ThreadResourceUsageAccountant].
  public static class DefaultThreadResourceUsageAccountant implements ThreadResourceUsageAccountant {
    public static final DefaultThreadResourceUsageAccountant INSTANCE = new DefaultThreadResourceUsageAccountant();

    @Override
    public void setupTask(QueryThreadContext threadContext) {
    }

    @Override
    public void clear() {
    }

    @Override
    public void sampleUsage() {
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
