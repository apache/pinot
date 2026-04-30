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
package org.apache.pinot.spi.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.pinot.spi.annotations.metrics.MetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.CONFIG_OF_METRICS_FACTORY_CLASS_NAME;
import static org.apache.pinot.spi.utils.CommonConstants.DEFAULT_METRICS_FACTORY_CLASS_NAME;


public class PinotMetricUtils {
  private PinotMetricUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotMetricUtils.class);
  private static final Map<PinotMetricsRegistry, Boolean> METRICS_REGISTRY_MAP = new ConcurrentHashMap<>();
  private static final Map<MetricsRegistryRegistrationListener, Boolean> METRICS_REGISTRY_REGISTRATION_LISTENERS_MAP =
      new ConcurrentHashMap<>();

  private static final PinotMetricsFactory NOOP_FACTORY = new PinotMetricsFactory.Noop();
  private static PinotMetricsFactory _pinotMetricsFactory = NOOP_FACTORY;

  /**
   * Initialize the metricsFactory ad registers the metricsRegistry
   */
  @VisibleForTesting
  public synchronized static void init(PinotConfiguration metricsConfiguration) {
    // Initializes PinotMetricsFactory.
    initializePinotMetricsFactory(metricsConfiguration);

    // Initializes metrics using the metrics configuration.
    initializeMetrics(metricsConfiguration);
    registerMetricsRegistry(getPinotMetricsRegistry());
  }

  /**
   * Initializes PinotMetricsFactory with metrics configurations.
   *
   * <p>Implementations are discovered via {@link PluginManager#loadServices(Class)} (which
   * walks every plugin classloader, including isolated plugin realms) instead of a system-classpath
   * reflection scan. The selected implementation must still be marked
   * {@link MetricsFactory#enabled() @MetricsFactory(enabled = true)} on its source class.</p>
   *
   * @param metricsConfiguration The subset of the configuration containing the metrics-related keys
   */
  private static void initializePinotMetricsFactory(PinotConfiguration metricsConfiguration) {
    List<PinotMetricsFactory> candidates = PluginManager.get().loadServices(PinotMetricsFactory.class);
    if (candidates.size() > 1) {
      LOGGER.warn("More than one PinotMetricsFactory was found: {}",
          candidates.stream().map(c -> c.getClass().getName()).collect(Collectors.toList()));
    }

    String metricsFactoryClassName = metricsConfiguration.getProperty(CONFIG_OF_METRICS_FACTORY_CLASS_NAME,
        DEFAULT_METRICS_FACTORY_CLASS_NAME);
    LOGGER.info("{} will be initialized as the PinotMetricsFactory", metricsFactoryClassName);

    Optional<PinotMetricsFactory> matched = candidates.stream()
        .filter(c -> c.getClass().getName().equals(metricsFactoryClassName))
        .findFirst();

    matched.ifPresent(instance -> {
          Class<?> clazz = instance.getClass();
          MetricsFactory annotation = clazz.getAnnotation(MetricsFactory.class);
          LOGGER.info("Trying to init PinotMetricsFactory: {} and MetricsFactory: {}", clazz, annotation);
          if (annotation == null) {
            LOGGER.warn("PinotMetricsFactory {} has no @MetricsFactory annotation; treating as disabled", clazz);
            return;
          }
          if (annotation.enabled()) {
            try {
              instance.init(metricsConfiguration);
              registerMetricsFactory(instance);
            } catch (Exception e) {
              LOGGER.error("Caught exception while initializing pinot metrics registry: {}, skipping it", clazz, e);
            }
          }
        }
    );

    Preconditions.checkState(_pinotMetricsFactory != NOOP_FACTORY,
        "Failed to initialize PinotMetricsFactory. Please check if any pinot-metrics related jar is actually added to"
            + " the classpath.");
  }

  /**
   * Returns the set of {@link PinotMetricsFactory} implementations discovered via
   * {@link PluginManager#loadServices(Class)}. After plugin classes move into isolated realms,
   * this set remains discoverable through the realm walk; before then, it matches the
   * `META-INF/services/org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory` files
   * generated by {@code @AutoService} on each implementation.
   */
  public static Set<Class<?>> getPinotMetricsFactoryClasses() {
    return PluginManager.get().loadServices(PinotMetricsFactory.class).stream()
        .map(PinotMetricsFactory::getClass)
        .collect(Collectors.toSet());
  }

  /**
   * Initializes the metrics system by initializing the registry registration listeners present in the configuration.
   *
   * @param configuration The subset of the configuration containing the metrics-related keys
   */
  private static void initializeMetrics(PinotConfiguration configuration) {
    synchronized (PinotMetricUtils.class) {
      List<String> listenerClassNames = configuration.getProperty("metricsRegistryRegistrationListeners",
          Arrays.asList(JmxReporterMetricsRegistryRegistrationListener.class.getName()));

      // Build each listener using their default constructor and add them. PluginManager.loadClass
      // is realm-aware (consults plugin realms in addition to the system classloader), and we keep
      // the explicit getDeclaredConstructor() pattern to tolerate non-public no-arg constructors.
      for (String listenerClassName : listenerClassNames) {
        try {
          Class<? extends MetricsRegistryRegistrationListener> clazz =
              (Class<? extends MetricsRegistryRegistrationListener>) PluginManager.get().loadClass(listenerClassName);
          Constructor<? extends MetricsRegistryRegistrationListener> defaultConstructor =
              clazz.getDeclaredConstructor();
          MetricsRegistryRegistrationListener listener = defaultConstructor.newInstance();

          LOGGER.info("Registering metricsRegistry to listener {}", listenerClassName);
          addMetricsRegistryRegistrationListener(listener);
        } catch (Exception e) {
          LOGGER.warn("Caught exception while initializing MetricsRegistryRegistrationListener {}", listenerClassName,
              e);
        }
      }
    }
    LOGGER.info("Number of listeners got registered: {}", METRICS_REGISTRY_REGISTRATION_LISTENERS_MAP.size());
  }

  /**
   * Adds a metrics registry registration listener. When adding a metrics registry registration listener, events are
   * fired to add all previously registered metrics registries to the newly added metrics registry registration
   * listener.
   *
   * @param listener The listener to add
   */
  private static void addMetricsRegistryRegistrationListener(MetricsRegistryRegistrationListener listener) {
    synchronized (PinotMetricUtils.class) {
      METRICS_REGISTRY_REGISTRATION_LISTENERS_MAP.put(listener, Boolean.TRUE);

      // Fire events to register all previously registered metrics registries
      Set<PinotMetricsRegistry> metricsRegistries = METRICS_REGISTRY_MAP.keySet();
      LOGGER.info("Number of metrics registry: {}", metricsRegistries.size());
      for (PinotMetricsRegistry metricsRegistry : metricsRegistries) {
        listener.onMetricsRegistryRegistered(metricsRegistry);
      }
    }
  }

  /**
   * Registers the metrics registry with the metrics helper.
   *
   * @param registry The registry to register
   */
  private static void registerMetricsRegistry(PinotMetricsRegistry registry) {
    synchronized (PinotMetricUtils.class) {
      METRICS_REGISTRY_MAP.put(registry, Boolean.TRUE);

      // Fire event to all registered listeners
      Set<MetricsRegistryRegistrationListener> metricsRegistryRegistrationListeners =
          METRICS_REGISTRY_REGISTRATION_LISTENERS_MAP.keySet();
      for (MetricsRegistryRegistrationListener metricsRegistryRegistrationListener
          : metricsRegistryRegistrationListeners) {
        metricsRegistryRegistrationListener.onMetricsRegistryRegistered(registry);
      }
    }
  }

  /**
   * Registers an metrics factory.
   */
  private static void registerMetricsFactory(PinotMetricsFactory metricsFactory) {
    LOGGER.info("Registering metrics factory: {}", metricsFactory.getMetricsFactoryName());
    _pinotMetricsFactory = metricsFactory;
  }

  @VisibleForTesting
  public static PinotMetricsRegistry getPinotMetricsRegistry() {
    return getPinotMetricsRegistry(new PinotConfiguration(Collections.emptyMap()));
  }

  /**
   * Cleans up previous emitted metrics
   */
  @VisibleForTesting
  public static void cleanUp() {
    if (_pinotMetricsFactory == null) {
      _pinotMetricsFactory = NOOP_FACTORY;
    } else if (_pinotMetricsFactory != NOOP_FACTORY) {
      _pinotMetricsFactory.getPinotMetricsRegistry().shutdown();
      _pinotMetricsFactory = NOOP_FACTORY;
    }
  }

  /**
   * Returns the metricsRegistry from the initialised metricsFactory.
   * If the metricsFactory is null, first creates and initializes the metricsFactory and registers the metrics registry.
   * @param metricsConfiguration metrics configs
   */
  public static synchronized PinotMetricsRegistry getPinotMetricsRegistry(PinotConfiguration metricsConfiguration) {
    if (_pinotMetricsFactory == null || _pinotMetricsFactory == NOOP_FACTORY) {
      init(metricsConfiguration);
    }
    return _pinotMetricsFactory.getPinotMetricsRegistry();
  }

  public static PinotMetricName makePinotMetricName(Class<?> klass, String name) {
    return _pinotMetricsFactory.makePinotMetricName(klass, name);
  }

  public static <T> PinotGauge<T> makePinotGauge(Function<Void, T> condition) {
    return _pinotMetricsFactory.makePinotGauge(condition);
  }

  public static <T> PinotGauge<T> makeGauge(PinotMetricsRegistry registry, PinotMetricName name, PinotGauge<T> gauge) {
    return registry.newGauge(name, gauge);
  }

  public static PinotTimer makePinotTimer(PinotMetricsRegistry registry, PinotMetricName name, TimeUnit durationUnit,
      TimeUnit rateUnit) {
    return registry.newTimer(name, durationUnit, rateUnit);
  }

  public static PinotMeter makePinotMeter(PinotMetricsRegistry registry, PinotMetricName name, String eventType,
      TimeUnit unit) {
    return registry.newMeter(name, eventType, unit);
  }

  public static void removeMetric(PinotMetricsRegistry registry, PinotMetricName name) {
    registry.removeMetric(name);
  }

  public static PinotJmxReporter makePinotJmxReporter(PinotMetricsRegistry metricsRegistry) {
    return _pinotMetricsFactory.makePinotJmxReporter(metricsRegistry);
  }
}
