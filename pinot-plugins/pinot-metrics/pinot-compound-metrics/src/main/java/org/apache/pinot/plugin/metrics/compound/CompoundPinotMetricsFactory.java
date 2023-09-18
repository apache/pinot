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
package org.apache.pinot.plugin.metrics.compound;

import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.pinot.spi.annotations.metrics.MetricsFactory;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotGauge;
import org.apache.pinot.spi.metrics.PinotJmxReporter;
import org.apache.pinot.spi.metrics.PinotMetricName;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a special {@link PinotMetricsRegistry} that actually reports metrics in one or more registries. For example,
 * it can be used to register metrics using both Yammer and Dropwizard. The way this class works is quite naive.
 * When it is created, a bunch of factories are used as sub-factories. Whenever a metric is registered in this factory,
 * it actually registers the metric in all the sub-factories.
 *
 * Probably the main reason to use this metrics is to compare the differences between one metric registry and another.
 * For example, Yammer and Dropwizard provide their own timer, but each one provides their own metrics on their timers.
 * Most metrics are the same (p50, p90, p95, etc) but some other may be different.
 *
 * Alternative it could be used in production, but it is important to make sure that the JMX MBeans produced by each
 * sub-registry are different. Otherwise the reported value is undetermined.
 *
 * In order to use this factory, you have to set the following properties in Pinot configuration:
 * <ol>
 *   <li>pinot.&lt;server|broker|minion|etc&gt;.metrics.factory.className should be set to
 *   org.apache.pinot.plugin.metrics.compound.CompoundPinotMetricsFactory</li>
 *   <li>(optional) pinot.&lt;server|broker|minion|etc&gt;.metrics.compound.algorithm should be set to SERVICE_LOADER,
 *   CLASSPATH or LIST. CLASSPATH is the default.</li>
 *   <li>(optional) pinot.&lt;server|broker|minion|etc&gt;.metrics.compound.ignored can be used to ignore specific
 *   metric registries</li>
 * </ol>
 */
@AutoService(PinotMetricsFactory.class)
@MetricsFactory
public class CompoundPinotMetricsFactory implements PinotMetricsFactory {
  public static final Logger LOGGER = LoggerFactory.getLogger(CompoundPinotMetricsFactory.class);
  /**
   * The suffix property used to define the algorithm used to look for other registries. It will be prefixed with the
   * corresponding property prefix (like pinot.server.plugin.metrics, pinot.broker.plugin.metrics, etc).
   *
   * See {@link Algorithm}
   */
  public static final String ALGORITHM_KEY = "compound.algorithm";
  /**
   * The suffix property used to define a list of metric registries to ignore. It will be prefixed with the
   * corresponding property prefix (like pinot.server.plugin.metrics, pinot.broker.plugin.metrics, etc).
   *
   * The list of metrics factory classes we want to ignore. They have to be actual names that can be converted into
   * classes by using {@link Class#forName(String)}. Any {@link PinotMetricsRegistry} that is implements or extends any
   * of the factories included here will be ignored by this metric registry.
   */
  public static final String IGNORED_METRICS = "compound.ignored";
  /**
   * The suffix property used to define a list of metric registries to include when using {@link Algorithm#LIST}.
   * It will be prefixed with the corresponding property prefix (like pinot.server.plugin.metrics,
   * pinot.broker.plugin.metrics, etc).
   *
   * Each value should be the name of a class that implements {@link PinotMetricsFactory} and can be instantiated with
   * the {@link PluginManager}.
   */
  public static final String LIST_KEY = "compound.list";
  private List<PinotMetricsFactory> _factories;
  private CompoundPinotMetricRegistry _registry;

  @Override
  public void init(PinotConfiguration metricsConfiguration) {
    String algorithmName = metricsConfiguration.getProperty(ALGORITHM_KEY, Algorithm.CLASSPATH.name());

    Set<Class<?>> allIgnored = metricsConfiguration.getProperty(IGNORED_METRICS, Collections.emptyList()).stream()
        .flatMap(ignoredClassName -> {
          try {
            return Stream.of(Class.forName(ignoredClassName));
          } catch (ClassNotFoundException ex) {
            LOGGER.warn("Ignored metric factory {} cannot be found", ignoredClassName);
            return Stream.empty();
          }
        })
        .collect(Collectors.toSet());

    Algorithm algorithm = Algorithm.valueOf(algorithmName.toUpperCase(Locale.US));
    _factories = algorithm.streamInstances(metricsConfiguration)
        .filter(factory -> allIgnored.stream().noneMatch(ignored -> ignored.isAssignableFrom(factory.getClass())))
        .filter(factory -> CompoundPinotMetricsFactory.class.isAssignableFrom(factory.getClass()))
        .collect(Collectors.toList());

    if (_factories.isEmpty()) {
      throw new IllegalStateException("There is no metric factory to be used");
    }
    for (PinotMetricsFactory factory : _factories) {
      factory.init(metricsConfiguration);
    }
  }

  @Override
  public PinotMetricsRegistry getPinotMetricsRegistry() {
    if (_registry == null) {
      List<PinotMetricsRegistry> allRegistries =
          _factories.stream().map(PinotMetricsFactory::getPinotMetricsRegistry).collect(Collectors.toList());
      _registry = new CompoundPinotMetricRegistry(allRegistries);
    }
    return _registry;
  }

  @Override
  public PinotMetricName makePinotMetricName(Class<?> klass, String name) {
    List<PinotMetricName> names = _factories.stream()
        .map(factory -> factory.makePinotMetricName(klass, name))
        .collect(Collectors.toList());
    return new CompoundPinotMetricName(name, names);
  }

  @Override
  public <T> PinotGauge<T> makePinotGauge(Function<Void, T> condition) {
    List<PinotGauge<T>> gauges = _factories.stream()
        .map(factory -> factory.makePinotGauge(condition))
        .collect(Collectors.toList());
    return new CompoundPinotGauge<T>(gauges);
  }

  @Override
  public PinotJmxReporter makePinotJmxReporter(PinotMetricsRegistry metricsRegistry) {
    CompoundPinotMetricRegistry registry = (CompoundPinotMetricRegistry) metricsRegistry;
    List<PinotMetricsRegistry> subRegistries = registry.getRegistries();
    Preconditions.checkState(subRegistries.size() == _factories.size(),
        "Number of registries ({}) should be the same than the number of factories ({})",
        subRegistries.size(), _factories.size());

    ArrayList<PinotJmxReporter> subJmx = new ArrayList<>(_factories.size());
    for (int i = 0; i < _factories.size(); i++) {
      PinotMetricsFactory subFactory = _factories.get(i);
      PinotMetricsRegistry subRegistry = subRegistries.get(i);

      subJmx.add(subFactory.makePinotJmxReporter(subRegistry));
    }

    return new CompoundPinotJmxReporter(subJmx);
  }

  @Override
  public String getMetricsFactoryName() {
    return "Compound";
  }

  /**
   * How to look for other {@link PinotMetricsFactory}.
   */
  enum Algorithm {
    /**
     * An algorithm that returns all {@link PinotMetricsFactory} defined as {@link ServiceLoader}.
     */
    SERVICE_LOADER {
      @Override
      protected Stream<PinotMetricsFactory> streamInstances(PinotConfiguration metricsConfiguration) {
        ArrayList<PinotMetricsFactory> result = new ArrayList<>();
        for (PinotMetricsFactory factory : ServiceLoader.load(PinotMetricsFactory.class)) {
          result.add(factory);
        }
        return result.stream();
      }
    },
    /**
     * An algorithm that returns all factories returned by {@link PinotMetricUtils#getPinotMetricsFactoryClasses()}.
     */
    CLASSPATH {
      @Override
      protected Stream<PinotMetricsFactory> streamInstances(PinotConfiguration metricsConfiguration) {
        return PinotMetricUtils.getPinotMetricsFactoryClasses().stream()
            .map(clazz -> {
                  try {
                    return (PinotMetricsFactory) clazz.getDeclaredConstructor().newInstance();
                  } catch (Exception ex) {
                    throw new IllegalArgumentException("Cannot instantiate class " + clazz, ex);
                  }
                }
            );
      }
    },
    /**
     * An algorithm returns all the factories listed in the config under the {@link #LIST_KEY}.
     */
    LIST {
      @Override
      protected Stream<PinotMetricsFactory> streamInstances(PinotConfiguration metricsConfiguration) {
        return metricsConfiguration.getProperty(LIST_KEY, Collections.emptyList()).stream()
            .map(className -> {
                  try {
                    return PluginManager.get().createInstance(className);
                  } catch (ClassNotFoundException ex) {
                    throw new IllegalArgumentException("Cannot find metric factory named " + className, ex);
                  } catch (Exception ex) {
                    throw new IllegalArgumentException("Cannot instantiate class " + className, ex);
                  }
                }
            );
      }
    };

    abstract protected Stream<PinotMetricsFactory> streamInstances(PinotConfiguration metricsConfiguration);
  }
}
