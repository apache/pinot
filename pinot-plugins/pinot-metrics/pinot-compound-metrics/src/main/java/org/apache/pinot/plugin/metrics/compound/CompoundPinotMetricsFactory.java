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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@MetricsFactory
public class CompoundPinotMetricsFactory implements PinotMetricsFactory {
  public static final Logger LOGGER = LoggerFactory.getLogger(CompoundPinotMetricsFactory.class);
  public static final String ALGORITHM_KEY = "pinot.metrics.compound.algorithm";
  public static final String IGNORED_METRICS = "pinot.metrics.compound.ignored";
  public static final String LIST_KEY = "pinot.metrics.compound.list";
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
    List<PinotMetricName> names = _factories.stream().map(factory -> factory.makePinotMetricName(klass, name))
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

  enum Algorithm {
    SERVICE_LOADER {
      @Override
      protected Stream<PinotMetricsFactory> streamInstances(PinotConfiguration metricsConfiguration) {
        return ServiceLoader.load(PinotMetricsFactory.class).stream().map(ServiceLoader.Provider::get);
      }
    },
    CLASSPATH {
      @Override
      protected Stream<PinotMetricsFactory> streamInstances(PinotConfiguration metricsConfiguration) {
        return PinotMetricUtils.getPinotMetricsFactoryClasses().stream()
            .filter(clazz -> !CompoundPinotMetricsFactory.class.isAssignableFrom(clazz))
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
    LIST {
      @Override
      protected Stream<PinotMetricsFactory> streamInstances(PinotConfiguration metricsConfiguration) {
        return metricsConfiguration.getProperty(LIST_KEY, Collections.emptyList()).stream()
            .map(className -> {
                  try {
                    Class<?> c = Class.forName(className);
                    return (PinotMetricsFactory) c.getDeclaredConstructor().newInstance();
                  } catch (ClassNotFoundException ex) {
                    throw new IllegalArgumentException("Cannot find metric factory named " + className);
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
