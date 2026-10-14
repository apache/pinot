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
package org.apache.pinot.plugin.metrics.dropwizard;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.apache.pinot.spi.metrics.PinotJmxReporter;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Exposes Dropwizard metrics using Pinot's legacy JMX names and units. Lifecycle and metric callbacks synchronize
/// on the owning registry so registration, removal, and shutdown observe the same metric metadata.
public class DropwizardJmxReporter implements PinotJmxReporter, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DropwizardJmxReporter.class);

  private final DropwizardMetricsRegistry _registry;
  private final MetricRegistry _metricRegistry;
  private final MBeanServer _mBeanServer = ManagementFactory.getPlatformMBeanServer();
  private final String _domainName;
  private final Map<String, ObjectName> _registeredNames = new HashMap<>();
  private boolean _started;
  private boolean _closed;

  private final MetricRegistryListener _listener = new MetricRegistryListener.Base() {
    @Override
    public void onGaugeAdded(String name, Gauge<?> metric) {
      register(name, metric, "gauges");
    }

    @Override
    public void onCounterAdded(String name, Counter metric) {
      register(name, metric, "counters");
    }

    @Override
    public void onHistogramAdded(String name, Histogram metric) {
      register(name, metric, "histograms");
    }

    @Override
    public void onMeterAdded(String name, Meter metric) {
      register(name, metric, "meters");
    }

    @Override
    public void onTimerAdded(String name, Timer metric) {
      register(name, metric, "timers");
    }

    @Override
    public void onGaugeRemoved(String name) {
      unregister(name);
    }

    @Override
    public void onCounterRemoved(String name) {
      unregister(name);
    }

    @Override
    public void onHistogramRemoved(String name) {
      unregister(name);
    }

    @Override
    public void onMeterRemoved(String name) {
      unregister(name);
    }

    @Override
    public void onTimerRemoved(String name) {
      unregister(name);
    }
  };

  public DropwizardJmxReporter(PinotMetricsRegistry metricsRegistry, String domainName) {
    _registry = (DropwizardMetricsRegistry) metricsRegistry;
    _metricRegistry = (MetricRegistry) metricsRegistry.getMetricsRegistry();
    _domainName = domainName;
  }

  @Override
  public void start() {
    synchronized (_registry) {
      if (_started || _closed || !_registry.registerReporter(this)) {
        return;
      }
      _started = true;
      _metricRegistry.addListener(_listener);
    }
  }

  private void register(String name, Metric metric, String category) {
    synchronized (_registry) {
      if (!_started || _closed || _registeredNames.containsKey(name)
          || _metricRegistry.getMetrics().get(name) != metric) {
        return;
      }
      DropwizardMetricsRegistry.MetricMetadata metadata = _registry.getMetricMetadata(name);
      String group = metadata != null ? metadata._name.getGroup() : DropwizardMetricsFactory.DEFAULT_DOMAIN_VALUE;
      String type = metadata != null ? metadata._name.getType() : null;
      String mBeanName = ObjectName.quote(_domainName != null ? _domainName : group) + ":type="
          + ObjectName.quote(type != null ? type : category);
      if (!name.isEmpty()) {
        mBeanName += ",name=" + ObjectName.quote(name);
      }
      try {
        ObjectName objectName = new ObjectName(mBeanName);
        _mBeanServer.registerMBean(new MetricBean(metric, objectName, metadata), objectName);
        _registeredNames.put(name, objectName);
      } catch (InstanceAlreadyExistsException e) {
        LOGGER.debug("Metric MBean is already registered: {}", mBeanName);
      } catch (JMException e) {
        LOGGER.warn("Failed to register metric MBean: {}", mBeanName, e);
      }
    }
  }

  private void unregister(String name) {
    synchronized (_registry) {
      ObjectName objectName = _registeredNames.remove(name);
      if (objectName == null) {
        return;
      }
      try {
        _mBeanServer.unregisterMBean(objectName);
      } catch (InstanceNotFoundException e) {
        LOGGER.debug("Metric MBean is already unregistered: {}", objectName);
      } catch (JMException e) {
        LOGGER.warn("Failed to unregister metric MBean: {}", objectName, e);
      }
    }
  }

  @Override
  public void close() {
    synchronized (_registry) {
      if (_closed) {
        return;
      }
      _closed = true;
      if (_started) {
        _metricRegistry.removeListener(_listener);
      }
      for (String name : _registeredNames.keySet().toArray(String[]::new)) {
        unregister(name);
      }
      _registry.unregisterReporter(this);
    }
  }

  /// Read-only legacy JMX view of a native Dropwizard metric. Metadata is immutable; native metrics provide
  /// thread-safe reads while reporters serialize registration and removal on the registry monitor.
  private static final class MetricBean implements DynamicMBean {
    private final Metric _metric;
    private final ObjectName _objectName;
    private final String _eventType;
    private final TimeUnit _rateUnit;
    private final TimeUnit _durationUnit;
    private final Map<String, String> _attributes = new LinkedHashMap<>();
    private final MBeanInfo _mBeanInfo;

    MetricBean(Metric metric, ObjectName objectName, DropwizardMetricsRegistry.MetricMetadata metadata) {
      _metric = metric;
      _objectName = objectName;
      _eventType = metric instanceof Timer ? "calls" : metadata != null ? metadata._eventType : "events";
      _rateUnit = metadata != null && metadata._rateUnit != null ? metadata._rateUnit : TimeUnit.SECONDS;
      _durationUnit = metadata != null && metadata._durationUnit != null
          ? metadata._durationUnit
          : TimeUnit.MILLISECONDS;
      if (metric instanceof Gauge) {
        _attributes.put("Value", Object.class.getName());
      } else {
        _attributes.put("Count", long.class.getName());
      }
      if (metric instanceof Metered) {
        _attributes.put("EventType", String.class.getName());
        _attributes.put("RateUnit", TimeUnit.class.getName());
        for (String attribute : new String[]{"MeanRate", "OneMinuteRate", "FiveMinuteRate", "FifteenMinuteRate"}) {
          _attributes.put(attribute, double.class.getName());
        }
      }
      if (metric instanceof Timer) {
        _attributes.put("LatencyUnit", TimeUnit.class.getName());
      }
      boolean sampled = metric instanceof Timer || metric instanceof Histogram;
      if (sampled) {
        for (String attribute : new String[]{"Min", "Max", "Mean", "StdDev", "50thPercentile", "75thPercentile",
            "95thPercentile", "98thPercentile", "99thPercentile", "999thPercentile"}) {
          _attributes.put(attribute, double.class.getName());
        }
      }
      MBeanAttributeInfo[] attributes = _attributes.entrySet().stream()
          .map(entry -> new MBeanAttributeInfo(entry.getKey(), entry.getValue(), entry.getKey(), true, false, false))
          .toArray(MBeanAttributeInfo[]::new);
      MBeanOperationInfo objectNameOperation = new MBeanOperationInfo("objectName", "Metric ObjectName",
          new MBeanParameterInfo[0], ObjectName.class.getName(), MBeanOperationInfo.INFO);
      MBeanOperationInfo[] operations = sampled ? new MBeanOperationInfo[]{objectNameOperation,
          new MBeanOperationInfo("values", "Sampled values", new MBeanParameterInfo[0], double[].class.getName(),
              MBeanOperationInfo.INFO)} : new MBeanOperationInfo[]{objectNameOperation};
      _mBeanInfo = new MBeanInfo(getClass().getName(), "Pinot metric", attributes, null, operations, null);
    }

    @Override
    public Object getAttribute(String attribute) throws AttributeNotFoundException {
      if (!_attributes.containsKey(attribute)) {
        throw new AttributeNotFoundException(attribute);
      }
      return switch (attribute) {
        case "Value" -> ((Gauge<?>) _metric).getValue();
        case "Count" -> ((Counting) _metric).getCount();
        case "EventType" -> _eventType;
        case "RateUnit" -> _rateUnit;
        case "LatencyUnit" -> _durationUnit;
        case "MeanRate" -> convertRate(((Metered) _metric).getMeanRate());
        case "OneMinuteRate" -> convertRate(((Metered) _metric).getOneMinuteRate());
        case "FiveMinuteRate" -> convertRate(((Metered) _metric).getFiveMinuteRate());
        case "FifteenMinuteRate" -> convertRate(((Metered) _metric).getFifteenMinuteRate());
        default -> getSampleAttribute(attribute);
      };
    }

    private double convertRate(double rate) {
      return rate * ((double) _rateUnit.toNanos(1) / TimeUnit.SECONDS.toNanos(1));
    }

    private Snapshot getSnapshot() {
      return _metric instanceof Timer ? ((Timer) _metric).getSnapshot() : ((Histogram) _metric).getSnapshot();
    }

    private double getSampleAttribute(String attribute) throws AttributeNotFoundException {
      Snapshot snapshot = getSnapshot();
      double value = switch (attribute) {
        case "Min" -> snapshot.getMin();
        case "Max" -> snapshot.getMax();
        case "Mean" -> snapshot.getMean();
        case "StdDev" -> snapshot.getStdDev();
        case "50thPercentile" -> snapshot.getMedian();
        case "75thPercentile" -> snapshot.get75thPercentile();
        case "95thPercentile" -> snapshot.get95thPercentile();
        case "98thPercentile" -> snapshot.get98thPercentile();
        case "99thPercentile" -> snapshot.get99thPercentile();
        case "999thPercentile" -> snapshot.get999thPercentile();
        default -> throw new AttributeNotFoundException(attribute);
      };
      return _metric instanceof Timer ? value / _durationUnit.toNanos(1) : value;
    }

    @Override
    public AttributeList getAttributes(String[] attributes) {
      AttributeList result = new AttributeList();
      for (String attribute : attributes) {
        try {
          result.add(new Attribute(attribute, getAttribute(attribute)));
        } catch (AttributeNotFoundException | RuntimeException e) {
          LOGGER.debug("Failed to read metric attribute: {}", attribute, e);
        }
      }
      return result;
    }

    @Override
    public void setAttribute(Attribute attribute) throws AttributeNotFoundException {
      throw new AttributeNotFoundException("Metric attributes are read-only");
    }

    @Override
    public AttributeList setAttributes(AttributeList attributes) {
      return new AttributeList();
    }

    @Override
    public Object invoke(String actionName, Object[] params, String[] signature) throws ReflectionException {
      if ((params == null || params.length == 0) && (signature == null || signature.length == 0)) {
        if ("objectName".equals(actionName)) {
          return _objectName;
        }
        if ("values".equals(actionName) && (_metric instanceof Timer || _metric instanceof Histogram)) {
          long[] values = getSnapshot().getValues();
          double factor = _metric instanceof Timer ? 1.0 / _durationUnit.toNanos(1) : 1.0;
          double[] result = new double[values.length];
          for (int i = 0; i < values.length; i++) {
            result[i] = values[i] * factor;
          }
          return result;
        }
      }
      throw new ReflectionException(new NoSuchMethodException(actionName));
    }

    @Override
    public MBeanInfo getMBeanInfo() {
      return _mBeanInfo;
    }
  }
}
