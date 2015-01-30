package com.linkedin.pinot.common.metrics;

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


/**
 * Broker metrics utility class, which provides facilities to log the execution performance of queries.
 *
 * @author jfim
 */
public class BrokerMetrics {
  private final MetricsRegistry _metricsRegistry;
  private static final String PINOT_BROKER_PREFIX = "pinot.broker.";

  public static enum BrokerQueryPhase {
    REQUEST_COMPILATION,
    QUERY_EXECUTION,
    QUERY_ROUTING,
    SCATTER_GATHER,
    DESERIALIZATION,
    REDUCE;

    private final String queryPhaseName;

    BrokerQueryPhase() {
      queryPhaseName = Utils.toCamelCase(name().toLowerCase());
    }

    public String getQueryPhaseName() {
      return queryPhaseName;
    }
  }

  public static enum BrokerMeter {
    UNCAUGHT_GET_EXCEPTIONS("exceptions", true),
    UNCAUGHT_POST_EXCEPTIONS("exceptions", true),
    QUERIES("queries", false),
    REQUEST_COMPILATION_EXCEPTIONS("exceptions", true),
    REQUEST_FETCH_EXCEPTIONS("exceptions", false),
    REQUEST_DESERIALIZATION_EXCEPTIONS("exceptions", false),
    DOCUMENTS_SCANNED("documents", false);

    private final String brokerMeterName;
    private final String unit;
    private final boolean global;

    BrokerMeter(String unit, boolean global) {
      this.unit = unit;
      this.global = global;
      this.brokerMeterName = Utils.toCamelCase(name().toLowerCase());
    }

    public String getBrokerMeterName() {
      return brokerMeterName;
    }

    public String getUnit() {
      return unit;
    }

    /**
     * Returns true if the metric is global (not attached to a particular table or resource)
     *
     * @return true if the metric is global
     */
    public boolean isGlobal() {
      return global;
    }
  }

  /**
   * Constructs the broker metrics.
   *
   * @param metricsRegistry The metric registry used to register timers and meters.
   */
  public BrokerMetrics(MetricsRegistry metricsRegistry) {
    _metricsRegistry = metricsRegistry;
  }

  /**
   * Logs the timing of a query phase.
   *
   * @param request The broker request associated with this query
   * @param phase The query phase for which to log time
   * @param nanos The number of nanoseconds that the phase execution took to complete
   */
  public void addPhaseTiming(final BrokerRequest request, final BrokerQueryPhase phase, final long nanos) {
    final String fullTimerName = PINOT_BROKER_PREFIX + request.getQuerySource().getResourceName() + "." +
        request.getQuerySource().getTableName() + "." + phase.getQueryPhaseName();
    final MetricName metricName = new MetricName(BrokerMetrics.class, fullTimerName);

    MetricsHelper.newTimer(_metricsRegistry, metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
      .update(nanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Logs the time taken to complete the given callable.
   *
   * @param request The broker request associated with this query
   * @param phase The query phase
   * @param callable The callable to execute
   * @param <T> The return type of the callable
   * @return The return value of the callable passed as a parameter
   * @throws Exception The exception thrown by the callable
   */
  public<T> T timePhase(final BrokerRequest request, final BrokerQueryPhase phase, final Callable<T> callable) throws Exception {
    long startTime = System.nanoTime();
    T returnValue = callable.call();
    long totalNanos = System.nanoTime() - startTime;

    addPhaseTiming(request, phase, totalNanos);

    return returnValue;
  }

  /**
   * Logs a value to a meter.
   *
   * @param request The broker request associated with this query or null if this meter applies globally
   * @param brokerMeter The broker meter to use
   * @param unitCount The number of units to add to the meter
   */
  public void addMeteredValue(final BrokerRequest request, final BrokerMeter brokerMeter, final long unitCount) {
    final String fullMeterName;
    if (request != null) {
      fullMeterName = PINOT_BROKER_PREFIX + request.getQuerySource().getResourceName() + "." +
          request.getQuerySource().getTableName() + "." + brokerMeter.getBrokerMeterName();
    } else {
      fullMeterName = PINOT_BROKER_PREFIX + brokerMeter.getBrokerMeterName();
    }
    final MetricName metricName = new MetricName(BrokerMetrics.class, fullMeterName);

    MetricsHelper.newMeter(_metricsRegistry, metricName, brokerMeter.getUnit(), TimeUnit.SECONDS).mark(unitCount);
  }

  /**
   * Initializes all global meters (such as exceptions count) to zero.
   */
  public void initializeGlobalMeters() {
    BrokerMeter[] meters = BrokerMeter.values();

    for (BrokerMeter meter : meters) {
      if (meter.isGlobal()) {
        addMeteredValue(null, meter, 0);
      }
    }
  }
}
