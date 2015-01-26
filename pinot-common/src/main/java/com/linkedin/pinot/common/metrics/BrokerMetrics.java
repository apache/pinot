package com.linkedin.pinot.common.metrics;

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
   * @param phase The name of the query phase
   * @param nanos The number of nanoseconds that the phase execution took to complete
   */
  public void addPhaseTiming(final BrokerRequest request, final String phase, final long nanos) {
    final String fullTimerName = "pinot.broker." + request.getQuerySource().getResourceName() + "." +
        request.getQuerySource().getTableName() + "." + phase;
    final MetricName metricName = new MetricName(BrokerMetrics.class, fullTimerName);

    MetricsHelper.newTimer(_metricsRegistry, metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
      .update(nanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Logs the time taken to complete the given callable.
   *
   * @param request The broker request associated with this query
   * @param phase The name of the query phase
   * @param callable The callable to execute
   * @param <T> The return type of the callable
   * @return The return value of the callable passed as a parameter
   * @throws Exception The exception thrown by the callable
   */
  public<T> T timePhase(final BrokerRequest request, final String phase, final Callable<T> callable) throws Exception {
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
   * @param meterName The name of the meter to use
   * @param unitName The unit that this meter uses
   * @param unitCount The number of units to add to the meter
   */
  public void addMeteredValue(final BrokerRequest request, final String meterName, final String unitName, final long unitCount) {
    final String fullMeterName;
    if (request != null) {
      fullMeterName = "pinot.broker." + request.getQuerySource().getResourceName() + "." +
          request.getQuerySource().getTableName() + "." + meterName;
    } else {
      fullMeterName = "pinot.broker." + meterName;
    }
    final MetricName metricName = new MetricName(BrokerMetrics.class, fullMeterName);

    MetricsHelper.newMeter(_metricsRegistry, metricName, fullMeterName, TimeUnit.SECONDS).mark(unitCount);
  }
}
