package com.linkedin.pinot.common.metrics;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.QuerySource;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


/**
 * Common code for metrics implementations.
 *
 * @author jfim
 */
public abstract class AbstractMetrics<QP extends AbstractMetrics.QueryPhase, M extends AbstractMetrics.Meter> {
  protected final String _metricPrefix;

  protected final MetricsRegistry _metricsRegistry;

  private static final Set<String> registeredTables = new HashSet<String>();

  private final Class _clazz;

  public AbstractMetrics(String metricPrefix, MetricsRegistry metricsRegistry, Class clazz) {
    _metricPrefix = metricPrefix;
    _metricsRegistry = metricsRegistry;
    _clazz = clazz;
  }

  public interface QueryPhase {
    public String getQueryPhaseName();
  }

  public interface Meter {
    public String getMeterName();
    public String getUnit();
    public boolean isGlobal();
  }

  /**
   * Logs the timing of a query phase.
   *
   * @param request The broker request associated with this query
   * @param phase The query phase for which to log time
   * @param nanos The number of nanoseconds that the phase execution took to complete
   */
  public void addPhaseTiming(final BrokerRequest request, final QP phase, final long nanos) {
    final String fullTimerName = buildMetricName(request, phase.getQueryPhaseName());
    final MetricName metricName = new MetricName(_clazz, fullTimerName);

    MetricsHelper.newTimer(_metricsRegistry, metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS)
      .update(nanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Builds a complete metric name, of the form prefix.resource.table.metric
   *
   * @param request The broker request containing all the information
   * @param metricName The metric name to register
   * @return The complete metric name
   */
  private String buildMetricName(BrokerRequest request, String metricName) {
    return _metricPrefix + request.getQuerySource().getResourceName() + "." +
        request.getQuerySource().getTableName() + "." + metricName;
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
  public<T> T timePhase(final BrokerRequest request, final QP phase, final Callable<T> callable) throws Exception {
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
   * @param meter The meter to use
   * @param unitCount The number of units to add to the meter
   */
  public void addMeteredValue(final BrokerRequest request, final M meter, final long unitCount) {
    final String fullMeterName;
    String meterName = meter.getMeterName();
    if (request != null) {
      fullMeterName = buildMetricName(request, meterName);
    } else {
      fullMeterName = _metricPrefix + meterName;
    }
    final MetricName metricName = new MetricName(_clazz, fullMeterName);

    MetricsHelper.newMeter(_metricsRegistry, metricName, meter.getUnit(), TimeUnit.SECONDS).mark(unitCount);
  }

  /**
   * Initializes all global meters (such as exceptions count) to zero.
   */
  public void initializeGlobalMeters() {
    M[] meters = getMeters();

    for (M meter : meters) {
      if (meter.isGlobal()) {
        addMeteredValue(null, meter, 0);
      }
    }
  }

  /**
   * Ensures that all metrics for a table are registered.
   */
  public void ensureTableIsRegistered(final String resourceName, final String tableName) {
    final String completeResourceName = resourceName + "." + tableName;

    synchronized (registeredTables) {
      if (!registeredTables.contains(completeResourceName)) {
        registeredTables.add(completeResourceName);

        BrokerRequest dummyRequest = new BrokerRequest();
        QuerySource dummyQuerySource = new QuerySource();
        dummyRequest.setQuerySource(dummyQuerySource);
        dummyQuerySource.setResourceName(resourceName);
        dummyQuerySource.setTableName(tableName);

        // Register all query phases
        QP[] queryPhases = getQueryPhases();
        for (QP queryPhase : queryPhases) {
          final String fullTimerName = buildMetricName(dummyRequest, queryPhase.getQueryPhaseName());
          final MetricName metricName = new MetricName(_clazz, fullTimerName);

          MetricsHelper.newTimer(_metricsRegistry, metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        }

        // Register all non global meters
        M[] meters = getMeters();
        for (M meter : meters) {
          if(!meter.isGlobal()) {
            addMeteredValue(dummyRequest, meter, 0);
          }
        }
      }
    }
  }

  protected abstract QP[] getQueryPhases();

  protected abstract M[] getMeters();
}
