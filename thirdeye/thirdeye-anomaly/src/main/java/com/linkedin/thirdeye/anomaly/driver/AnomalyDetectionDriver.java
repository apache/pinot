package com.linkedin.thirdeye.anomaly.driver;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.api.AnomalyDetectionDriverConfig;
import com.linkedin.thirdeye.anomaly.util.ThirdEyeRequestUtils;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeRequest;

/**
 * This class implements the exploration driver.
 */
public class AnomalyDetectionDriver implements Callable<List<DimensionKeySeries>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyDetectionDriver.class);

  private static final TimeGranularity ONE_HOURS = new TimeGranularity(1, TimeUnit.HOURS);

  private final AnomalyDetectionDriverConfig driverConfig;
  private final StarTreeConfig starTreeConfig;
  private final TimeRange timeRange;
  private final ThirdEyeClient thirdEyeClient;

  private final String dimensionKeyContributionMetric;

  /**
   * @param driverConfig
   * @param starTreeConfig
   * @param timeRange
   *  The time range that want to detect anomalies on.
   * @param thirdEyeClient
   */
  public AnomalyDetectionDriver(
      AnomalyDetectionDriverConfig driverConfig,
      StarTreeConfig starTreeConfig,
      TimeRange timeRange,
      ThirdEyeClient thirdEyeClient) {
    /*
     * Use the metric specified in the driverConfig
     */
    if (driverConfig.getContributionEstimateMetric() != null) {
      dimensionKeyContributionMetric = driverConfig.getContributionEstimateMetric();
    } else {
      /*
       * Use the first metric in the collection if no metric is specified.
       */
      LOGGER.warn("no metric provided for thresholding collection {}", starTreeConfig.getCollection());
      dimensionKeyContributionMetric = starTreeConfig.getMetrics().get(0).getName();
    }

    this.driverConfig = driverConfig;
    this.starTreeConfig = starTreeConfig;
    this.timeRange = timeRange;
    this.thirdEyeClient = thirdEyeClient;
  }

  /**
   * {@inheritDoc}
   * @see java.util.concurrent.Callable#call()
   */
  @Override
  public List<DimensionKeySeries> call() throws Exception {
    List<DimensionKeySeries> dimensionKeySeries = new LinkedList<DimensionKeySeries>();

    // get the total series
    Entry<DimensionKey, MetricTimeSeries> totalSeriesEntry = getTotalSeries();
    double totalSeriesSum = sumOverContributionMetric(totalSeriesEntry.getValue());
    dimensionKeySeries.add(new DimensionKeySeries(totalSeriesEntry.getKey(), 1.0));

    explore(new HashMap<String, String>(), 0, totalSeriesSum, dimensionKeySeries);

    // sort in descending order of contribution
    Collections.sort(dimensionKeySeries, new Comparator<DimensionKeySeries>() {
      @Override
      public int compare(DimensionKeySeries d1, DimensionKeySeries d2) {
        double diff = d1.getContributionEstimate() - d2.getContributionEstimate();
        if (diff == 0.0) {
          return 0; // this is a rather pointless check
        } else {
          return (diff > 0) ? -1 : 1;
        }
      }
    });

    return dimensionKeySeries;
  }

  /**
   * @return
   *  The time series for the  all *s dimension combination.
   * @throws Exception
   */
  private Entry<DimensionKey, MetricTimeSeries> getTotalSeries() throws Exception {
    ThirdEyeRequest request = ThirdEyeRequestUtils.buildRequest(
        starTreeConfig.getCollection(),
        ThirdEyeRequestUtils.NULL_GROUP_BY,
        new HashMap<String, String>(),
        Collections.singletonList(dimensionKeyContributionMetric),
        ONE_HOURS,
        timeRange);
    Map<DimensionKey, MetricTimeSeries> dataset = thirdEyeClient.execute(request);
    if (dataset.size() != 1) {
      throw new IllegalStateException();
    }
    return dataset.entrySet().iterator().next();
  }

  /**
   * @param series
   * @return
   *  The sum over the contribution estimate metric
   */
  private double sumOverContributionMetric(MetricTimeSeries series) {
    Number[] sums = series.getMetricSums();
    return sums[series.getSchema().getOffset(dimensionKeyContributionMetric)].doubleValue();
  }

  /**
   * Recursively explore to find out which dimension combinations pass the minimum threshold. By nature of the
   * precedence there will be no duplicates. This will be most efficient from a networking perspective if precedence of
   * dimensions is ordered by descending cardinality.
   *
   * @param dimensionValues
   *  Previously fixed dimension values.
   * @param groupByDimensionIndex
   *  The index in the dimensionPrecedence list to start expanding at.
   * @param totalSeriesSum
   *  The denominator of the contributionEstimate.
   * @param dimensionKeySeries
   *  The List in which to append results.
   * @throws Exception
   */
  private void explore(Map<String, String> dimensionValues, int groupByDimensionIndex,
      double totalSeriesSum, List<DimensionKeySeries> dimensionKeySeries) throws Exception {
    // base case: early stopping
    if (dimensionValues.size() >= driverConfig.getMaxExplorationDepth()) {
      // not efficient, but cleanest since it only needs to be checked in one place
      return;
    }

    // base case: groupByDimensionIndex == driverConfig.getDimensionPrecedence().size()
    for (int i = groupByDimensionIndex; i < driverConfig.getDimensionPrecedence().size(); i++) {
      String groupByDimension = driverConfig.getDimensionPrecedence().get(i);
      ThirdEyeRequest request = ThirdEyeRequestUtils.buildRequest(
          starTreeConfig.getCollection(),
          groupByDimension,
          dimensionValues,
          Collections.singletonList(dimensionKeyContributionMetric),
          ONE_HOURS,
          timeRange);
      Map<DimensionKey, MetricTimeSeries> dataset = thirdEyeClient.execute(request);

      for (Entry<DimensionKey, MetricTimeSeries> entry : dataset.entrySet()) {
        double estimateProportion = sumOverContributionMetric(entry.getValue()) / totalSeriesSum;

        // doesn't meet threshold
        if (estimateProportion < driverConfig.getContributionMinProportion()) {
          continue;
        }

        DimensionKey dimensionKey = entry.getKey();
        dimensionKeySeries.add(new DimensionKeySeries(dimensionKey, estimateProportion));

        dimensionValues.put(groupByDimension, dimensionKey.getDimensionValue(
            starTreeConfig.getDimensions(), groupByDimension));
        explore(dimensionValues, i + 1, totalSeriesSum, dimensionKeySeries);
        dimensionValues.remove(groupByDimension);
      }
    }
  }
}
