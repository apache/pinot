package com.linkedin.thirdeye.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.AggregationLoader;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregate;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTimeZone;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * The Current and baseline loader.
 */
public class CurrentAndBaselineLoader {
  private static final long TIMEOUT = 60000;

  private MetricConfigManager metricDAO;
  private DatasetConfigManager datasetDAO;
  private AggregationLoader aggregationLoader;

  /**
   * Instantiates a new Current and baseline loader.
   *
   * @param metricDAO the metric dao
   * @param datasetDAO the dataset dao
   * @param aggregationLoader the aggregation loader
   */
  public CurrentAndBaselineLoader(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO,
      AggregationLoader aggregationLoader) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.aggregationLoader = aggregationLoader;
  }

  /**
   * Fill in current and baseline value in the anomalies.
   *
   * @param anomalies the anomalies
   * @throws Exception the exception
   */
  public void fillInCurrentAndBaselineValue(Collection<MergedAnomalyResultDTO> anomalies) throws Exception {
    ExecutorService executor = Executors.newCachedThreadPool();

    for (final MergedAnomalyResultDTO anomaly : anomalies) {
      if (anomaly.getAvgBaselineVal() == 0 || anomaly.getAvgCurrentVal() == 0) {
        MetricConfigDTO metricConfigDTO =
            this.metricDAO.findByMetricAndDataset(anomaly.getMetric(), anomaly.getCollection());
        if (metricConfigDTO == null) {
          throw new IllegalArgumentException(
              String.format("Could not resolve metric '%s' and dataset '%s'", anomaly.getMetric(),
                  anomaly.getCollection()));
        }

        DatasetConfigDTO datasetConfigDTO = this.datasetDAO.findByDataset(anomaly.getCollection());
        if (datasetConfigDTO == null) {
          throw new IllegalArgumentException(String.format("Could not dataset '%s'", anomaly.getCollection()));
        }

        Multimap<String, String> filters = getFiltersFromDimensionMaps(anomaly);

        final MetricSlice slice = MetricSlice.from(metricConfigDTO.getId(), anomaly.getStartTime(), anomaly.getEndTime(), filters);

        DateTimeZone timezone = getDateTimeZone(datasetConfigDTO);
        final Baseline baseline = BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.SUM, 1, 1, timezone);

        executor.submit(new Runnable() {
          @Override
          public void run() {
            anomaly.setAvgCurrentVal(getAggregate(slice));
            anomaly.setAvgBaselineVal(getAggregate(baseline.scatter(slice).get(0)));
          }
        });
      }
    }

    executor.shutdown();
    executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
  }

  private DateTimeZone getDateTimeZone(DatasetConfigDTO datasetConfigDTO) {
    try {
      if (StringUtils.isBlank(datasetConfigDTO.getTimezone())) {
        return DateTimeZone.forID(datasetConfigDTO.getTimezone());
      }
    } catch (Exception ignore) {
      // ignored
    }
    return DateTimeZone.UTC;
  }

  private double getAggregate(MetricSlice slice) {
    try {
      return this.aggregationLoader.loadAggregate(slice, Collections.<String>emptyList()).getDouble(COL_VALUE, 0);
    } catch (Exception e) {
      return Double.NaN;
    }
  }

  private Multimap<String, String> getFiltersFromDimensionMaps(MergedAnomalyResultDTO anomaly) {
    Multimap<String, String> filters = ArrayListMultimap.create();
    for (Map.Entry<String, String> entry : anomaly.getDimensions().entrySet()) {
      filters.put(entry.getKey(), entry.getValue());
    }
    return filters;
  }
}
