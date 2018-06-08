package com.linkedin.thirdeye.detection;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.dataframe.BooleanSeries;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * DetectionPipeline forms the root of the detection class hierarchy. It represents a wireframe
 * for implementing (intermittently stateful) executable pipelines on top of it.
 */
public abstract class DetectionPipeline {
  protected final DataProvider provider;
  protected final DetectionConfigDTO config;
  protected final long startTime;
  protected final long endTime;

  protected DetectionPipeline(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    this.provider = provider;
    this.config = config;
    this.startTime = startTime;
    this.endTime = endTime;
  }

  /**
   * Returns a detection result for the time range between {@code startTime} and {@code endTime}.
   *
   * @return detection result
   * @throws Exception
   */
  public abstract DetectionPipelineResult run() throws Exception;

  /**
   * Helper for creating an anomaly for a given metric slice. Injects properties such as
   * metric name, filter dimensions, etc.
   *
   * @param slice metric slice
   * @return anomaly template
   */
  protected final MergedAnomalyResultDTO makeAnomaly(MetricSlice slice) {
    Map<Long, MetricConfigDTO> metrics = this.provider.fetchMetrics(Collections.singleton(slice.getMetricId()));
    if (!metrics.containsKey(slice.getMetricId())) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", slice.getMetricId()));
    }

    MetricConfigDTO metric = metrics.get(slice.getMetricId());

    return makeAnomaly(slice, metric);
  }

  /**
   * Helper for creating an anomaly for a given metric slice. Injects properties such as
   * metric name, filter dimensions, etc.
   *
   * @param slice metric slice
   * @param metric metric config dto related to slice
   * @return anomaly template
   */
  protected final MergedAnomalyResultDTO makeAnomaly(MetricSlice slice, MetricConfigDTO metric) {
    MergedAnomalyResultDTO anomaly = new MergedAnomalyResultDTO();
    anomaly.setStartTime(slice.getStart());
    anomaly.setEndTime(slice.getEnd());
    anomaly.setMetric(metric.getName());
    anomaly.setCollection(metric.getDataset());
    anomaly.setDimensions(toFilterMap(slice.getFilters()));
    anomaly.setDetectionConfigId(this.config.getId());
    anomaly.setChildren(new HashSet<MergedAnomalyResultDTO>());

    return anomaly;
  }

  /**
   * Helper for creating a list of anomalies from a boolean series. Injects properties via
   * {@code makeAnomaly(MetricSlice, MetricConfigDTO)}.
   *
   * @see DetectionPipeline#makeAnomaly(MetricSlice, MetricConfigDTO)
   *
   * @param slice metric slice
   * @param dfTimeseries time series with COL_TIME and at least one value series
   * @param seriesName name of the value series
   * @return list of anomalies
   */
  protected final List<MergedAnomalyResultDTO> makeAnomalies(MetricSlice slice, DataFrame dfTimeseries, String seriesName) {
    BooleanSeries filter = dfTimeseries.getLongs(COL_TIME).gte(this.startTime).and(dfTimeseries.getLongs(COL_TIME).lt(this.endTime));
    DataFrame dfTruncated = dfTimeseries.filter(filter).dropNull(COL_TIME);

    if (dfTruncated.isEmpty()) {
      return Collections.emptyList();
    }

    Map<Long, MetricConfigDTO> metrics = this.provider.fetchMetrics(Collections.singleton(slice.getMetricId()));
    if (!metrics.containsKey(slice.getMetricId())) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", slice.getMetricId()));
    }

    MetricConfigDTO metric = metrics.get(slice.getMetricId());

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    LongSeries sTime = dfTruncated.getLongs(COL_TIME);
    BooleanSeries sVal = dfTruncated.getBooleans(seriesName);

    int lastStart = -1;
    for (int i = 0; i < dfTruncated.size(); i++) {
      if (sVal.isNull(i) || !BooleanSeries.booleanValueOf(sVal.get(i))) {
        // end of a run
        if (lastStart >= 0) {
          long start = sTime.get(lastStart);
          long end = sTime.get(i);
          anomalies.add(makeAnomaly(slice.withStart(start).withEnd(end), metric));
        }
        lastStart = -1;

      } else {
        // start of a run
        if (lastStart < 0) {
          lastStart = i;
        }
      }
    }

    // end of current run
    if (lastStart >= 0) {
      long start = sTime.get(lastStart);
      long end = start + 1;

      // guess-timate of next time series timestamp
      DatasetConfigDTO dataset = this.provider.fetchDatasets(Collections.singleton(metric.getDataset())).get(metric.getDataset());
      if (dataset != null) {
        Period period = dataset.bucketTimeGranularity().toPeriod();
        DateTimeZone timezone = DateTimeZone.forID(dataset.getTimezone());

        long lastTimestamp = sTime.getLong(sTime.size() - 1);

        end = new DateTime(lastTimestamp, timezone).plus(period).getMillis();
      }

      // truncate at analysis end time
      end = Math.min(end, this.endTime);

      anomalies.add(makeAnomaly(slice.withStart(start).withEnd(end), metric));
    }

    return anomalies;
  }

  // TODO anomaly should support multimap
  private DimensionMap toFilterMap(Multimap<String, String> filters) {
    DimensionMap map = new DimensionMap();
    for (Map.Entry<String, String> entry : filters.entries()) {
      map.put(entry.getKey(), entry.getValue());
    }
    return map;
  }

  public DataProvider getProvider() {
    return provider;
  }

  public DetectionConfigDTO getConfig() {
    return config;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getEndTime() {
    return endTime;
  }
}
