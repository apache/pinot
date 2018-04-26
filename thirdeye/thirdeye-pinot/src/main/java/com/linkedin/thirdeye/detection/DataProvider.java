package com.linkedin.thirdeye.detection;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Centralized data source for anomaly detection algorithms. All data used by any
 * algorithm <b>MUST</b> be obtained through this interface to maintain loose coupling.
 *
 * <br/><b>NOTE:</b> extend this interface in case necessary data cannot be obtained
 * through one of the existing methods.
 */
public interface DataProvider {
  /**
   * Returns a map of granular timeseries (keyed by slice) for a given set of slices.
   * The format of the DataFrame follows the standard convention of DataFrameUtils.
   *
   * @see MetricSlice
   * @see com.linkedin.thirdeye.dataframe.util.DataFrameUtils
   *
   * @param slices metric slices
   * @return map of timeseries (keyed by slice)
   */
  Map<MetricSlice, DataFrame> fetchTimeseries(Collection<MetricSlice> slices);

  /**
   * Returns a map of aggregation values (keyed by slice) for a given set of slices,
   * grouped by the given dimensions.
   * The format of the DataFrame follows the standard convention of DataFrameUtils.
   *
   * @see MetricSlice
   * @see com.linkedin.thirdeye.dataframe.util.DataFrameUtils
   *
   * @param slices metric slices
   * @param dimensions dimensions to group by
   * @return map of aggregation values (keyed by slice)
   */
  Map<MetricSlice, DataFrame> fetchAggregates(Collection<MetricSlice> slices, List<String> dimensions);

  /**
   * Returns a multimap of anomalies (keyed by slice) for a given set of slices.
   *
   * @see MergedAnomalyResultDTO
   * @see AnomalySlice
   *
   * @param slices anomaly slice
   * @return multimap of anomalies (keyed by slice)
   */
  Multimap<AnomalySlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<AnomalySlice> slices);

  /**
   * Returns a multimap of events (keyed by slice) for a given set of slices.
   *
   * @see EventDTO
   * @see EventSlice
   *
   * @param slices event slice
   * @return multimap of events (keyed by slice)
   */
  Multimap<EventSlice, EventDTO> fetchEvents(Collection<EventSlice> slices);

  /**
   * Returns a map of metric configs (keyed by id) for a given set of ids.
   *
   * @see MetricConfigDTO
   *
   * @param ids metric config ids
   * @return map of metric configs (keyed by id)
   */
  Map<Long, MetricConfigDTO> fetchMetrics(Collection<Long> ids);

  /**
   * Returns an initialized instance of a detection pipeline for the given config. Injects this
   * DataProvider as provider for the new pipeline.
   *
   * <br/><b>NOTE:</b> this method is typically used for prototyping and creating nested pipelines
   *
   * @param config detection config
   * @param start detection window start time
   * @param end detection window end time
   * @return detection pipeline instance
   * @throws Exception
   */
  DetectionPipeline loadPipeline(DetectionConfigDTO config, long start, long end) throws Exception;
}
