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


public interface DataProvider {
  Map<MetricSlice, DataFrame> fetchTimeseries(Collection<MetricSlice> slices);

  Map<MetricSlice, DataFrame> fetchAggregates(Collection<MetricSlice> slices, List<String> dimensions);

  Multimap<AnomalySlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<AnomalySlice> slices);

  Multimap<EventSlice, EventDTO> fetchEvents(Collection<EventSlice> slices);

  Map<Long, MetricConfigDTO> fetchMetrics(Collection<Long> ids);

  DetectionPipeline loadPipeline(DetectionConfigDTO config, long start, long end) throws Exception;
}
