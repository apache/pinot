package com.linkedin.thirdeye.detection;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.Collections;
import java.util.Map;


public abstract class StaticDetectionPipeline extends DetectionPipeline {
  protected StaticDetectionPipeline(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);
  }

  public abstract StaticDetectionPipelineModel getModel();

  public abstract DetectionPipelineResult run(StaticDetectionPipelineData data) throws Exception;

  @Override
  public final DetectionPipelineResult run() throws Exception {
    StaticDetectionPipelineModel model = this.getModel();
    Map<MetricSlice, DataFrame> timeseries = this.provider.fetchTimeseries(model.timeseriesSlices);
    Map<MetricSlice, DataFrame> aggregates = this.provider.fetchAggregates(model.aggregateSlices, Collections.<String>emptyList());
    Multimap<AnomalySlice, MergedAnomalyResultDTO> anomalies = this.provider.fetchAnomalies(model.anomalySlices);
    Multimap<EventSlice, EventDTO> events = this.provider.fetchEvents(model.eventSlices);

    StaticDetectionPipelineData data = new StaticDetectionPipelineData(
        model, timeseries, aggregates, anomalies, events);

    return this.run(data);
  }
}
