package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.AggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.DefaultAggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.DefaultTimeSeriesLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.TimeSeriesLoader;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import java.util.Collections;
import java.util.List;


public class DetectionPipelineTaskRunner implements TaskRunner {
  private final DetectionPipelineLoader loader =
      new DetectionPipelineLoader();

  private final DetectionConfigManager detectionDAO =
      DAORegistry.getInstance().getDetectionConfigManager();

  private final MetricConfigManager metricDAO =
      DAORegistry.getInstance().getMetricConfigDAO();

  private final DatasetConfigManager datasetDAO =
      DAORegistry.getInstance().getDatasetConfigDAO();

  private final MergedAnomalyResultManager anomalyDAO =
      DAORegistry.getInstance().getMergedAnomalyResultDAO();

  private final EventManager eventDAO =
      DAORegistry.getInstance().getEventDAO();

  private final TimeSeriesLoader timeseriesLoader =
      new DefaultTimeSeriesLoader(this.metricDAO, this.datasetDAO,
          ThirdEyeCacheRegistry.getInstance().getQueryCache());

  private final AggregationLoader aggregationLoader =
      new DefaultAggregationLoader(this.metricDAO, this.datasetDAO,
          ThirdEyeCacheRegistry.getInstance().getQueryCache(),
          ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

  private final DataProvider provider = new DefaultDataProvider(
      this.metricDAO, this.eventDAO, this.anomalyDAO, this.timeseriesLoader, this.aggregationLoader, this.loader);

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    DetectionPipelineTaskInfo info = (DetectionPipelineTaskInfo) taskInfo;

    DetectionConfigDTO config = this.detectionDAO.findById(info.configId);
    if (config == null) {
      throw new IllegalArgumentException(String.format("Could not resolve config id %d", info.configId));
    }

    DetectionPipeline pipeline = this.loader.from(this.provider, config, info.start, info.end);
    DetectionPipelineResult result = pipeline.run();

    config.setLastTimestamp(result.getLastTimestamp());
    detectionDAO.update(config);

    // TODO process results

    return Collections.emptyList();
  }
}
