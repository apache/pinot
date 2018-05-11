package com.linkedin.thirdeye.detection.alert;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.alert.commons.EmailContentFormatterFactory;
import com.linkedin.thirdeye.alert.commons.EmailEntity;
import com.linkedin.thirdeye.alert.content.EmailContentFormatter;
import com.linkedin.thirdeye.alert.content.EmailContentFormatterConfiguration;
import com.linkedin.thirdeye.alert.content.EmailContentFormatterContext;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.util.EmailHelper;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.AggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.DefaultAggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.DefaultTimeSeriesLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.TimeSeriesLoader;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DefaultDataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Detection alert task runner. This runner looks for the new anomalies and run the detection alert filter to get
 * mappings from anomalies to recipients and then send email to the recipients.
 */
public class DetectionAlertTaskRunner implements TaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionAlertTaskRunner.class);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final String DEFAULT_EMAIL_FORMATTER_TYPE = "MultipleAnomaliesEmailContentFormatter";

  private DetectionAlertConfigManager alertConfigDAO;
  private DetectionAlertConfigDTO detectionAlertConfig;
  private final DetectionAlertFilterLoader alertFilterLoader;
  private final DataProvider provider;
  private ThirdEyeAnomalyConfiguration thirdeyeConfig;

  public DetectionAlertTaskRunner() {
    this.alertConfigDAO = DAO_REGISTRY.getDetectionAlertConfigManager();
    this.alertFilterLoader = new DetectionAlertFilterLoader();

    MergedAnomalyResultManager anomalyMergedResultDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    EventManager eventDAO = DAORegistry.getInstance().getEventDAO();
    TimeSeriesLoader timeseriesLoader =
        new DefaultTimeSeriesLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache());

    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.provider =
        new DefaultDataProvider(metricDAO, eventDAO, anomalyMergedResultDAO, timeseriesLoader, aggregationLoader,
            new DetectionPipelineLoader());
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    List<TaskResult> taskResult = new ArrayList<>();
    DetectionAlertTaskInfo alertTaskInfo = (DetectionAlertTaskInfo) taskInfo;
    this.thirdeyeConfig = taskContext.getThirdEyeAnomalyConfiguration();

    long detectionAlertConfigId = alertTaskInfo.getDetectionAlertConfigId();
    this.detectionAlertConfig = this.alertConfigDAO.findById(detectionAlertConfigId);
    if (this.detectionAlertConfig.getProperties() == null) {
      LOG.warn(String.format("Detection alert %d contains no properties", detectionAlertConfigId));
    }

    DetectionAlertFilter alertFilter = this.alertFilterLoader.from(this.provider, this.detectionAlertConfig,
        System.currentTimeMillis());

    DetectionAlertFilterResult result = alertFilter.run();

    if (result.getResult().isEmpty()) {
      LOG.info("Zero anomalies found, skipping sending email");
    } else {
      sendEmail(result);

      this.detectionAlertConfig.setVectorClocks(result.getVectorClocks());
      this.alertConfigDAO.save(this.detectionAlertConfig);
    }
    return taskResult;
  }

  private void sendEmail(DetectionAlertFilterResult detectionResult) throws Exception {
    for (Map.Entry<List<MergedAnomalyResultDTO>, List<String>> entry : detectionResult.getResult().entrySet()) {
      List<String> recipients = entry.getValue();
      List<MergedAnomalyResultDTO> anomalies = entry.getKey();

      EmailContentFormatter emailContentFormatter =
          EmailContentFormatterFactory.fromClassName(DEFAULT_EMAIL_FORMATTER_TYPE);

      emailContentFormatter.init(new Properties(),
          EmailContentFormatterConfiguration.fromThirdEyeAnomalyConfiguration(this.thirdeyeConfig));

      Joiner joiner = Joiner.on(",").skipNulls();

      List<AnomalyResult> anomalyResultListOfGroup = new ArrayList<>();
      anomalyResultListOfGroup.addAll(anomalies);

      AlertConfigDTO alertConfig = new AlertConfigDTO();
      alertConfig.setName(this.detectionAlertConfig.getName());
      alertConfig.setFromAddress(this.detectionAlertConfig.getFromAddress());

      EmailEntity emailEntity = emailContentFormatter.getEmailEntity(alertConfig, joiner.join(recipients),
          "Thirdeye Alert : " + this.detectionAlertConfig.getName(), null, null, anomalyResultListOfGroup,
          new EmailContentFormatterContext());
      EmailHelper.sendEmailWithEmailEntity(emailEntity, thirdeyeConfig.getSmtpConfiguration());
    }
  }
}
