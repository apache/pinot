package com.linkedin.thirdeye.detection.alert;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
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
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DefaultDataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineLoader;
import com.linkedin.thirdeye.rootcause.timeseries.Baseline;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregate;
import com.linkedin.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * The Detection alert task runner. This runner looks for the new anomalies and run the detection alert filter to get
 * mappings from anomalies to recipients and then send email to the recipients.
 */
public class DetectionAlertTaskRunner implements TaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionAlertTaskRunner.class);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();
  private static final String DEFAULT_EMAIL_FORMATTER_TYPE = "MultipleAnomaliesEmailContentFormatter";

  private DetectionAlertConfigManager alertConfigDAO;
  private MetricConfigManager metricDAO;
  private DatasetConfigManager datasetDAO;
  private DetectionAlertConfigDTO detectionAlertConfig;
  private final TimeSeriesLoader timeseriesLoader;
  private final AggregationLoader aggregationLoader;
  private final DetectionAlertFilterLoader alertFilterLoader;
  private final DataProvider provider;
  private ThirdEyeAnomalyConfiguration thirdeyeConfig;

  public DetectionAlertTaskRunner() {
    this.alertConfigDAO = DAO_REGISTRY.getDetectionAlertConfigManager();
    this.alertFilterLoader = new DetectionAlertFilterLoader();

    MergedAnomalyResultManager anomalyMergedResultDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    EventManager eventDAO = DAORegistry.getInstance().getEventDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.timeseriesLoader =
        new DefaultTimeSeriesLoader(this.metricDAO, this.datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache());

    this.aggregationLoader =
        new DefaultAggregationLoader(this.metricDAO, this.datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.provider = new DefaultDataProvider(this.metricDAO, this.datasetDAO, eventDAO, anomalyMergedResultDAO,
        this.timeseriesLoader, this.aggregationLoader, new DetectionPipelineLoader());
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

    DetectionAlertFilter alertFilter =
        this.alertFilterLoader.from(this.provider, this.detectionAlertConfig, System.currentTimeMillis());

    DetectionAlertFilterResult result = alertFilter.run();

    if (result.getResult().isEmpty()) {
      LOG.info("Zero anomalies found, skipping sending email");
    } else {
      fillInCurrentAndBaselineValue(result.getAllAnomalies());
      sendEmail(result);

      this.detectionAlertConfig.setVectorClocks(result.getVectorClocks());
      this.alertConfigDAO.save(this.detectionAlertConfig);
    }
    return taskResult;
  }

  private void sendEmail(DetectionAlertFilterResult detectionResult) throws Exception {
    for (Map.Entry<Set<String>, Set<MergedAnomalyResultDTO>> entry : detectionResult.getResult().entrySet()) {
      Set<String> recipients = entry.getKey();
      Set<MergedAnomalyResultDTO> anomalies = entry.getValue();

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

  void fillInCurrentAndBaselineValue(Collection<MergedAnomalyResultDTO> anomalies) throws Exception {
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      if (anomaly.getAvgBaselineVal() == 0 || anomaly.getAvgCurrentVal() == 0) {
        MetricConfigDTO metricConfigDTO = this.metricDAO.findByMetricAndDataset(anomaly.getMetric(), anomaly.getCollection());
        if (metricConfigDTO == null) {
          throw new IllegalArgumentException(String.format("Could not resolve metric '%s' and dataset '%s'", anomaly.getMetric(), anomaly.getCollection()));
        }

        DatasetConfigDTO datasetConfigDTO = this.datasetDAO.findByDataset(anomaly.getCollection());
        if (datasetConfigDTO == null) {
          throw new IllegalArgumentException(String.format("Could not dataset '%s'", anomaly.getCollection()));
        }

        Multimap<String, String> filters = getFiltersFromDimensionMaps(anomaly);

        MetricSlice slice = MetricSlice.from(metricConfigDTO.getId(), anomaly.getStartTime(), anomaly.getEndTime(), filters);
        anomaly.setAvgCurrentVal(getAggregate(slice));

        DateTimeZone timezone = getDateTimeZone(datasetConfigDTO);

        Baseline baseline = BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.MEDIAN, 1, 1, timezone);
        MetricSlice baselineSlice = baseline.scatter(slice).get(0);
        anomaly.setAvgBaselineVal(getAggregate(baselineSlice));
      }
    }
  }

  private static DateTimeZone getDateTimeZone(DatasetConfigDTO datasetConfigDTO) {
    try {
      if (StringUtils.isBlank(datasetConfigDTO.getTimezone())) {
        return DateTimeZone.forID(datasetConfigDTO.getTimezone());
      }
    } catch (Exception ignore) {
      // ignored
    }
    return DateTimeZone.UTC;
  }

  private double getAggregate(MetricSlice slice) throws Exception {
    DataFrame df = this.aggregationLoader.loadAggregate(slice, Collections.<String>emptyList());
    try {
      return df.getDouble(COL_VALUE, 0);
    } catch (Exception e) {
      return Double.NaN;
    }
  }

  private static Multimap<String, String> getFiltersFromDimensionMaps(MergedAnomalyResultDTO anomaly) {
    Multimap<String, String> filters = ArrayListMultimap.create();
    for (Map.Entry<String, String> entry : anomaly.getDimensions().entrySet()) {
      filters.put(entry.getKey(), entry.getValue());
    }
    return filters;
  }
}
