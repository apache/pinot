package com.linkedin.thirdeye.detection.alert;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.linkedin.thirdeye.alert.commons.EmailContentFormatterFactory;
import com.linkedin.thirdeye.alert.commons.EmailEntity;
import com.linkedin.thirdeye.alert.content.EmailContentFormatter;
import com.linkedin.thirdeye.alert.content.EmailContentFormatterConfiguration;
import com.linkedin.thirdeye.alert.content.EmailContentFormatterContext;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
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
import com.linkedin.thirdeye.datasource.loader.AggregationLoader;
import com.linkedin.thirdeye.datasource.loader.DefaultAggregationLoader;
import com.linkedin.thirdeye.datasource.loader.DefaultTimeSeriesLoader;
import com.linkedin.thirdeye.datasource.loader.TimeSeriesLoader;
import com.linkedin.thirdeye.detection.CurrentAndBaselineLoader;
import com.linkedin.thirdeye.detection.DataProvider;
import com.linkedin.thirdeye.detection.DefaultDataProvider;
import com.linkedin.thirdeye.detection.DetectionPipelineLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The Detection alert task runner. This runner looks for the new anomalies and run the detection alert filter to get
 * mappings from anomalies to recipients and then send email to the recipients.
 */
public class DetectionAlertTaskRunner implements TaskRunner {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionAlertTaskRunner.class);

  private static final Comparator<AnomalyResult> COMPARATOR_DESC = new Comparator<AnomalyResult>() {
    @Override
    public int compare(AnomalyResult o1, AnomalyResult o2) {
      return -1 * Long.compare(o1.getStartTime(), o2.getStartTime());
    }
  };

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
  private CurrentAndBaselineLoader currentAndBaselineLoader;

  public DetectionAlertTaskRunner() {
    this.alertConfigDAO = DAO_REGISTRY.getDetectionAlertConfigManager();
    this.alertFilterLoader = new DetectionAlertFilterLoader();

    MergedAnomalyResultManager anomalyMergedResultDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    EventManager eventDAO = DAORegistry.getInstance().getEventDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.timeseriesLoader = new DefaultTimeSeriesLoader(this.metricDAO, this.datasetDAO,
        ThirdEyeCacheRegistry.getInstance().getQueryCache());

    this.aggregationLoader = new DefaultAggregationLoader(this.metricDAO, this.datasetDAO,
        ThirdEyeCacheRegistry.getInstance().getQueryCache(),
        ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());

    this.provider = new DefaultDataProvider(this.metricDAO, this.datasetDAO, eventDAO, anomalyMergedResultDAO,
        this.timeseriesLoader, this.aggregationLoader, new DetectionPipelineLoader());

    this.currentAndBaselineLoader =
        new CurrentAndBaselineLoader(this.metricDAO, this.datasetDAO, this.aggregationLoader);
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    ThirdeyeMetricsUtil.alertTaskCounter.inc();

    try {
      List<TaskResult> taskResult = new ArrayList<>();
      DetectionAlertTaskInfo alertTaskInfo = (DetectionAlertTaskInfo) taskInfo;
      this.thirdeyeConfig = taskContext.getThirdEyeAnomalyConfiguration();

      long detectionAlertConfigId = alertTaskInfo.getDetectionAlertConfigId();
      this.detectionAlertConfig = this.alertConfigDAO.findById(detectionAlertConfigId);
      if (this.detectionAlertConfig.getProperties() == null) {
        LOG.warn(String.format("Detection alert %d contains no properties", detectionAlertConfigId));
      }

      DetectionAlertFilter alertFilter = this.alertFilterLoader.from(this.provider, this.detectionAlertConfig, System.currentTimeMillis());

      DetectionAlertFilterResult result = alertFilter.run();

      if (result.getResult().isEmpty()) {
        LOG.info("Zero anomalies found, skipping sending email");
      } else {
        this.currentAndBaselineLoader.fillInCurrentAndBaselineValue(result.getAllAnomalies());
        sendEmail(result);

        this.detectionAlertConfig.setVectorClocks(mergeVectorClock(
            this.detectionAlertConfig.getVectorClocks(),
            makeVectorClock(result.getAllAnomalies())));

        long highWaterMark = getHighWaterMark(result.getAllAnomalies());
        if (this.detectionAlertConfig.getHighWaterMark() != null) {
          highWaterMark = Math.max(this.detectionAlertConfig.getHighWaterMark(), highWaterMark);
        }
        this.detectionAlertConfig.setHighWaterMark(highWaterMark);

        this.alertConfigDAO.save(this.detectionAlertConfig);
      }
      return taskResult;

    } finally {
      ThirdeyeMetricsUtil.alertTaskSuccessCounter.inc();
    }
  }

  private void sendEmail(DetectionAlertFilterResult detectionResult) throws Exception {
    for (Map.Entry<DetectionAlertFilterRecipients, Set<MergedAnomalyResultDTO>> entry : detectionResult.getResult().entrySet()) {
      DetectionAlertFilterRecipients recipients = entry.getKey();
      Set<MergedAnomalyResultDTO> anomalies = entry.getValue();

      if (!this.thirdeyeConfig.getEmailWhitelist().isEmpty()) {
        recipients.to.retainAll(this.thirdeyeConfig.getEmailWhitelist());
        recipients.cc.retainAll(this.thirdeyeConfig.getEmailWhitelist());
        recipients.bcc.retainAll(this.thirdeyeConfig.getEmailWhitelist());
      }

      if (recipients.to.isEmpty()) {
        LOG.warn("Email doesn't have any valid (whitelisted) recipients. Skipping.");
        continue;
      }

      EmailContentFormatter emailContentFormatter =
          EmailContentFormatterFactory.fromClassName(DEFAULT_EMAIL_FORMATTER_TYPE);

      emailContentFormatter.init(new Properties(),
          EmailContentFormatterConfiguration.fromThirdEyeAnomalyConfiguration(this.thirdeyeConfig));

      List<AnomalyResult> anomalyResultListOfGroup = new ArrayList<>();
      anomalyResultListOfGroup.addAll(anomalies);
      Collections.sort(anomalyResultListOfGroup, COMPARATOR_DESC);

      AlertConfigDTO alertConfig = new AlertConfigDTO();
      alertConfig.setName(this.detectionAlertConfig.getName());
      alertConfig.setFromAddress(this.detectionAlertConfig.getFrom());
      alertConfig.setSubjectType(this.detectionAlertConfig.getSubjectType());

      EmailEntity emailEntity = emailContentFormatter.getEmailEntity(alertConfig, null,
          "Thirdeye Alert : " + this.detectionAlertConfig.getName(), null, null, anomalyResultListOfGroup,
          new EmailContentFormatterContext());

      HtmlEmail email = emailEntity.getContent();
      email.setFrom(this.detectionAlertConfig.getFrom());
      email.setTo(AlertUtils.toAddress(recipients.to));
      email.setCc(AlertUtils.toAddress(recipients.cc));
      email.setBcc(AlertUtils.toAddress(recipients.bcc));

      this.sendEmail(emailEntity);
    }
  }

  private static long getHighWaterMark(Collection<MergedAnomalyResultDTO> anomalies) {
    if (anomalies.isEmpty()) {
      return -1;
    }
    return Collections.max(Collections2.transform(anomalies, new Function<MergedAnomalyResultDTO, Long>() {
      @Override
      public Long apply(MergedAnomalyResultDTO mergedAnomalyResultDTO) {
        return mergedAnomalyResultDTO.getId();
      }
    }));
  }

  private static long getLastTimeStamp(Collection<MergedAnomalyResultDTO> anomalies, long startTime) {
    long lastTimeStamp = startTime;
    for (MergedAnomalyResultDTO anomaly : anomalies) {
      lastTimeStamp = Math.max(anomaly.getEndTime(), lastTimeStamp);
    }
    return lastTimeStamp;
  }

  private static Map<Long, Long> makeVectorClock(Collection<MergedAnomalyResultDTO> anomalies) {
    Multimap<Long, MergedAnomalyResultDTO> grouped = Multimaps.index(anomalies, new Function<MergedAnomalyResultDTO, Long>() {
      @Nullable
      @Override
      public Long apply(@Nullable MergedAnomalyResultDTO mergedAnomalyResultDTO) {
        return mergedAnomalyResultDTO.getDetectionConfigId();
      }
    });
    Map<Long, Long> detection2max = new HashMap<>();
    for (Map.Entry<Long, Collection<MergedAnomalyResultDTO>> entry : grouped.asMap().entrySet()) {
      detection2max.put(entry.getKey(), getLastTimeStamp(entry.getValue(), -1));
    }
    return detection2max;
  }

  private static Map<Long, Long> mergeVectorClock(Map<Long, Long> a, Map<Long, Long> b) {
    Set<Long> keySet = new HashSet<>();
    keySet.addAll(a.keySet());
    keySet.addAll(b.keySet());

    Map<Long, Long> result = new HashMap<>();
    for (Long detectionId : keySet) {
      long valA = MapUtils.getLongValue(a, detectionId, -1);
      long valB = MapUtils.getLongValue(b, detectionId, -1);
      result.put(detectionId, Math.max(valA, valB));
    }

    return result;
  }

  /** Sends email according to the provided config. */
  private void sendEmail(EmailEntity entity) throws EmailException {
    HtmlEmail email = entity.getContent();
    SmtpConfiguration config = this.thirdeyeConfig.getSmtpConfiguration();

    if (config == null) {
      LOG.error("No email configuration available. Skipping.");
      return;
    }

    email.setHostName(config.getSmtpHost());
    email.setSmtpPort(config.getSmtpPort());
    if (config.getSmtpUser() != null && config.getSmtpPassword() != null) {
      email.setAuthenticator(new DefaultAuthenticator(config.getSmtpUser(), config.getSmtpPassword()));
      email.setSSLOnConnect(true);
    }
    email.send();

    int recipientCount = email.getToAddresses().size() + email.getCcAddresses().size() + email.getBccAddresses().size();
    LOG.info("Sent email sent with subject '{}' to {} recipients", email.getSubject(), recipientCount);
  }
}
