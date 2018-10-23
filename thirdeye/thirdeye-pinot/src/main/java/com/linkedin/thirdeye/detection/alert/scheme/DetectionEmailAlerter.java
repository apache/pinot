/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection.alert.scheme;

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
import com.linkedin.thirdeye.anomalydetection.context.AnomalyResult;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.DetectionAlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.loader.AggregationLoader;
import com.linkedin.thirdeye.datasource.loader.DefaultAggregationLoader;
import com.linkedin.thirdeye.detection.CurrentAndBaselineLoader;
import com.linkedin.thirdeye.detection.alert.AlertUtils;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import com.linkedin.thirdeye.detection.alert.DetectionAlertFilterResult;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DetectionEmailAlerter extends DetectionAlertScheme {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionEmailAlerter.class);

  private static final Comparator<AnomalyResult> COMPARATOR_DESC = new Comparator<AnomalyResult>() {
    @Override
    public int compare(AnomalyResult o1, AnomalyResult o2) {
      return -1 * Long.compare(o1.getStartTime(), o2.getStartTime());
    }
  };

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private static final String DEFAULT_EMAIL_FORMATTER_TYPE = "MultipleAnomaliesEmailContentFormatter";

  private DetectionAlertConfigManager alertConfigDAO;
  private final DetectionAlertConfigDTO config;
  private final DetectionAlertFilterResult result;
  private ThirdEyeAnomalyConfiguration thirdeyeConfig;
  private CurrentAndBaselineLoader currentAndBaselineLoader;

  public DetectionEmailAlerter(DetectionAlertConfigDTO config, TaskContext taskContext,
      DetectionAlertFilterResult result) throws Exception {
    super(config, result);
    this.alertConfigDAO = DAO_REGISTRY.getDetectionAlertConfigManager();
    this.thirdeyeConfig = taskContext.getThirdEyeAnomalyConfiguration();

    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    AggregationLoader aggregationLoader =
        new DefaultAggregationLoader(metricDAO, datasetDAO, ThirdEyeCacheRegistry.getInstance().getQueryCache(),
            ThirdEyeCacheRegistry.getInstance().getDatasetMaxDataTimeCache());
    this.currentAndBaselineLoader = new CurrentAndBaselineLoader(metricDAO, datasetDAO, aggregationLoader);

    this.config = config;
    this.result = result;
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

  private void sendEmail(DetectionAlertFilterResult detectionResult) throws Exception {
    for (Map.Entry<DetectionAlertFilterRecipients, Set<MergedAnomalyResultDTO>> entry : detectionResult.getResult().entrySet()) {
      DetectionAlertFilterRecipients recipients = entry.getKey();
      Set<MergedAnomalyResultDTO> anomalies = entry.getValue();

      if (!this.thirdeyeConfig.getEmailWhitelist().isEmpty()) {
        recipients.getTo().retainAll(this.thirdeyeConfig.getEmailWhitelist());
        recipients.getCc().retainAll(this.thirdeyeConfig.getEmailWhitelist());
        recipients.getBcc().retainAll(this.thirdeyeConfig.getEmailWhitelist());
      }

      if (recipients.getTo().isEmpty()) {
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
      alertConfig.setName(this.config.getName());
      alertConfig.setFromAddress(this.config.getFrom());
      alertConfig.setSubjectType(this.config.getSubjectType());

      EmailEntity emailEntity = emailContentFormatter.getEmailEntity(alertConfig, null,
          "Thirdeye Alert : " + this.config.getName(), null, null, anomalyResultListOfGroup,
          new EmailContentFormatterContext());

      HtmlEmail email = emailEntity.getContent();
      email.setFrom(this.config.getFrom());
      email.setTo(AlertUtils.toAddress(recipients.getTo()));
      if (CollectionUtils.isNotEmpty(recipients.getCc())) {
        email.setCc(AlertUtils.toAddress(recipients.getCc()));
      }
      if (CollectionUtils.isNotEmpty(recipients.getBcc())) {
        email.setBcc(AlertUtils.toAddress(recipients.getBcc()));
      }

      this.sendEmail(emailEntity);
    }
  }

  @Override
  public void run() throws Exception {
    if (result.getResult().isEmpty()) {
      LOG.info("Zero anomalies found, skipping sending email");
    } else {
      this.currentAndBaselineLoader.fillInCurrentAndBaselineValue(result.getAllAnomalies());
      sendEmail(result);

      this.config.setVectorClocks(mergeVectorClock(
          this.config.getVectorClocks(),
          makeVectorClock(result.getAllAnomalies())));

      long highWaterMark = getHighWaterMark(result.getAllAnomalies());
      if (this.config.getHighWaterMark() != null) {
        highWaterMark = Math.max(this.config.getHighWaterMark(), highWaterMark);
      }
      this.config.setHighWaterMark(highWaterMark);

      this.alertConfigDAO.save(this.config);
    }
  }
}
