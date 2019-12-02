/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.anomaly.alert.util;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.notification.commons.EmailEntity;
import org.apache.pinot.thirdeye.notification.commons.SmtpConfiguration;
import org.apache.pinot.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import org.apache.pinot.thirdeye.anomaly.alert.v2.AlertTaskRunnerV2;
import org.apache.pinot.thirdeye.anomaly.utils.EmailUtils;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dashboard.Utils;
import org.apache.pinot.thirdeye.dashboard.views.contributor.ContributorViewHandler;
import org.apache.pinot.thirdeye.dashboard.views.contributor.ContributorViewRequest;
import org.apache.pinot.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import org.apache.pinot.thirdeye.datalayer.pojo.AlertConfigBean;

import org.apache.pinot.thirdeye.detection.alert.AlertUtils;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilterRecipients;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.Weeks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.MetricExpression;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;

import static org.apache.pinot.thirdeye.notification.commons.SmtpConfiguration.SMTP_CONFIG_KEY;


/**
 * Stateless class to provide util methods to help build anomaly report
 */
public abstract class EmailHelper {

  private static final Logger LOG = LoggerFactory.getLogger(EmailHelper.class);

  private static final long HOUR_MILLIS = TimeUnit.HOURS.toMillis(1);
  private static final long DAY_MILLIS = TimeUnit.DAYS.toMillis(1);
  private static final long WEEK_MILLIS = TimeUnit.DAYS.toMillis(7);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final LoadingCache<String, Long> collectionMaxDataTimeCache = CACHE_REGISTRY_INSTANCE.getDatasetMaxDataTimeCache();
  private static final QueryCache queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();

  private static final DatasetConfigManager datasetConfigManager = DAO_REGISTRY.getDatasetConfigDAO();

  private EmailHelper() {

  }

  public static void sendEmailWithTextBody(HtmlEmail email, SmtpConfiguration smtpConfigutation, String subject,
      String textBody, String fromAddress, DetectionAlertFilterRecipients recipients) throws EmailException {
    email.setTextMsg(textBody);
    sendEmail(smtpConfigutation, email, subject, fromAddress, recipients);
  }

  public static void sendEmailWithHtml(HtmlEmail email, SmtpConfiguration smtpConfiguration, String subject,
      String htmlBody, String fromAddress, DetectionAlertFilterRecipients recipients) throws EmailException {
    email.setHtmlMsg(htmlBody);
    sendEmail(smtpConfiguration, email, subject, fromAddress, recipients);
  }

  public static void sendEmailWithEmailEntity(EmailEntity emailEntity, SmtpConfiguration smtpConfiguration)
      throws EmailException {
    sendEmail(smtpConfiguration, emailEntity.getContent(), emailEntity.getSubject(), emailEntity.getFrom(), emailEntity.getTo());
  }

  /** Sends email according to the provided config. */
  private static void sendEmail(SmtpConfiguration config, HtmlEmail email, String subject,
      String fromAddress, DetectionAlertFilterRecipients recipients) throws EmailException {
    if (config != null) {
      email.setSubject(subject);
      LOG.info("Sending email to {}", recipients.toString());
      email.setHostName(config.getSmtpHost());
      email.setSmtpPort(config.getSmtpPort());
      if (config.getSmtpUser() != null && config.getSmtpPassword() != null) {
        email.setAuthenticator(
            new DefaultAuthenticator(config.getSmtpUser(), config.getSmtpPassword()));
        email.setSSLOnConnect(true);
      }
      email.setFrom(fromAddress);
      email.setTo(AlertUtils.toAddress(recipients.getTo()));
      if (CollectionUtils.isNotEmpty(recipients.getCc())) {
        email.setCc(AlertUtils.toAddress(recipients.getCc()));
      }
      if (CollectionUtils.isNotEmpty(recipients.getBcc())) {
        email.setBcc(AlertUtils.toAddress(recipients.getBcc()));
      }
      email.send();
      LOG.info("Email sent with subject [{}] to address [{}]!", subject, recipients.toString());
    } else {
      LOG.error("No email config provided for email with subject [{}]!", subject);
    }
  }

  public static ContributorViewResponse getContributorDataForDataReport(String collection,
      String metric, List<String> dimensions, AlertConfigBean.COMPARE_MODE compareMode,
      long offsetDelayMillis, boolean intraday)
      throws Exception {

    return getContributorDataForDataReport(collection, metric, dimensions, HashMultimap.<String, String>create(),
        compareMode, offsetDelayMillis, intraday);
  }


  public static ContributorViewResponse getContributorDataForDataReport(String collection,
      String metric, List<String> dimensions, Multimap<String, String> filters, AlertConfigBean.COMPARE_MODE compareMode,
      long offsetDelayMillis, boolean intraday)
      throws Exception {
    long currentEnd = System.currentTimeMillis();
    long maxDataTime = collectionMaxDataTimeCache.get(collection);
    if (currentEnd > maxDataTime) {
      currentEnd = maxDataTime;
    }

    // align to nearest hour
    currentEnd = (currentEnd - (currentEnd % HOUR_MILLIS)) - offsetDelayMillis;

    long currentStart = currentEnd - DAY_MILLIS;

    // intraday option
    if (intraday) {
      DateTimeZone timeZone = DateTimeZone.forTimeZone(AlertTaskRunnerV2.DEFAULT_TIME_ZONE);
      DateTime endDate = new DateTime(currentEnd, timeZone);
      DateTime intraDayStartTime = new DateTime(endDate.toString().split("T")[0], timeZone);
      if (intraDayStartTime.getMillis() != currentEnd) {
        currentStart = intraDayStartTime.getMillis();
      }
    }

    DatasetConfigDTO datasetConfigDTO = datasetConfigManager.findByDataset(collection);
    if (datasetConfigDTO != null && TimeUnit.DAYS.equals(datasetConfigDTO.bucketTimeGranularity().getUnit())) {
      currentEnd = currentEnd - (currentEnd % DAY_MILLIS);
      currentStart = currentEnd - WEEK_MILLIS;
    }
    return getContributorDataForDataReport(collection, metric, dimensions, filters, compareMode, MetricAggFunction.SUM,
        currentStart, currentEnd);
  }


  public static ContributorViewResponse getContributorDataForDataReport(String collection, String metric,
      List<String> dimensions, Multimap<String, String> filters, AlertConfigBean.COMPARE_MODE compareMode, MetricAggFunction metricAggFunction,
      long startInMillis, long endInMillis) throws Exception {
    DatasetConfigDTO datasetConfigDTO = datasetConfigManager.findByDataset(collection);
    DateTimeZone timeZone = DateTimeZone.forID(datasetConfigDTO.getTimezone());

    Period baselinePeriod = getBaselinePeriod(compareMode);
    ContributorViewRequest request = new ContributorViewRequest();
    request.setCollection(collection);

    List<MetricExpression> metricExpressions =
        Utils.convertToMetricExpressions(metric, metricAggFunction, collection);
    request.setMetricExpressions(metricExpressions);

    long currentEndInMills = endInMillis;
    long maxDataTime = collectionMaxDataTimeCache.get(collection);
    currentEndInMills = Math.min(currentEndInMills, maxDataTime);

    DateTime currentStart = new DateTime(startInMillis, timeZone);
    DateTime currentEnd = new DateTime(currentEndInMills, timeZone);

    String aggTimeGranularity = "HOURS";
    if (datasetConfigDTO != null && TimeUnit.DAYS.equals(datasetConfigDTO.bucketTimeGranularity().getUnit())) {
      aggTimeGranularity = datasetConfigDTO.bucketTimeGranularity().getUnit().name();
    }

    DateTime baselineStart = currentStart.minus(baselinePeriod);
    DateTime baselineEnd = currentEnd.minus(baselinePeriod);

    request.setBaselineStart(baselineStart);
    request.setBaselineEnd(baselineEnd);
    request.setCurrentStart(currentStart);
    request.setCurrentEnd(currentEnd);
    request.setTimeGranularity(Utils.getAggregationTimeGranularity(aggTimeGranularity, collection));
    request.setGroupByDimensions(dimensions);
    request.setFilters(filters);
    ContributorViewHandler handler = new ContributorViewHandler(queryCache);
    return handler.process(request);
  }

  public static Period getBaselinePeriod(AlertConfigBean.COMPARE_MODE compareMode) {
    switch (compareMode) {
      case Wo2W:
        return Weeks.TWO.toPeriod();
      case Wo3W:
        return Weeks.THREE.toPeriod();
      case Wo4W:
        return Weeks.weeks(4).toPeriod();
      case WoW:
      default:
        return Weeks.ONE.toPeriod();
    }
  }

  public static void sendNotificationForDataIncomplete(
      Multimap<String, DataCompletenessConfigDTO> incompleteEntriesToNotify, ThirdEyeAnomalyConfiguration thirdeyeConfig) {
    HtmlEmail email = new HtmlEmail();
    String subject = String.format("Data Completeness Checker Report");
    StringBuilder textBody = new StringBuilder();
    for (String dataset : incompleteEntriesToNotify.keySet()) {
      List<DataCompletenessConfigDTO> entries = Lists.newArrayList(incompleteEntriesToNotify.get(dataset));
      textBody.append(String.format("\nDataset: %s\n", dataset));
      for (DataCompletenessConfigDTO entry : entries) {
        textBody.append(String.format("%s ", entry.getDateToCheckInSDF()));
      }
      textBody.append("\n*******************************************************\n");
    }
    LOG.info("Data Completeness Checker Report : Sending email to {} with subject {} and text {}",
        thirdeyeConfig.getFailureToAddress(), subject, textBody.toString());

    try {
      EmailHelper.sendEmailWithTextBody(email,
          SmtpConfiguration.createFromProperties(thirdeyeConfig.getAlerterConfiguration().get(SMTP_CONFIG_KEY)),
          subject, textBody.toString(), thirdeyeConfig.getFailureFromAddress(),
          new DetectionAlertFilterRecipients(EmailUtils.getValidEmailAddresses(thirdeyeConfig.getFailureToAddress())));
    } catch (EmailException e) {
      LOG.error("Exception in sending email notification for incomplete datasets", e);
    }

  }
}
