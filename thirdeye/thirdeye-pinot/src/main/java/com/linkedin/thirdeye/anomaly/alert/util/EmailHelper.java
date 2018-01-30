package com.linkedin.thirdeye.anomaly.alert.util;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.alert.commons.EmailEntity;
import com.linkedin.thirdeye.alert.content.EmailContentFormatterConfiguration;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.v2.AlertTaskRunnerV2;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewHandler;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewRequest;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.Weeks;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.cache.QueryCache;

/**
 * Stateless class to provide util methods to help build anomaly report
 */
public abstract class EmailHelper {

  private static final Logger LOG = LoggerFactory.getLogger(EmailHelper.class);

  private static final long HOUR_MILLIS = TimeUnit.HOURS.toMillis(1);
  private static final long DAY_MILLIS = TimeUnit.DAYS.toMillis(1);
  private static final long WEEK_MILLIS = TimeUnit.DAYS.toMillis(7);

  public static final String EMAIL_ADDRESS_SEPARATOR = ",";

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final LoadingCache<String, Long> collectionMaxDataTimeCache = CACHE_REGISTRY_INSTANCE.getDatasetMaxDataTimeCache();
  private static final QueryCache queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();

  private static final DatasetConfigManager datasetConfigManager = DAO_REGISTRY.getDatasetConfigDAO();

  private EmailHelper() {

  }

  public static void sendEmailWithTextBody(HtmlEmail email, SmtpConfiguration smtpConfigutation, String subject,
      String textBody, String fromAddress, String toAddress) throws EmailException {
    email.setTextMsg(textBody);
    sendEmail(smtpConfigutation, email, subject, fromAddress, toAddress);
  }

  public static void sendEmailWithHtml(HtmlEmail email, SmtpConfiguration smtpConfiguration, String subject,
      String htmlBody, String fromAddress, String toAddress) throws EmailException {
    email.setHtmlMsg(htmlBody);
    sendEmail(smtpConfiguration, email, subject, fromAddress, toAddress);
  }

  public static void sendEmailWithEmailEntity(EmailEntity emailEntity, SmtpConfiguration smtpConfiguration)
      throws EmailException {
    sendEmail(smtpConfiguration, emailEntity.getContent(), emailEntity.getSubject(), emailEntity.getFrom(), emailEntity.getTo());
  }

  /** Sends email according to the provided config. */
  private static void sendEmail(SmtpConfiguration config, HtmlEmail email, String subject,
      String fromAddress, String toAddress) throws EmailException {
    if (config != null) {
      email.setSubject(subject);
      LOG.info("Sending email to {}", toAddress);
      email.setHostName(config.getSmtpHost());
      email.setSmtpPort(config.getSmtpPort());
      if (config.getSmtpUser() != null && config.getSmtpPassword() != null) {
        email.setAuthenticator(
            new DefaultAuthenticator(config.getSmtpUser(), config.getSmtpPassword()));
        email.setSSLOnConnect(true);
      }
      email.setFrom(fromAddress);
      for (String address : toAddress.split(EMAIL_ADDRESS_SEPARATOR)) {
        email.addTo(address);
      }
      email.send();
      LOG.info("Email sent with subject [{}] to address [{}]!", subject, toAddress);
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


  public static void sendFailureEmailForScreenshot(String anomalyId, Throwable t, ThirdEyeConfiguration thirdeyeConfig)
      throws JobExecutionException {
    sendFailureEmailForScreenshot(anomalyId, t, thirdeyeConfig.getSmtpConfiguration(), thirdeyeConfig.getFailureFromAddress(),
        thirdeyeConfig.getFailureToAddress());
  }

  public static void sendFailureEmailForScreenshot(String anomalyId, Throwable t, SmtpConfiguration smtpConfiguration,
    String failureFromAddress, String failureToAddress) throws JobExecutionException {
    HtmlEmail email = new HtmlEmail();
    String subject = String
        .format("[ThirdEye Anomaly Detector] FAILED SCREENSHOT FOR ANOMALY ID=%s", anomalyId);
    String textBody = String
        .format("Anomaly ID:%s; Exception:%s", anomalyId, ExceptionUtils.getStackTrace(t));
    try {
      EmailHelper
          .sendEmailWithTextBody(email, smtpConfiguration, subject, textBody, failureFromAddress, failureToAddress);
    } catch (EmailException e) {
      LOG.error("Exception in sending email for failed screenshot", e);
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
      EmailHelper.sendEmailWithTextBody(email, thirdeyeConfig.getSmtpConfiguration(), subject, textBody.toString(),
              thirdeyeConfig.getFailureFromAddress(), thirdeyeConfig.getFailureToAddress());
    } catch (EmailException e) {
      LOG.error("Exception in sending email notification for incomplete datasets", e);
    }

  }
}
