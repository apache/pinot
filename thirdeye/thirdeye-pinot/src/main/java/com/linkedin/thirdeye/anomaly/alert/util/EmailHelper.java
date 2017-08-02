package com.linkedin.thirdeye.anomaly.alert.util;

import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.alert.v2.AlertTaskRunnerV2;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewHandler;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewRequest;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import com.linkedin.thirdeye.datalayer.pojo.AlertConfigBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.jfree.chart.JFreeChart;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.datasource.MetricExpression;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.cache.QueryCache;
import com.linkedin.thirdeye.datasource.comparison.TimeOnTimeComparisonHandler;
import com.linkedin.thirdeye.datasource.comparison.TimeOnTimeComparisonRequest;
import com.linkedin.thirdeye.datasource.comparison.TimeOnTimeComparisonResponse;
import com.linkedin.thirdeye.detector.email.AnomalyGraphGenerator;

/**
 * Stateless class to provide util methods to help build anomaly report
 */
public abstract class EmailHelper {

  private static final Logger LOG = LoggerFactory.getLogger(EmailHelper.class);
  private static final String PNG = ".png";

  private static final String EMAIL_REPORT_CHART_PREFIX = "email_report_chart_";

  private static final long HOUR_MILLIS = TimeUnit.HOURS.toMillis(1);
  private static final long DAY_MILLIS = TimeUnit.DAYS.toMillis(1);
  private static final long WEEK_MILLIS = TimeUnit.DAYS.toMillis(7);

  private static final int MINIMUM_GRAPH_WINDOW_HOURS = 24;
  private static final int MINIMUM_GRAPH_WINDOW_DAYS = 7;

  public static final String EMAIL_ADDRESS_SEPARATOR = ",";

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private static final LoadingCache<String, Long> collectionMaxDataTimeCache = CACHE_REGISTRY_INSTANCE.getDatasetMaxDataTimeCache();
  private static final QueryCache queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();

  private static final DatasetConfigManager datasetConfigManager = DAO_REGISTRY.getDatasetConfigDAO();

  private EmailHelper() {

  }

  public static String writeTimeSeriesChart(final EmailConfigurationDTO config,
      TimeOnTimeComparisonHandler timeOnTimeComparisonHandler, final DateTime now,
      final DateTime then, final String collection,
      final Map<RawAnomalyResultDTO, String> anomaliesWithLabels) throws JobExecutionException {
    try {
      int windowSize = config.getWindowSize();
      TimeUnit windowUnit = config.getWindowUnit();
      long windowMillis = windowUnit.toMillis(windowSize);

      // TODO provide a way for email reports to specify desired graph granularity.
      DatasetConfigDTO datasetConfig = CACHE_REGISTRY_INSTANCE.getDatasetConfigCache().get(collection);
      TimeGranularity dataGranularity = datasetConfig.bucketTimeGranularity();

      TimeOnTimeComparisonResponse chartData =
          getData(timeOnTimeComparisonHandler, config, then, now, WEEK_MILLIS, dataGranularity);
      AnomalyGraphGenerator anomalyGraphGenerator = AnomalyGraphGenerator.getInstance();
      JFreeChart chart = anomalyGraphGenerator
          .createChart(chartData, dataGranularity, windowMillis, anomaliesWithLabels);
      String chartFilePath = EMAIL_REPORT_CHART_PREFIX + config.getId() + PNG;
      LOG.info("Writing chart to {}", chartFilePath);
      anomalyGraphGenerator.writeChartToFile(chart, chartFilePath);
      return chartFilePath;
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
  }

  /**
   * Generate and send request to retrieve chart data. If the request window is too small, the graph
   * data retrieved has default window sizes based on time granularity and ending at the defined
   * endpoint: <br/> <ul> <li>DAYS: 7</li> <li>HOURS: 24</li> </ul>
   *
   * @param bucketGranularity
   *
   * @throws JobExecutionException
   */
  public static TimeOnTimeComparisonResponse getData(
      TimeOnTimeComparisonHandler timeOnTimeComparisonHandler, EmailConfigurationDTO config,
      DateTime start, final DateTime end, long baselinePeriodMillis,
      TimeGranularity bucketGranularity) throws JobExecutionException {
    start = calculateGraphDataStart(start, end, bucketGranularity);
    try {
      TimeOnTimeComparisonRequest comparisonRequest = new TimeOnTimeComparisonRequest();
      comparisonRequest.setCollectionName(config.getCollection());
      comparisonRequest.setBaselineStart(start.minus(baselinePeriodMillis));
      comparisonRequest.setBaselineEnd(end.minus(baselinePeriodMillis));
      comparisonRequest.setCurrentStart(start);
      comparisonRequest.setCurrentEnd(end);
      comparisonRequest.setEndDateInclusive(true);

      List<MetricExpression> metricExpressions = new ArrayList<>();
      MetricExpression expression = new MetricExpression(config.getMetric(), config.getCollection());
      metricExpressions.add(expression);
      comparisonRequest.setMetricExpressions(metricExpressions);
      comparisonRequest.setAggregationTimeGranularity(bucketGranularity);
      LOG.debug("Starting...");
      TimeOnTimeComparisonResponse response = timeOnTimeComparisonHandler.handle(comparisonRequest);
      LOG.debug("Done!");
      return response;
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
  }

  public static DateTime calculateGraphDataStart(DateTime start, DateTime end,
      TimeGranularity bucketGranularity) {
    TimeUnit unit = bucketGranularity.getUnit();
    long minUnits;
    if (TimeUnit.DAYS.equals(unit)) {
      minUnits = MINIMUM_GRAPH_WINDOW_DAYS;
    } else if (TimeUnit.HOURS.equals(unit)) {
      minUnits = MINIMUM_GRAPH_WINDOW_HOURS;
    } else {
      // no need to do calculation, return input start;
      return start;
    }
    long currentUnits = unit.convert(end.getMillis() - start.getMillis(), TimeUnit.MILLISECONDS);
    if (currentUnits < minUnits) {
      LOG.info("Overriding config window size {} {} with minimum default of {}", currentUnits, unit,
          minUnits);
      start = end.minus(unit.toMillis(minUnits));
    }
    return start;
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

    long baselineOffset = getBaselineOffset(compareMode);

    ContributorViewRequest request = new ContributorViewRequest();
    request.setCollection(collection);

    List<MetricExpression> metricExpressions =
        Utils.convertToMetricExpressions(metric, MetricAggFunction.SUM, collection);
    request.setMetricExpressions(metricExpressions);
    long currentEnd = System.currentTimeMillis();
    long maxDataTime = collectionMaxDataTimeCache.get(collection);
    if (currentEnd > maxDataTime) {
      currentEnd = maxDataTime;
    }

    // align to nearest hour
    currentEnd = (currentEnd - (currentEnd % HOUR_MILLIS)) - offsetDelayMillis;

    String aggTimeGranularity = "HOURS";
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
      aggTimeGranularity = datasetConfigDTO.bucketTimeGranularity().getUnit().name();
      currentEnd = currentEnd - (currentEnd % DAY_MILLIS);
      currentStart = currentEnd - WEEK_MILLIS;
    }

    long baselineStart = currentStart - baselineOffset ;
    long baselineEnd = currentEnd - baselineOffset;

    String timeZone = datasetConfigDTO.getTimezone();
    request.setBaselineStart(new DateTime(baselineStart, DateTimeZone.forID(timeZone)));
    request.setBaselineEnd(new DateTime(baselineEnd, DateTimeZone.forID(timeZone)));
    request.setCurrentStart(new DateTime(currentStart, DateTimeZone.forID(timeZone)));
    request.setCurrentEnd(new DateTime(currentEnd, DateTimeZone.forID(timeZone)));
    request.setTimeGranularity(Utils.getAggregationTimeGranularity(aggTimeGranularity, collection));
    request.setGroupByDimensions(dimensions);
    ContributorViewHandler handler = new ContributorViewHandler(queryCache);
    return handler.process(request);
  }

  public static ContributorViewResponse getContributorDataForDataReport(String collection, String metric, List<String> dimensions)
      throws Exception {
    return getContributorDataForDataReport(collection, metric, dimensions, AlertConfigBean.COMPARE_MODE.WoW, 2 * 36_00_000, false); // add 2 hours delay
  }

  public static long getBaselineOffset(AlertConfigBean.COMPARE_MODE compareMode) {
    switch (compareMode) {
    case Wo2W:
      return 2 * WEEK_MILLIS;
    case Wo3W:
      return 3 * WEEK_MILLIS;
    case Wo4W:
      return 4 * WEEK_MILLIS;
    case WoW:
      default:
      return WEEK_MILLIS;
    }
  }


  public static void sendFailureEmailForScreenshot(String anomalyId, Throwable t, ThirdEyeAnomalyConfiguration thirdeyeConfig)
      throws JobExecutionException {
    HtmlEmail email = new HtmlEmail();
    String subject = String
        .format("[ThirdEye Anomaly Detector] FAILED SCREENSHOT FOR ANOMALY ID=%s", anomalyId);
    String textBody = String
        .format("Anomaly ID:%s; Exception:%s", anomalyId, ExceptionUtils.getStackTrace(t));
    try {
      EmailHelper
          .sendEmailWithTextBody(email, thirdeyeConfig.getSmtpConfiguration(), subject, textBody,
              thirdeyeConfig.getFailureFromAddress(), thirdeyeConfig.getFailureToAddress());
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
