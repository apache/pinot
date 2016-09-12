package com.linkedin.thirdeye.anomaly.alert;

import com.linkedin.thirdeye.anomaly.SmtpConfiguration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.jfree.chart.JFreeChart;
import org.joda.time.DateTime;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonHandler;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonRequest;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonResponse;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.email.AnomalyGraphGenerator;

public abstract class EmailHelper {

  private static final Logger LOG = LoggerFactory.getLogger(EmailHelper.class);
  private static final String PNG = ".png";

  private static final String EMAIL_REPORT_CHART_PREFIX = "email_report_chart_";

  private static final long WEEK_MILLIS = TimeUnit.DAYS.toMillis(7); // TODO make w/w configurable.
  private static final int MINIMUM_GRAPH_WINDOW_HOURS = 24;
  private static final int MINIMUM_GRAPH_WINDOW_DAYS = 7;

  public static final String EMAIL_ADDRESS_SEPARATOR = ",";

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

      ThirdEyeClient client = timeOnTimeComparisonHandler.getClient();
      // TODO provide a way for email reports to specify desired graph granularity.
      TimeGranularity dataGranularity =
          client.getCollectionSchema(collection).getTime().getDataGranularity();

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
      MetricExpression expression = new MetricExpression(config.getMetric());
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
}
