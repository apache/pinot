package com.linkedin.thirdeye.detector.email;

import static com.linkedin.thirdeye.detector.driver.FailureEmailConfiguration.FAILURE_EMAIL_CONFIG_KEY;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.mail.DefaultAuthenticator;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.hibernate.SessionFactory;
import org.jfree.chart.JFreeChart;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonHandler;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonRequest;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonResponse;
import com.linkedin.thirdeye.detector.api.AnomalyResult;
import com.linkedin.thirdeye.detector.api.EmailConfiguration;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.db.HibernateSessionWrapper;
import com.linkedin.thirdeye.detector.driver.FailureEmailConfiguration;
import com.linkedin.thirdeye.detector.lib.util.JobUtils;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModelException;
import freemarker.template.TemplateNumberModel;
import freemarker.template.TemplateScalarModel;

public class EmailReportJob implements Job {
  private static final String PNG = ".png";

  private static final String EMAIL_REPORT_CHART_PREFIX = "email_report_chart_";

  private static final long WEEK_MILLIS = TimeUnit.DAYS.toMillis(7); // TODO make w/w configurable.
  private static final int MINIMUM_GRAPH_WINDOW_HOURS = 24;
  private static final int MINIMUM_GRAPH_WINDOW_DAYS = 7;

  private static final Logger LOG = LoggerFactory.getLogger(EmailReportJob.class);

  public static final String CONFIG = "CONFIG";
  public static final String RESULT_DAO = "RESULT_DAO";
  public static final String SESSION_FACTORY = "SESSION_FACTORY";
  public static final String APPLICATION_PORT = "APPLICATION_PORT";
  public static final String TIME_ON_TIME_COMPARISON_HANDLER = "TIME_ON_TIME_COMPARISON_HANDLER";
  public static final String DASHBOARD_HOST = "DASHBOARD_HOST";

  public static final String CHARSET = "UTF-8";

  @Override
  public void execute(final JobExecutionContext context) throws JobExecutionException {
    final EmailConfiguration config =
        (EmailConfiguration) context.getJobDetail().getJobDataMap().get(CONFIG);
    final FailureEmailConfiguration failureEmailConfig =
        (FailureEmailConfiguration) context.getJobDetail().getJobDataMap()
            .get(FAILURE_EMAIL_CONFIG_KEY);
    try {
      run(context, config);
    } catch (Throwable t) {
      LOG.error("Job failed with exception:", t);

      String collection = config.getCollection();
      String metric = config.getMetric();

      String subject =
          String.format("FAILED ALERT ID=%d (%s:%s)", config.getId(), collection, metric);
      String textBody =
          String.format("%s%n%nException:%s", config.toString(), ExceptionUtils.getStackTrace(t));
      try {
        JobUtils.sendFailureEmail(failureEmailConfig, subject, textBody);
      } catch (EmailException e) {
        throw new JobExecutionException(e);
      }
    }
  }

  private void run(final JobExecutionContext context, final EmailConfiguration config)
      throws JobExecutionException, ExecutionException {
    LOG.info("Starting email report {}", config.getId());
    SessionFactory sessionFactory =
        (SessionFactory) context.getJobDetail().getJobDataMap().get(SESSION_FACTORY);
    int applicationPort = context.getJobDetail().getJobDataMap().getInt(APPLICATION_PORT);
    TimeOnTimeComparisonHandler timeOnTimeComparisonHandler =
        (TimeOnTimeComparisonHandler) context.getJobDetail().getJobDataMap()
            .get(TIME_ON_TIME_COMPARISON_HANDLER);
    ThirdEyeClient client = timeOnTimeComparisonHandler.getClient();
    String dashboardHost = context.getJobDetail().getJobDataMap().getString(DASHBOARD_HOST);

    // Get time
    Date scheduledFireTime = context.getScheduledFireTime();
    long deltaMillis =
        TimeUnit.MILLISECONDS.convert(config.getWindowSize(), config.getWindowUnit());
    long delayMillis =
        TimeUnit.MILLISECONDS.convert(config.getWindowDelay(), config.getWindowDelayUnit());
    final DateTime nowWithoutDelay = new DateTime(scheduledFireTime, DateTimeZone.UTC);
    final DateTime now = nowWithoutDelay.minus(delayMillis);
    final DateTime then = now.minus(deltaMillis);

    final String collection = config.getCollection();
    final String collectionAlias = ThirdEyeUtils.getAliasFromCollection(collection);

    // Get the anomalies in that range
    final List<AnomalyResult> results =
        getAnomalyResults(context, config, sessionFactory, then, now);

    if (results.isEmpty() && !config.getSendZeroAnomalyEmail()) {
      LOG.info("Zero anomalies found, skipping sending email");
      return;
    }

    // Group by dimension key, then sort according to anomaly result compareTo method.
    Map<String, List<AnomalyResult>> groupedResults = new TreeMap<>();
    for (AnomalyResult result : results) {
      String dimensions = result.getDimensions();
      if (!groupedResults.containsKey(dimensions)) {
        groupedResults.put(dimensions, new ArrayList<AnomalyResult>());
      }
      groupedResults.get(dimensions).add(result);
    }
    // sort each list of anomaly results afterwards and keep track of sequence number in a new list
    Map<AnomalyResult, String> anomaliesWithLabels = new LinkedHashMap<AnomalyResult, String>();
    int counter = 1;
    for (List<AnomalyResult> resultsByDimensionKey : groupedResults.values()) {
      Collections.sort(resultsByDimensionKey);
      for (AnomalyResult result : resultsByDimensionKey) {
        anomaliesWithLabels.put(result, String.valueOf(counter));
        counter++;
      }
    }

    String chartFilePath =
        writeTimeSeriesChart(config, timeOnTimeComparisonHandler, now, then, collection,
            anomaliesWithLabels);

    // get dimensions for rendering
    List<String> dimensionNames;
    try {
      dimensionNames = client.getCollectionSchema(collection).getDimensionNames();
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }

    // get link to REST endpoints
    String anomalyEndpoint;
    String functionEndpoint;
    try {
      anomalyEndpoint =
          String.format("http://%s:%d/anomaly-results/", InetAddress.getLocalHost()
              .getCanonicalHostName(), applicationPort);
      functionEndpoint =
          String.format("http://%s:%d/anomaly-functions/", InetAddress.getLocalHost()
              .getCanonicalHostName(), applicationPort);
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }

    DateTimeZone timeZone = DateTimeZone.forID("America/Los_Angeles");
    DateFormatMethod dateFormatMethod = new DateFormatMethod(timeZone);

    // Render template - create email first so we can get embedded image string
    HtmlEmail email = new HtmlEmail();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    File chartFile = null;
    try (Writer out = new OutputStreamWriter(baos, CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/com/linkedin/thirdeye/detector/");
      freemarkerConfig.setDefaultEncoding(CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Map<String, Object> templateData = new HashMap<>();
      String metric = config.getMetric();
      String filtersJson = ThirdEyeUtils.convertMultiMapToJson(config.getFilterSet());
      String filtersJsonEncoded = URLEncoder.encode(filtersJson, "UTF-8");
      String windowUnit = config.getWindowUnit().toString();

      // templateData.put("anomalyResults", results);
      templateData.put("groupedAnomalyResults", groupedResults);
      templateData.put("anomalyCount", results.size());
      templateData.put("startTime", then.getMillis());
      templateData.put("endTime", now.getMillis());
      templateData.put("anomalyEndpoint", anomalyEndpoint);
      templateData.put("functionEndpoint", functionEndpoint);
      templateData.put("reportGenerationTimeMillis", scheduledFireTime.getTime());
      templateData.put("assignedDimensions", new AssignedDimensionsMethod(dimensionNames));
      templateData.put("dateFormat", dateFormatMethod);
      templateData.put("timeZone", timeZone);
      // http://stackoverflow.com/questions/13339445/feemarker-writing-images-to-html
      chartFile = new File(chartFilePath);
      templateData.put("embeddedChart", email.embed(chartFile));
      templateData.put("collection", collectionAlias);
      templateData.put("metric", metric);
      templateData.put("filters", filtersJsonEncoded);
      templateData.put("windowUnit", windowUnit);
      templateData.put("dashboardHost", dashboardHost);

      Template template = freemarkerConfig.getTemplate("simple-anomaly-report.ftl");
      template.process(templateData, out);

    } catch (Exception e) {
      throw new JobExecutionException(e);
    }

    // Send email
    try {
      email.setHostName(config.getSmtpHost());
      email.setSmtpPort(config.getSmtpPort());
      if (config.getSmtpUser() != null && config.getSmtpPassword() != null) {
        email.setAuthenticator(new DefaultAuthenticator(config.getSmtpUser(), config
            .getSmtpPassword()));
        email.setSSLOnConnect(true);
      }
      email.setFrom(config.getFromAddress());
      for (String toAddress : config.getToAddresses().split(",")) {
        email.addTo(toAddress);
      }
      email.setSubject(String.format("Anomaly Alert!: %d anomalies detected for %s:%s",
          results.size(), collectionAlias, config.getMetric()));
      final String html = new String(baos.toByteArray(), CHARSET);

      email.setHtmlMsg(html);
      email.send();
    } catch (Exception e) {
      throw new JobExecutionException(e);
    } finally {
      if (!FileUtils.deleteQuietly(chartFile)) {
        LOG.error("Unable to delete chart {}", chartFilePath);
      }
    }

    LOG.info("Sent email with {} anomalies! {}", results.size(), config);
  }

  private String writeTimeSeriesChart(final EmailConfiguration config,
      TimeOnTimeComparisonHandler timeOnTimeComparisonHandler, final DateTime now,
      final DateTime then, final String collection,
      final Map<AnomalyResult, String> anomaliesWithLabels) throws JobExecutionException {
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
      JFreeChart chart =
          anomalyGraphGenerator.createChart(chartData, dataGranularity, windowMillis,
              anomaliesWithLabels);
      String chartFilePath = EMAIL_REPORT_CHART_PREFIX + config.getId() + PNG;
      LOG.info("Writing chart to {}", chartFilePath);
      anomalyGraphGenerator.writeChartToFile(chart, chartFilePath);
      return chartFilePath;
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
  }

  private List<AnomalyResult> getAnomalyResults(final JobExecutionContext context,
      final EmailConfiguration config, SessionFactory sessionFactory, final DateTime start,
      final DateTime end) throws JobExecutionException {
    final List<AnomalyResult> results;
    try {
      results =
          new HibernateSessionWrapper<List<AnomalyResult>>(sessionFactory)
              .execute(new Callable<List<AnomalyResult>>() {
                @Override
                public List<AnomalyResult> call() throws Exception {
                  AnomalyResultDAO resultDAO =
                      (AnomalyResultDAO) context.getJobDetail().getJobDataMap().get(RESULT_DAO);
                  return resultDAO.findAllByTimeAndEmailId(start, end, config.getId());
                }
              });
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
    return results;
  }

  /**
   * Generate and send request to retrieve chart data. If the request window is too small, the graph
   * data retrieved has default window sizes based on time granularity and ending at the defined
   * endpoint: <br/>
   * <ul>
   * <li>DAYS: 7</li>
   * <li>HOURS: 24</li>
   * </ul>
   * @param bucketGranularity
   * @throws JobExecutionException
   */
  private TimeOnTimeComparisonResponse getData(
      TimeOnTimeComparisonHandler timeOnTimeComparisonHandler, EmailConfiguration config,
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

  private DateTime calculateGraphDataStart(DateTime start, DateTime end,
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
      LOG.info("Overriding config window size {} {} with minimum default of {}", currentUnits,
          unit, minUnits);
      start = end.minus(unit.toMillis(minUnits));
    }
    return start;

  }

  private class AssignedDimensionsMethod implements TemplateMethodModelEx {
    private static final String UNASSIGNED_DIMENSION_VALUE = "*";
    private static final String DIMENSION_VALUE_SEPARATOR = ",";
    private static final String EQUALS = "=";
    private final List<String> dimensionNames;

    public AssignedDimensionsMethod(List<String> dimensionNames) {
      this.dimensionNames = dimensionNames;
    }

    @Override
    public Object exec(@SuppressWarnings("rawtypes") List arguments) throws TemplateModelException {
      if (arguments.size() != 1) {
        throw new TemplateModelException(
            "Wrong arguments, expected single comma-separated dimension string");
      }
      TemplateScalarModel tsm = (TemplateScalarModel) arguments.get(0);
      String dimensions = tsm.getAsString();
      String[] split = dimensions.split(DIMENSION_VALUE_SEPARATOR, dimensionNames.size());
      // TODO decide what to do if split.length doesn't match up, ie schema has changed
      List<String> assignments = new ArrayList<>();
      for (int i = 0; i < split.length; i++) {
        String value = split[i];
        if (!value.equals(UNASSIGNED_DIMENSION_VALUE)) { // TODO figure out actual constant /
                                                         // rewrite function API to only
          // return assignments.
          String dimension = dimensionNames.get(i);
          assignments.add(dimension + EQUALS + value);
        }
      }
      if (assignments.isEmpty()) {
        return "ALL"; // TODO determine final message for no assigned dimensions
      } else {
        return StringUtils.join(assignments, DIMENSION_VALUE_SEPARATOR);
      }
    }
  }

  private class DateFormatMethod implements TemplateMethodModelEx {
    private final DateTimeZone TZ;
    private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

    DateFormatMethod(DateTimeZone timeZone) {
      this.TZ = timeZone;
    }

    @Override
    public Object exec(@SuppressWarnings("rawtypes") List arguments) throws TemplateModelException {
      if (arguments.size() != 1) {
        throw new TemplateModelException("Wrong arguments, expected single millisSinceEpoch");
      }
      TemplateNumberModel tnm = (TemplateNumberModel) arguments.get(0);
      if (tnm == null) {
        return null;
      }

      Long millisSinceEpoch = tnm.getAsNumber().longValue();
      DateTime date = new DateTime(millisSinceEpoch, TZ);
      return date.toString(DATE_PATTERN);
    }
  }
}
