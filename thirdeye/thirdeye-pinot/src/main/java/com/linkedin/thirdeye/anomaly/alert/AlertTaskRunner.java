package com.linkedin.thirdeye.anomaly.alert;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonHandler;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.util.ThirdEyeUtils;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModelException;
import freemarker.template.TemplateNumberModel;
import freemarker.template.TemplateScalarModel;

public class AlertTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(AlertTaskRunner.class);
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE =
      ThirdEyeCacheRegistry.getInstance();

  private MergedAnomalyResultManager anomalyMergedResultDAO;
  private EmailConfigurationDTO alertConfig;
  private DateTime windowStart;
  private DateTime windowEnd;
  private ThirdEyeAnomalyConfiguration thirdeyeConfig;

  private QueryCache queryCache;
  private TimeOnTimeComparisonHandler timeOnTimeComparisonHandler;


  public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone("America/Los_Angeles");
  public static final String CHARSET = "UTF-8";

  public AlertTaskRunner() {
    queryCache = CACHE_REGISTRY_INSTANCE.getQueryCache();
    timeOnTimeComparisonHandler = new TimeOnTimeComparisonHandler(queryCache);
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    AlertTaskInfo alertTaskInfo = (AlertTaskInfo) taskInfo;
    List<TaskResult> taskResult = new ArrayList<>();
    anomalyMergedResultDAO = taskContext.getMergedResultDAO();
    alertConfig = alertTaskInfo.getAlertConfig();
    windowStart = alertTaskInfo.getWindowStartTime();
    windowEnd = alertTaskInfo.getWindowEndTime();
    thirdeyeConfig = taskContext.getThirdEyeAnomalyConfiguration();

    try {
      LOG.info("Begin executing task {}", taskInfo);
      run();
    } catch (Throwable t) {
      LOG.error("Job failed with exception:", t);
      sendFailureEmail(t);
    }

    return taskResult;
  }

  private void run() throws Exception {
    LOG.info("Starting email report {}", alertConfig.getId());

    ThirdEyeClient client = queryCache.getClient();

    final String collection = alertConfig.getCollection();
    final String collectionAlias = ThirdEyeUtils.getAliasFromCollection(collection);

    // Get the anomalies in that range
    final List<MergedAnomalyResultDTO> results = anomalyMergedResultDAO.getAllByTimeEmailIdAndNotifiedFalse(windowStart.getMillis(), windowEnd.getMillis(), alertConfig.getId());

    if (results.isEmpty() && !alertConfig.getSendZeroAnomalyEmail()) {
      LOG.info("Zero anomalies found, skipping sending email");
      return;
    }

    // Group by dimension key, then sort according to anomaly result compareTo method.
    Map<String, List<MergedAnomalyResultDTO>> groupedResults = new TreeMap<>();
    for (MergedAnomalyResultDTO result : results) {
      String dimensions = result.getDimensions();
      if (!groupedResults.containsKey(dimensions)) {
        groupedResults.put(dimensions, new ArrayList<>());
      }
      groupedResults.get(dimensions).add(result);
    }
    // sort each list of anomaly results afterwards and keep track of sequence number in a new list
    Map<MergedAnomalyResultDTO, String> anomaliesWithLabels = new LinkedHashMap<>();
    int counter = 1;
    for (List<MergedAnomalyResultDTO> resultsByDimensionKey : groupedResults.values()) {
      Collections.sort(resultsByDimensionKey);
      for (MergedAnomalyResultDTO result : resultsByDimensionKey) {
        anomaliesWithLabels.put(result, String.valueOf(counter));
        counter++;
      }
    }
    // TODO : clean up charts from email
    //    String chartFilePath =
    //        EmailHelper.writeTimeSeriesChart(alertConfig, timeOnTimeComparisonHandler, windowStart, windowEnd,
    //            collection, anomaliesWithLabels);

    // get dimensions for rendering
    List<String> dimensionNames;
    try {
      dimensionNames = client.getCollectionSchema(collection).getDimensionNames();
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }

    sendAlertForAnomalies(collectionAlias, results, groupedResults, dimensionNames);
    updateNotifiedStatus(results);
  }

  private void sendAlertForAnomalies(String collectionAlias, List<MergedAnomalyResultDTO> results,
      Map<String, List<MergedAnomalyResultDTO>> groupedResults, List<String> dimensionNames)
      throws JobExecutionException {

    DateTimeZone timeZone = DateTimeZone.forTimeZone(DEFAULT_TIME_ZONE);
    DateFormatMethod dateFormatMethod = new DateFormatMethod(timeZone);

    // Render template - create email first so we can get embedded image string
    HtmlEmail email = new HtmlEmail();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    //    File chartFile = null;
    try (Writer out = new OutputStreamWriter(baos, CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/com/linkedin/thirdeye/detector/");
      freemarkerConfig.setDefaultEncoding(CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Map<String, Object> templateData = new HashMap<>();
      String metric = alertConfig.getMetric();
      String filtersJson = ThirdEyeUtils.convertMultiMapToJson(alertConfig.getFilterSet());
      String filtersJsonEncoded = URLEncoder.encode(filtersJson, "UTF-8");
      String windowUnit = alertConfig.getWindowUnit().toString();
      Set<String> functionTypes = new HashSet<>();
      for (AnomalyFunctionDTO spec : alertConfig.getFunctions()) {
        functionTypes.add(spec.getType());
      }

      // templateData.put("anomalyResults", results);
      templateData.put("groupedAnomalyResults", groupedResults);
      templateData.put("anomalyCount", results.size());
      templateData.put("startTime", windowStart.getMillis());
      templateData.put("endTime", windowEnd.getMillis());
      templateData.put("reportGenerationTimeMillis", System.currentTimeMillis());
      templateData.put("assignedDimensions", new AssignedDimensionsMethod(dimensionNames));
      templateData.put("dateFormat", dateFormatMethod);
      templateData.put("timeZone", timeZone);
      // http://stackoverflow.com/questions/13339445/feemarker-writing-images-to-html
      //      chartFile = new File(chartFilePath);
      //      templateData.put("embeddedChart", email.embed(chartFile));
      templateData.put("collection", collectionAlias);
      templateData.put("metric", metric);
      templateData.put("filters", filtersJsonEncoded);
      templateData.put("windowUnit", windowUnit);
      templateData.put("dashboardHost", thirdeyeConfig.getDashboardHost());
      templateData.put("functionTypes", functionTypes.toString());

      Template template = freemarkerConfig.getTemplate("merged-anomaly-report.ftl");
      template.process(templateData, out);
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }

    // Send email
    try {
      String alertEmailSubject = String.format("Anomaly Alert!: %d anomalies detected for %s:%s",
          results.size(), collectionAlias, alertConfig.getMetric());
      String alertEmailHtml = new String(baos.toByteArray(), CHARSET);
      AlertJobUtils
          .sendEmailWithHtml(email, thirdeyeConfig.getSmtpConfiguration(), alertEmailSubject, alertEmailHtml,
          alertConfig.getFromAddress(), alertConfig.getToAddresses());
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }
    LOG.info("Sent email with {} anomalies! {}", results.size(), alertConfig);
  }

  private void updateNotifiedStatus(List<MergedAnomalyResultDTO> mergedResults) {
    for (MergedAnomalyResultDTO mergedResult : mergedResults) {
      mergedResult.setNotified(true);
      anomalyMergedResultDAO.update(mergedResult);
    }
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

  private void sendFailureEmail(Throwable t) throws JobExecutionException {

    HtmlEmail email = new HtmlEmail();
    String collection = alertConfig.getCollection();
    String metric = alertConfig.getMetric();

    String subject =
        String.format("[ThirdEye Anomaly Detector] FAILED ALERT ID=%d (%s:%s)", alertConfig.getId(), collection, metric);
    String textBody =
        String.format("%s%n%nException:%s", alertConfig.toString(), ExceptionUtils.getStackTrace(t));
    try {
      AlertJobUtils.sendEmailWithTextBody(email, thirdeyeConfig.getSmtpConfiguration(), subject, textBody,
          thirdeyeConfig.getFailureFromAddress(), thirdeyeConfig.getFailureToAddress());
    } catch (EmailException e) {
      throw new JobExecutionException(e);
    }
  }
}
