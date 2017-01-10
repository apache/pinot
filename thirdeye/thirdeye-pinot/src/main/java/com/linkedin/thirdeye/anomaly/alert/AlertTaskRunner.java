package com.linkedin.thirdeye.anomaly.alert;

import com.linkedin.thirdeye.anomaly.alert.template.pojo.MetricDimensionReport;
import com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper;
import com.linkedin.thirdeye.anomaly.alert.util.EmailHelper;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.dashboard.views.contributor.ContributorViewResponse;
import com.linkedin.thirdeye.datalayer.bao.AlertConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EmailConfigurationManager;
import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import com.linkedin.thirdeye.detector.email.filter.AlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterType;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
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
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.EmailConfigurationDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.TemplateMethodModelEx;
import freemarker.template.TemplateModelException;
import freemarker.template.TemplateNumberModel;

import static com.linkedin.thirdeye.anomaly.alert.util.AlertFilterHelper.FILTER_TYPE_KEY;

public class AlertTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(AlertTaskRunner.class);
  private static final String DIMENSION_VALUE_SEPARATOR = ", ";
  private static final String EQUALS = "=";

  private static final DAORegistry daoRegistry = DAORegistry.getInstance();

  private final MergedAnomalyResultManager anomalyMergedResultDAO;
  private final EmailConfigurationManager emailConfigurationDAO;
  private final AlertConfigManager alertConfigManager;

  private EmailConfigurationDTO alertConfig;
  private AlertConfigDTO alertConfigDTO;
  private DateTime windowStart;
  private DateTime windowEnd;
  private ThirdEyeAnomalyConfiguration thirdeyeConfig;

  public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone("America/Los_Angeles");
  public static final String CHARSET = "UTF-8";
  public static final String DECIMAL_FORMATTER = "%+.1f";
  public static final String OVER_ALL = "OverAll";

  public AlertTaskRunner() {
    anomalyMergedResultDAO = daoRegistry.getMergedAnomalyResultDAO();
    emailConfigurationDAO = daoRegistry.getEmailConfigurationDAO();
    alertConfigManager = daoRegistry.getAlertConfigDAO();
  }

  @Override
  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    AlertTaskInfo alertTaskInfo = (AlertTaskInfo) taskInfo;
    List<TaskResult> taskResult = new ArrayList<>();
    alertConfig = alertTaskInfo.getAlertConfig();
    alertConfigDTO = alertTaskInfo.getAlertConfigDTO();
    windowStart = alertTaskInfo.getWindowStartTime();
    windowEnd = alertTaskInfo.getWindowEndTime();
    thirdeyeConfig = taskContext.getThirdEyeAnomalyConfiguration();

    try {
      LOG.info("Begin executing task {}", taskInfo);
      runTask();
    } catch (Exception t) {
      LOG.error("Task failed with exception:", t);
      sendFailureEmail(t);
      // Let task driver mark this task failed
      throw t;
    }
    return taskResult;
  }

  // TODO : separate code path for new vs old alert config !
  private void runTask() throws Exception {
    LOG.info("Starting email report {}", alertConfig.getId());

    final String collection = alertConfig.getCollection();

    // Get the anomalies in that range
    final List<MergedAnomalyResultDTO> allResults = anomalyMergedResultDAO
        .getAllByTimeEmailIdAndNotifiedFalse(windowStart.getMillis(), windowEnd.getMillis(),
            alertConfig.getId());

    // apply filtration rule
    List<MergedAnomalyResultDTO> results = applyFiltrationRule(allResults);

    if (results.isEmpty() && !alertConfig.isSendZeroAnomalyEmail()) {
      LOG.info("Zero anomalies found, skipping sending email");
      return;
    }

    // Group by dimension key, then sort according to anomaly result compareTo method.
    Map<DimensionMap, List<MergedAnomalyResultDTO>> groupedResults = new TreeMap<>();
    for (MergedAnomalyResultDTO result : results) {
      DimensionMap dimensions = result.getDimensions();
      if (!groupedResults.containsKey(dimensions)) {
        groupedResults.put(dimensions, new ArrayList<>());
      }
      groupedResults.get(dimensions).add(result);
    }
    // sort each list of anomaly results afterwards
    for (List<MergedAnomalyResultDTO> resultsByExploredDimensions : groupedResults.values()) {
      Collections.sort(resultsByExploredDimensions);
    }
    sendAlertForAnomalies(collection, results, groupedResults);
    updateNotifiedStatus(results);
  }

  private List<MergedAnomalyResultDTO> applyFiltrationRule(List<MergedAnomalyResultDTO> results) {
    if (results.size() == 0) {
      return results;
    }

    // Function ID to Alert Filter
    Map<Long, AlertFilter> functionAlertFilter = new HashMap<>();

    List<MergedAnomalyResultDTO> qualifiedAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO result : results) {
      // Lazy initiates alert filter for anomalies of the same anomaly function
      AnomalyFunctionBean anomalyFunctionSpec = result.getFunction();
      long functionId = anomalyFunctionSpec.getId();
      AlertFilter alertFilter = functionAlertFilter.get(functionId);
      if (alertFilter == null) {
        // Get filtration rule from anomaly function configuration
        alertFilter = initiateAlertFilter(anomalyFunctionSpec);
        functionAlertFilter.put(functionId, alertFilter);
        LOG.info("Using filter {} for anomaly function {} (dataset: {}, metric: {})", alertFilter,
            functionId, anomalyFunctionSpec.getCollection(), anomalyFunctionSpec.getMetric());
      }
      if (alertFilter.isQualified(result)) {
        qualifiedAnomalies.add(result);
      }
    }

    LOG.info(
        "Found [{}] anomalies qualified to alert after applying filtration rule on [{}] anomalies",
        qualifiedAnomalies.size(), results.size());
    return qualifiedAnomalies;
  }

  /**
   * Initiates an alert filter for the given anomaly function.
   *
   * @param anomalyFunctionSpec the anomaly function that contains the alert filter spec.
   *
   * @return the alert filter specified by the alert filter spec or a dummy filter if the function
   * does not have an alert filter spec or this method fails to initiates an alert filter from the
   * spec.
   */
  private AlertFilter initiateAlertFilter(AnomalyFunctionBean anomalyFunctionSpec) {
    Map<String, String> alertFilterInfo = anomalyFunctionSpec.getAlertFilter();
    if (alertFilterInfo == null) {
      alertFilterInfo = Collections.emptyMap();
    }
    AlertFilter alertFilter;
    if (alertFilterInfo.containsKey(FILTER_TYPE_KEY)) {
      AlertFilterType type = AlertFilterType.valueOf(alertFilterInfo.get(FILTER_TYPE_KEY).toUpperCase());
      alertFilter = AlertFilterHelper.getAlertFilter(type);
      alertFilter.setParameters(alertFilterInfo);
    } else {
      // Every anomaly triggers an alert by default
      alertFilter = AlertFilterHelper.getAlertFilter(AlertFilterType.DUMMY);
    }
    return alertFilter;
  }

  private void sendAlertForAnomalies(String collectionAlias, List<MergedAnomalyResultDTO> results,
      Map<DimensionMap, List<MergedAnomalyResultDTO>> groupedResults)
      throws JobExecutionException {

    long anomalyStartMillis = 0;
    long anomalyEndMillis = 0;
    int anomalyResultSize = 0;
    if (CollectionUtils.isNotEmpty(results)) {
      anomalyResultSize = results.size();
      anomalyStartMillis = results.get(0).getStartTime();
      anomalyEndMillis = results.get(0).getEndTime();
      for (MergedAnomalyResultDTO mergedAnomalyResultDTO : results) {
        if (mergedAnomalyResultDTO.getStartTime() < anomalyStartMillis) {
          anomalyStartMillis = mergedAnomalyResultDTO.getStartTime();
        }
        if (mergedAnomalyResultDTO.getEndTime() > anomalyEndMillis) {
          anomalyEndMillis = mergedAnomalyResultDTO.getEndTime();
        }
      }
    }

    DateTimeZone timeZone = DateTimeZone.forTimeZone(DEFAULT_TIME_ZONE);
    DateFormatMethod dateFormatMethod = new DateFormatMethod(timeZone);

    HtmlEmail email = new HtmlEmail();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (Writer out = new OutputStreamWriter(baos, CHARSET)) {
      Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_21);
      freemarkerConfig.setClassForTemplateLoading(getClass(), "/com/linkedin/thirdeye/detector/");
      freemarkerConfig.setDefaultEncoding(CHARSET);
      freemarkerConfig.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
      Map<String, Object> templateData = new HashMap<>();

      String metric = alertConfig.getMetric();
      String windowUnit = alertConfig.getWindowUnit().toString();
      templateData.put("groupedAnomalyResults", convertToStringKeyBasedMap(groupedResults));
      templateData.put("anomalyCount", anomalyResultSize);
      templateData.put("startTime", anomalyStartMillis);
      templateData.put("endTime", anomalyEndMillis);
      templateData.put("reportGenerationTimeMillis", System.currentTimeMillis());
      templateData.put("dateFormat", dateFormatMethod);
      templateData.put("timeZone", timeZone);
      templateData.put("collection", collectionAlias);
      templateData.put("metric", metric);
      templateData.put("windowUnit", windowUnit);
      templateData.put("dashboardHost", thirdeyeConfig.getDashboardHost());

      if (alertConfig.isReportEnabled() & alertConfig.getDimensions() != null) {
        long reportStartTs = 0;
        List<MetricDimensionReport> metricDimensionValueReports;
        List<ContributorViewResponse> reports = new ArrayList<>();
        for (String dimension : alertConfig.getDimensions()) {
          ContributorViewResponse report = EmailHelper
              .getContributorData(collectionAlias, alertConfig.getMetric(), Arrays.asList(dimension));
          if(report != null) {
            reports.add(report);
          }
        }
        reportStartTs = reports.get(0).getTimeBuckets().get(0).getCurrentStart();
        metricDimensionValueReports = getDimensionReportList(reports);
        templateData.put("metricDimensionValueReports", metricDimensionValueReports);
        templateData.put("reportStartDateTime", reportStartTs);
      }

      Template template = freemarkerConfig.getTemplate("anomaly-report.ftl");
      template.process(templateData, out);
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }

    // Send email
    try {
      String alertEmailSubject;
      if (results.size() > 0) {
      String anomalyString = (results.size() == 1) ? "anomaly" : "anomalies";
        alertEmailSubject = String
            .format("Thirdeye: %s: %s - %d %s detected", alertConfig.getMetric(), collectionAlias,
                results.size(), anomalyString);
      } else {
        alertEmailSubject = String
            .format("Thirdeye data report : %s: %s", alertConfig.getMetric(), collectionAlias);
      }

      String alertEmailHtml = new String(baos.toByteArray(), CHARSET);
      EmailHelper.sendEmailWithHtml(email, thirdeyeConfig.getSmtpConfiguration(), alertEmailSubject,
          alertEmailHtml, alertConfig.getFromAddress(), alertConfig.getToAddresses());
    } catch (Exception e) {
      throw new JobExecutionException(e);
    }

    // once email is sent, update the last merged anomaly id as watermark in email config
    long anomalyId = 0;
    for (MergedAnomalyResultDTO anomalyResultDTO : results) {
      if (anomalyResultDTO.getId() > anomalyId) {
        anomalyId = anomalyResultDTO.getId();
      }
    }
    alertConfig.setLastNotifiedAnomalyId(anomalyId);
    emailConfigurationDAO.update(alertConfig);

    LOG.info("Sent email with {} anomalies! {}", results.size(), alertConfig);
  }

  // TODO: push custom report related methods to a ReportHelper util just like EmailHelper
  // report list metric vs groupByKey vs dimensionVal vs timeBucket vs values
  private static List<MetricDimensionReport> getDimensionReportList(
      List<ContributorViewResponse> reports) {
    List<MetricDimensionReport> ultimateResult = new ArrayList<>();
    for (ContributorViewResponse report : reports) {
      MetricDimensionReport metricDimensionReport = new MetricDimensionReport();
      String metric = report.getMetrics().get(0);
      String groupByDimension = report.getDimensions().get(0);
      metricDimensionReport.setMetricName(metric);
      metricDimensionReport.setDimensionName(groupByDimension);
      int valIndex =
          report.getResponseData().getSchema().getColumnsToIndexMapping().get("percentageChange");
      int dimensionIndex =
          report.getResponseData().getSchema().getColumnsToIndexMapping().get("dimensionValue");

      int numDimensions = report.getDimensionValuesMap().get(groupByDimension).size();
      int numBuckets = report.getTimeBuckets().size();

      // this is dimension vs timeBucketValue map, this should be sorted based on first bucket value
      Map<String, Map<String, String>> dimensionValueMap = new LinkedHashMap<>();

      // Lets populate 'OverAll' contribution
      Map<String, String> overAllValueMap = new LinkedHashMap<>();
      populateOverAllValuesMap(report, overAllValueMap);
      dimensionValueMap.put(OVER_ALL, overAllValueMap);

      Map<String, Map<String, String>> dimensionValueMapUnordered = new HashMap<>();

      for (int p = 0; p < numDimensions; p++) {
        if (p == 0) {
          metricDimensionReport
              .setCurrentStartTime(report.getTimeBuckets().get(0).getCurrentStart());
          metricDimensionReport
              .setCurrentEndTime(report.getTimeBuckets().get(numBuckets - 1).getCurrentEnd());
          metricDimensionReport
              .setBaselineStartTime(report.getTimeBuckets().get(0).getBaselineStart());
          metricDimensionReport
              .setBaselineEndTime(report.getTimeBuckets().get(numBuckets - 1).getBaselineEnd());
        }

        // valueMap is timeBucket vs value map
        LinkedHashMap<String, String> valueMap = new LinkedHashMap<>();
        String currentDimension = "";
        for (int q = 0; q < numBuckets; q++) {
          int index = p * numBuckets + q;
          currentDimension = report.getResponseData().getResponseData().get(index)[dimensionIndex];
          valueMap.put(String.valueOf(report.getTimeBuckets().get(q).getCurrentStart()), String
              .format(DECIMAL_FORMATTER,
                  Double.valueOf(report.getResponseData().getResponseData().get(index)[valIndex])));
        }
        dimensionValueMapUnordered.put(currentDimension, valueMap);
      }

      orderDimensionValueMap(dimensionValueMapUnordered, dimensionValueMap,
          report.getDimensionValuesMap().get(groupByDimension));
      metricDimensionReport.setSubDimensionValueMap(dimensionValueMap);
      populateShareAndTotalMap(report, metricDimensionReport, metric, groupByDimension);
      ultimateResult.add(metricDimensionReport);
    }
    return ultimateResult;
  }

  private static void orderDimensionValueMap(Map<String, Map<String, String>> src,
      Map<String, Map<String, String>> target, List<String> orderedKeys) {
    for (String key : orderedKeys) {
      target.put(key, src.get(key));
    }
  }

  private static void populateShareAndTotalMap(ContributorViewResponse report,
      MetricDimensionReport metricDimensionReport, String metric, String dimension) {
    Map<String, Double> currentValueMap =
        report.getCurrentTotalMapPerDimensionValue().get(metric).get(dimension);
    Map<String, Double> baselineValueMap =
        report.getBaselineTotalMapPerDimensionValue().get(metric).get(dimension);
    Map<String, String> shareMap = new HashMap<>();
    Map<String, String> totalMap = new HashMap<>();
    Double totalCurrent = 0d;
    Double totalBaseline = 0d;
    for (Map.Entry<String, Double> entry : currentValueMap.entrySet()) {
      totalCurrent += entry.getValue();
    }
    for (Map.Entry<String, Double> entry : baselineValueMap.entrySet()) {
      totalBaseline += entry.getValue();
    }
    // set value for overall as a sub-dimension
    shareMap.put(OVER_ALL, "100");
    totalMap.put(OVER_ALL,
        String.format(DECIMAL_FORMATTER, computePercentage(totalBaseline, totalCurrent)));

    for (Map.Entry<String, Double> entry : currentValueMap.entrySet()) {
      String subDimension = entry.getKey();
      // Share formatter does not need sign
      shareMap.put(subDimension, String.format("%.1f", 100 * entry.getValue() / totalCurrent));

      totalMap.put(subDimension, String.format(DECIMAL_FORMATTER,
          computePercentage(baselineValueMap.get(subDimension),
              currentValueMap.get(subDimension))));
    }

    metricDimensionReport.setSubDimensionShareValueMap(shareMap);
    metricDimensionReport.setSubDimensionTotalValueMap(totalMap);
  }

  private static double computePercentage(double baseline, double current) {
    if (baseline == current) {
      return 0d;
    }
    if (baseline == 0) {
      return 100d;
    }
    return 100 * (current - baseline) / baseline;
  }

  private static void populateOverAllValuesMap(ContributorViewResponse report,
      Map<String, String> overAllValueChangeMap) {
    Map<Long, Double> timeBucketVsCurrentValueMap = new LinkedHashMap<>();
    Map<Long, Double> timeBucketVsBaselineValueMap = new LinkedHashMap<>();
    int currentValIndex =
        report.getResponseData().getSchema().getColumnsToIndexMapping().get("currentValue");
    int baselineValIndex =
        report.getResponseData().getSchema().getColumnsToIndexMapping().get("baselineValue");

    // this is dimension vs timeBucketValue map, this should be sorted based on first bucket value
    String groupByDimension = report.getDimensions().get(0);
    int numDimensions = report.getDimensionValuesMap().get(groupByDimension).size();
    int numBuckets = report.getTimeBuckets().size();

    for (int p = 0; p < numDimensions; p++) {
      for (int q = 0; q < numBuckets; q++) {
        int index = p * numBuckets + q;
        long currentTimeKey = report.getTimeBuckets().get(q).getCurrentStart();
        double currentVal =
            Double.valueOf(report.getResponseData().getResponseData().get(index)[currentValIndex]);
        double baselineVal =
            Double.valueOf(report.getResponseData().getResponseData().get(index)[baselineValIndex]);

        if (!timeBucketVsCurrentValueMap.containsKey(currentTimeKey)) {
          timeBucketVsCurrentValueMap.put(currentTimeKey, 0D);
          timeBucketVsBaselineValueMap.put(currentTimeKey, 0D);
        }
        timeBucketVsCurrentValueMap
            .put(currentTimeKey, timeBucketVsCurrentValueMap.get(currentTimeKey) + currentVal);
        timeBucketVsBaselineValueMap
            .put(currentTimeKey, timeBucketVsBaselineValueMap.get(currentTimeKey) + baselineVal);
      }
    }
    for (Map.Entry<Long, Double> entry : timeBucketVsCurrentValueMap.entrySet()) {
      Double currentTotal = timeBucketVsCurrentValueMap.get(entry.getKey());
      Double baselineTotal = timeBucketVsBaselineValueMap.get(entry.getKey());
      double percentageChange = 0d;
      if (baselineTotal != 0d) {
        percentageChange = 100 * (currentTotal - baselineTotal) / baselineTotal;
      }
      overAllValueChangeMap.put(entry.getKey().toString(),
          String.format(DECIMAL_FORMATTER,percentageChange));
    }
  }

  /**
   * Convert a map of "dimension map to merged anomalies" to a map of "human readable dimension string to merged
   * anomalies".
   *
   * The dimension map is converted as follows. Assume that we have a dimension map (in Json string):
   * {"country"="US","page_name"="front_page'}, then it is converted to this String: "country=US, page_name=front_page".
   *
   * @param groupedResults a map of dimensionMap to a group of merged anomaly results
   * @return a map of "human readable dimension string to merged anomalies"
   */
  private Map<String, List<MergedAnomalyResultDTO>> convertToStringKeyBasedMap(
      Map<DimensionMap, List<MergedAnomalyResultDTO>> groupedResults) {
    // Sorted by dimension name and value pairs
    Map<String, List<MergedAnomalyResultDTO>> freemarkerGroupedResults = new TreeMap<>();

    if (MapUtils.isNotEmpty(groupedResults)) {
      for (Map.Entry<DimensionMap, List<MergedAnomalyResultDTO>> entry : groupedResults.entrySet()) {
        DimensionMap dimensionMap = entry.getKey();
        String dimensionMapString;
        if (MapUtils.isNotEmpty(dimensionMap)) {
          StringBuilder sb = new StringBuilder();
          String dimensionValueSeparator = "";
          for (Map.Entry<String, String> dimensionMapEntry : dimensionMap.entrySet()) {
            sb.append(dimensionValueSeparator).append(dimensionMapEntry.getKey());
            sb.append(EQUALS).append(dimensionMapEntry.getValue());
            dimensionValueSeparator = DIMENSION_VALUE_SEPARATOR;
          }
          dimensionMapString = sb.toString();
        } else {
          dimensionMapString = "ALL";
        }
        freemarkerGroupedResults.put(dimensionMapString, entry.getValue());
      }
    }

    return freemarkerGroupedResults;
  }

  // TODO : deprecate this, move last notified alert id in the alertConfig
  private void updateNotifiedStatus(List<MergedAnomalyResultDTO> mergedResults) {
    for (MergedAnomalyResultDTO mergedResult : mergedResults) {
      mergedResult.setNotified(true);
      anomalyMergedResultDAO.update(mergedResult);
    }
  }

  private void sendFailureEmail(Throwable t) throws JobExecutionException {
    HtmlEmail email = new HtmlEmail();
    String collection = alertConfig.getCollection();
    String metric = alertConfig.getMetric();

    String subject = String
        .format("[ThirdEye Anomaly Detector] FAILED ALERT ID=%d (%s:%s)", alertConfig.getId(),
            collection, metric);
    String textBody = String
        .format("%s%n%nException:%s", alertConfig.toString(), ExceptionUtils.getStackTrace(t));
    try {
      EmailHelper
          .sendEmailWithTextBody(email, thirdeyeConfig.getSmtpConfiguration(), subject, textBody,
              thirdeyeConfig.getFailureFromAddress(), thirdeyeConfig.getFailureToAddress());
    } catch (EmailException e) {
      throw new JobExecutionException(e);
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
