package com.linkedin.thirdeye.reporting.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.time.DateUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.client.CachedThirdEyeClientConfig;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeMetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeRawResponse;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeRequest.ThirdEyeRequestBuilder;
import com.linkedin.thirdeye.client.factory.DefaultThirdEyeClientFactory;
import com.linkedin.thirdeye.reporting.api.anomaly.AnomalyReportGeneratorApi;
import com.linkedin.thirdeye.reporting.api.anomaly.AnomalyReportTable;
import com.linkedin.thirdeye.reporting.util.DimensionKeyUtils;
import com.linkedin.thirdeye.reporting.util.SegmentDescriptorUtils;
import com.linkedin.thirdeye.reporting.util.ThirdeyeUrlUtils;

public class ReportGenerator implements Job {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReportGenerator.class);
  private static DecimalFormat DOUBLE_FORMAT = new DecimalFormat("0.00");
  private static int DEFAULT_AGGREGATION_GRANULARITY = 1;
  private static TimeUnit DEFAULT_AGGREGATION_UNIT = TimeUnit.HOURS;
  private static final int DEFAULT_CACHE_EXPIRATION_MINUTES = 5;
  private static final String RATIO_METRIC_PREFIX = "RATIO";

  private String collection;
  private String serverUri;
  private String dashboardUri;
  private ReportConfig reportConfig;
  private String templatePath;
  private StarTreeConfig starTreeConfig;
  private ThirdEyeClient thirdeyeClient;

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    try {
      generateReports(context.getJobDetail().getDescription(),
          context.getJobDetail().getJobDataMap());
    } catch (Exception e) {
      LOGGER.error("Generate Reports failed", e);
    }
  }

  private void createThirdeyeClient() throws URISyntaxException {
    CachedThirdEyeClientConfig thirdEyeClientConfig = new CachedThirdEyeClientConfig();
    thirdEyeClientConfig.setExpirationTime(DEFAULT_CACHE_EXPIRATION_MINUTES);
    thirdEyeClientConfig.setExpirationUnit(TimeUnit.MINUTES);
    thirdEyeClientConfig.setExpireAfterAccess(false);
    thirdEyeClientConfig.setUseCacheForExecuteMethod(true);
    thirdeyeClient = new DefaultThirdEyeClientFactory(thirdEyeClientConfig)
        .getClient(getThirdeyeHost(), getThirdeyePort());
  }

  /**
   * This method will generate the reports according to config
   * @param jobDescription : description set in Quartz Job
   * @param jobDataMap : data map from Quartz Job
   * @throws Exception
   */
  private void generateReports(String jobDescription, JobDataMap jobDataMap) throws Exception {

    LOGGER.info("Executing {}", jobDescription);
    try {
      File reportConfigFile = new File(jobDataMap.get(ReportConstants.CONFIG_FILE_KEY).toString());

      // get report and schedule config
      reportConfig = ReportConfig.decode(new FileInputStream(reportConfigFile));
      templatePath = jobDataMap.getString(ReportConstants.TEMPLATE_PATH_KEY).toString();
      collection = reportConfig.getCollection();
      serverUri = jobDataMap.get(ReportConstants.SERVER_URI_KEY).toString();
      dashboardUri = jobDataMap.getString(ReportConstants.DASHBOARD_URI_KEY.toString());

      createThirdeyeClient();
      starTreeConfig = thirdeyeClient.getStarTreeConfig(collection);

      ScheduleSpec scheduleSpec = reportConfig.getSchedules().get(jobDescription);
      int aggregationSize = scheduleSpec.getAggregationSize();
      TimeUnit aggregationUnit = scheduleSpec.getAggregationUnit();
      int lagSize = scheduleSpec.getLagSize();
      TimeUnit lagUnit = scheduleSpec.getLagUnit();

      List<Table> tables = new ArrayList<Table>();
      Map<String, AnomalyReportTable> anomalyReportTables = null;
      List<TimeRange> missingSegments = null;

      // generate report for every tableSpec
      for (TableSpec tableSpec : reportConfig.getTables()) {

        LOGGER.info("Collecting data for table {}", tableSpec);

        // get table config
        int baselineSize = tableSpec.getBaselineSize();
        TimeUnit baselineUnit = tableSpec.getBaselineUnit();

        DateTime endTime = new DateTime().minus(TimeUnit.MILLISECONDS.convert(lagSize, lagUnit));
        DateTime currentEndHour =
            new DateTime(DateUtils.truncate(new Date(endTime.getMillis()), Calendar.HOUR));
        DateTime baselineEndHour =
            currentEndHour.minus(TimeUnit.MILLISECONDS.convert(baselineSize, baselineUnit));
        DateTime currentStartHour = currentEndHour.minus(TimeUnit.MILLISECONDS.convert(
            scheduleSpec.getReportWindow() * scheduleSpec.getAggregationSize(),
            scheduleSpec.getAggregationUnit()));
        DateTime baselineStartHour = baselineEndHour.minus(TimeUnit.MILLISECONDS.convert(
            scheduleSpec.getReportWindow() * scheduleSpec.getAggregationSize(),
            scheduleSpec.getAggregationUnit()));
        reportConfig.setStartTime(
            currentStartHour.withZone(DateTimeZone.forID(reportConfig.getTimezone())));
        reportConfig.setStartTimeString(
            ReportConstants.DATE_TIME_FORMATTER.print(reportConfig.getStartTime()));
        reportConfig
            .setEndTime(currentEndHour.withZone(DateTimeZone.forID(reportConfig.getTimezone())));
        reportConfig
            .setEndTimeString(ReportConstants.DATE_TIME_FORMATTER.print(reportConfig.getEndTime()));

        // check for missing data segments in thirdeye
        missingSegments = SegmentDescriptorUtils.checkSegments(serverUri, collection,
            reportConfig.getTimezone(), reportConfig.getStartTime(), reportConfig.getEndTime(),
            baselineStartHour.withZone(DateTimeZone.forID(reportConfig.getTimezone())),
            baselineEndHour.withZone(DateTimeZone.forID(reportConfig.getTimezone())));
        if (missingSegments != null && missingSegments.size() != 0) {
          ReportEmailSender.sendErrorReport(missingSegments, scheduleSpec, reportConfig);
        }

        // generate thirdeye url
        URL thirdeyeUri = ThirdeyeUrlUtils.getThirdeyeUri(reportConfig.getDashboardUri(),
            collection, scheduleSpec, tableSpec,
            baselineEndHour.minus(TimeUnit.MILLISECONDS.convert(DEFAULT_AGGREGATION_GRANULARITY,
                DEFAULT_AGGREGATION_UNIT)).getMillis(),
            currentEndHour.minus(TimeUnit.MILLISECONDS.convert(DEFAULT_AGGREGATION_GRANULARITY,
                DEFAULT_AGGREGATION_UNIT)).getMillis());
        LOGGER.info("Generating Thirdeye URL {}", thirdeyeUri);

        Map<String, String> dimensionValues = DimensionKeyUtils.createDimensionValues(tableSpec);
        String groupBy = tableSpec.getGroupBy();
        LOGGER.info("Generated dimension values");
        Map<String, List<ReportRow>> metricTableRows = new HashMap<>();
        List<TableReportRow> tableReportRows = new ArrayList<>();

        for (int i = 0; i < scheduleSpec.getReportWindow(); i++) {
          currentEndHour = currentStartHour
              .plus(TimeUnit.MILLISECONDS.convert(aggregationSize, aggregationUnit));
          baselineEndHour = baselineStartHour
              .plus(TimeUnit.MILLISECONDS.convert(aggregationSize, aggregationUnit));

          // generate report for every metric
          for (String metric : tableSpec.getMetrics()) {
            LOGGER.info("Metric : " + metric);

            ThirdEyeMetricFunction metricFunction = new ThirdEyeMetricFunction(
                starTreeConfig.getTime().getBucket(), Collections.singletonList(metric));
            ThirdEyeRequest currentReq = new ThirdEyeRequestBuilder().setCollection(collection)
                .setMetricFunction(metricFunction).setStartTimeInclusive(currentStartHour)
                .setEndTime(currentEndHour).setDimensionValues(dimensionValues).setGroupBy(groupBy)
                .build();
            ThirdEyeRequest baselineReq = new ThirdEyeRequestBuilder().setCollection(collection)
                .setMetricFunction(metricFunction).setStartTimeInclusive(baselineStartHour)
                .setEndTime(baselineEndHour).setDimensionValues(dimensionValues).setGroupBy(groupBy)
                .build();
            LOGGER.info("Current req: {}", currentReq);
            LOGGER.info("Baseline req: {}", baselineReq);

            ThirdEyeRawResponse currentQueryResult = thirdeyeClient.getRawResponse(currentReq);
            ThirdEyeRawResponse baselineQueryResult = thirdeyeClient.getRawResponse(baselineReq);

            LOGGER.info("Applying filters");
            // apply filters
            Map<DimensionKey, Number> currentReportOutput = applyFilters(currentQueryResult,
                tableSpec, metric, currentStartHour, currentEndHour);
            Map<DimensionKey, Number> baselineReportOutput = applyFilters(baselineQueryResult,
                tableSpec, metric, baselineStartHour, baselineEndHour);

            LOGGER.info("Creating report rows");
            // create report rows
            List<ReportRow> reportRows;
            if (tableSpec.getGroupBy() != null) {
              reportRows = createRow(currentStartHour, currentEndHour, baselineStartHour,
                  baselineEndHour, currentReportOutput, baselineReportOutput, metric,
                  currentQueryResult.getDimensions().indexOf(tableSpec.getGroupBy()));
            } else {
              reportRows = createRow(currentStartHour, currentEndHour, baselineStartHour,
                  baselineEndHour, currentReportOutput, baselineReportOutput, metric, -1);
            }

            if (metricTableRows.get(metric) == null) {
              metricTableRows.put(metric, reportRows);
            } else {
              metricTableRows.get(metric).addAll(reportRows);
            }
          }
          currentStartHour = currentEndHour;
          baselineStartHour = baselineEndHour;
        }

        LOGGER.info("Calculating summary row");
        // compute Total row
        calculateSummaryRow(metricTableRows);
        LOGGER.info("Calculating ratio summary row");
        calculateRatioTotal(metricTableRows);
        tableReportRows = getGroupBy(metricTableRows, tableSpec.getMetrics());

        // find anomalies
        if (scheduleSpec.isFindAnomalies() && reportConfig.getDbconfig() != null) {
          if (anomalyReportTables == null) {
            anomalyReportTables = new HashMap<String, AnomalyReportTable>();
          }
          LOGGER.info("Finding anomalies...");
          AnomalyReportGeneratorApi anomalyReportGeneratorApi = new AnomalyReportGeneratorApi();
          anomalyReportTables
              .putAll(anomalyReportGeneratorApi.getAnomalies(reportConfig, tableSpec, collection));
        }

        Table table = new Table(tableReportRows, tableSpec, thirdeyeUri);
        tables.add(table);
      }

      LOGGER.info("Applying aliases");
      // apply alias
      AliasSpec.alias(reportConfig, tables, anomalyReportTables);

      LOGGER.info("Creating data model");
      // send data to email sender
      ReportEmailDataModel reportObjects = new ReportEmailDataModel(reportConfig, tables,
          anomalyReportTables, missingSegments, scheduleSpec, new ReportEmailCssSpec());
      ReportEmailSender reportEmailSender = new ReportEmailSender(reportObjects, templatePath);
      reportEmailSender.emailReport();

    } catch (IOException e) {
      LOGGER.error("Job failed", e);
    }
  }

  private List<TableReportRow> getGroupBy(Map<String, List<ReportRow>> metricTableRows,
      List<String> metrics) {
    List<TableReportRow> tableReportRows = new ArrayList<TableReportRow>();
    for (String metric : metrics) {
      List<ReportRow> rows = metricTableRows.get(metric);
      for (ReportRow row : rows) {
        String startTime;
        if (row.getDimension().equals("Total")) {
          startTime = "Total";
        } else {
          startTime = ReportConstants.HOUR_FORMATTER
              .print(row.getCurrentStart().withZone(DateTimeZone.forID(reportConfig.getTimezone())))
              .toLowerCase();
        }
        TableReportRow tableReportRow = new TableReportRow();
        tableReportRow.setGroupByDimensions(new GroupBy(startTime, row.getDimension()));
        tableReportRows.add(tableReportRow);
      }
      break;
    }
    for (String metric : metrics) {
      List<ReportRow> metricRows = metricTableRows.get(metric);
      for (int i = 0; i < metricRows.size(); i++) {
        tableReportRows.get(i).getRows().add(metricRows.get(i));
      }
    }
    return tableReportRows;
  }

  private void calculateRatioTotal(Map<String, List<ReportRow>> metricTableRows) {

    for (Entry<String, List<ReportRow>> entry : metricTableRows.entrySet()) {
      List<ReportRow> tableRows = entry.getValue();
      String metric = entry.getKey();
      if (tableRows.size() == 0) {
        throw new IllegalStateException("No rows found for metric " + metric);
      }
      if (metric.startsWith(RATIO_METRIC_PREFIX)) {
        String[] tokens = metric.split("[\\W]");
        String metric1 = tokens[1];
        String metric2 = tokens[2];

        List<ReportRow> metric1Rows = metricTableRows.get(metric1);
        List<ReportRow> metric2Rows = metricTableRows.get(metric2);
        List<ReportRow> metricRows = metricTableRows.get(metric);
        if (metric1Rows == null || metric2Rows == null) {
          throw new IllegalStateException("Ratio metrics not present in report");
        }
        ReportRow lastMetric1Row = metric1Rows.get(metric1Rows.size() - 1);
        ReportRow lastMetric2Row = metric2Rows.get(metric2Rows.size() - 1);
        ReportRow lastMetricRow = metricRows.get(metricRows.size() - 1);
        lastMetricRow.setBaseline(lastMetric1Row.getBaseline().doubleValue()
            / lastMetric2Row.getBaseline().doubleValue());
        lastMetricRow.setCurrent(
            lastMetric1Row.getCurrent().doubleValue() / lastMetric2Row.getCurrent().doubleValue());
      }
    }
  }

  /**
   * Method to compute Total row from given list of rows
   */
  private void calculateSummaryRow(Map<String, List<ReportRow>> metricTableRows) {

    for (Entry<String, List<ReportRow>> entry : metricTableRows.entrySet()) {
      List<ReportRow> tableRows = entry.getValue();
      String metric = entry.getKey();
      if (tableRows.size() == 0) {
        throw new IllegalStateException("No rows found for metric " + metric);
      }

      ReportRow summary = new ReportRow(tableRows.get(0));

      int metricPosition = 0;
      List<MetricSpec> metricSpecs = getMetricSpecs(metric);
      MetricTimeSeries currentSeries =
          new MetricTimeSeries(MetricSchema.fromMetricSpecs(metricSpecs));
      MetricTimeSeries baselineSeries =
          new MetricTimeSeries(MetricSchema.fromMetricSpecs(metricSpecs));

      for (int i = 0; i < tableRows.size(); i++) {
        currentSeries.set(i, metric, tableRows.get(i).getCurrent());
        baselineSeries.set(i, metric, tableRows.get(i).getBaseline());
      }
      summary.setCurrent(currentSeries.getMetricSums()[metricPosition]);
      summary.setBaseline(baselineSeries.getMetricSums()[metricPosition]);

      summary.setRatio(DOUBLE_FORMAT
          .format((summary.getCurrent().doubleValue() - summary.getBaseline().doubleValue())
              / (summary.getBaseline().doubleValue()) * 100));
      summary.setDimension("Total");
      tableRows.add(summary);
    }
  }

  /**
   * Create report rows
   */
  private List<ReportRow> createRow(DateTime currentStartHour, DateTime currentEndHour,
      DateTime baselineStartHour, DateTime baselineEndHour,
      Map<DimensionKey, Number> currentReportOutput, Map<DimensionKey, Number> baselineReportOutput,
      String metric, int dimensionPosition) {

    List<ReportRow> reportRows = new ArrayList<ReportRow>();
    for (Entry<DimensionKey, Number> entry : currentReportOutput.entrySet()) {
      String dimension;
      if (dimensionPosition == -1) {
        dimension = "-";
      } else {
        dimension = entry.getKey().getDimensionValues()[dimensionPosition];
      }

      ReportRow row =
          new ReportRow(currentStartHour, currentEndHour, baselineStartHour, baselineEndHour,
              entry.getKey(), dimension, metric, baselineReportOutput.get(entry.getKey()) == null
                  ? 0 : baselineReportOutput.get(entry.getKey()),
              entry.getValue());
      row.setRatio(
          DOUBLE_FORMAT.format((row.getCurrent().doubleValue() - row.getBaseline().doubleValue())
              / (row.getBaseline().doubleValue()) * 100));
      reportRows.add(row);
    }
    return reportRows;
  }

  /**
   * Apply filters to thirdeye raw sql response
   * @param queryResult : Thirdeye raw sql response
   * @return
   */
  private Map<DimensionKey, Number> applyFilters(ThirdEyeRawResponse queryResult,
      TableSpec tableSpec, String metric, DateTime startTime, DateTime endTime)
          throws JsonParseException, JsonMappingException, IOException {

    Map<DimensionKey, Number> reportOutput = new HashMap<DimensionKey, Number>();

    // apply fixed dimensions filter
    String[] filteredDimension = new String[queryResult.getDimensions().size()];
    for (int i = 0; i < queryResult.getDimensions().size(); i++) {
      if (tableSpec.getFixedDimensions() != null
          && tableSpec.getFixedDimensions().containsKey(queryResult.getDimensions().get(i))) {
        filteredDimension[i] =
            tableSpec.getFixedDimensions().get(queryResult.getDimensions().get(i));
      } else {
        filteredDimension[i] = "*";
      }
    }

    // get dimensions to include in group by filter
    Set<String> includeDimensions = new HashSet<>();
    Set<String> excludeDimensions = new HashSet<String>();
    int dimensionPosition = -1;
    if (tableSpec.getGroupBy() != null) {
      dimensionPosition = queryResult.getDimensions().indexOf(tableSpec.getGroupBy());
      // include specific dimension values
      if (tableSpec.getFilter() != null && tableSpec.getFilter().getIncludeDimensions() != null) {
        for (String includeDimension : tableSpec.getFilter().getIncludeDimensions()) {
          filteredDimension[dimensionPosition] = includeDimension;
          includeDimensions.add(DimensionKeyUtils.createQueryKey(filteredDimension));
        }
        for (Entry<String, Map<String, Number[]>> entry : queryResult.getData().entrySet()) {
          if (!includeDimensions.contains(entry.getKey())) {
            excludeDimensions.add(entry.getKey());
          }
        }
        for (String excludeDimension : excludeDimensions) {
          queryResult.getData().remove(excludeDimension);
        }
      }
    }

    // get metric position
    int metricPosition = 0;
    List<MetricSpec> metricSpecs = getMetricSpecs(metric);
    MetricSchema schema = MetricSchema.fromMetricSpecs(metricSpecs);

    for (Entry<String, Map<String, Number[]>> entry : queryResult.getData().entrySet()) {
      DimensionKey dimensionKey = DimensionKeyUtils.createDimensionKey(entry.getKey());
      Map<String, Number[]> data = entry.getValue();
      MetricTimeSeries series = new MetricTimeSeries(schema);
      for (Entry<String, Number[]> dataEntry : data.entrySet()) {
        if (!String.valueOf(endTime.getMillis()).equals(dataEntry.getKey())) {
          int position = queryResult.getMetrics().indexOf(metric);
          series.set(Long.valueOf(dataEntry.getKey()), metric, dataEntry.getValue()[position]);
        }
      }
      Number[] seriesSums = series.getMetricSums();
      reportOutput.put(dimensionKey, seriesSums[metricPosition]);
    }
    return reportOutput;
  }

  private List<MetricSpec> getMetricSpecs(String metric) {
    List<MetricSpec> metricSpecs = new ArrayList<MetricSpec>();
    MetricSpec metricSpec = new MetricSpec(metric, MetricType.DOUBLE);
    for (MetricSpec spec : starTreeConfig.getMetrics()) {
      if (spec.getName().equals(metric)) {
        metricSpec = new MetricSpec(metric, MetricType.LONG);
        break;
      }
    }
    metricSpecs.add(metricSpec);
    return metricSpecs;
  }

  private String getThirdeyeHost() throws URISyntaxException {
    URI uri = new URI(serverUri);
    return uri.getHost();
  }

  private int getThirdeyePort() throws URISyntaxException {
    URI uri = new URI(serverUri);
    return uri.getPort();
  }

}
