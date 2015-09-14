package com.linkedin.thirdeye.reporting.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
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

import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.reporting.api.TimeRange;
import com.linkedin.thirdeye.client.ThirdEyeRawResponse;
import com.linkedin.thirdeye.client.util.SqlUtils;
import com.linkedin.thirdeye.reporting.api.anomaly.AnomalyReportGenerator;
import com.linkedin.thirdeye.reporting.api.anomaly.AnomalyReportTable;
import com.linkedin.thirdeye.reporting.util.DimensionKeyUtils;
import com.linkedin.thirdeye.reporting.util.SegmentDescriptorUtils;
import com.linkedin.thirdeye.reporting.util.ThirdeyeAnomalyUtils;
import com.linkedin.thirdeye.reporting.util.ThirdeyeServerUtils;
import com.linkedin.thirdeye.reporting.util.ThirdeyeUrlUtils;


public class ReportGenerator implements Job{

  private static final Logger LOGGER = LoggerFactory.getLogger(ReportGenerator.class);
  private static DecimalFormat DOUBLE_FORMAT = new DecimalFormat("0.00");
  private static int DEFAULT_AGGREGATION_GRANULARITY = 1;
  private static TimeUnit DEFAULT_AGGREGATION_UNIT = TimeUnit.HOURS;

  private String collection;
  private String serverUri;
  private String dashboardUri;
  private ReportConfig reportConfig;
  private String templatePath;
  private StarTreeConfig starTreeConfig;

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    generateReports(context.getJobDetail().getDescription(), context.getJobDetail().getJobDataMap());
  }

  /**
   * This method will generate the reports according to config
   * @param jobDescription : description set in Quartz Job
   * @param jobDataMap : data map from Quartz Job
   */
  private void generateReports(String jobDescription, JobDataMap jobDataMap) {

    LOGGER.info("Executing {}", jobDescription);
    try {
      File reportConfigFile = new File(jobDataMap.get(ReportConstants.CONFIG_FILE_KEY).toString());
      // get report and schedule config
      reportConfig = ReportConfig.decode(new FileInputStream(reportConfigFile));
      templatePath = jobDataMap.getString(ReportConstants.TEMPLATE_PATH_KEY).toString();
      collection = reportConfig.getCollection();
      serverUri = jobDataMap.get(ReportConstants.SERVER_URI_KEY).toString();
      dashboardUri = jobDataMap.getString(ReportConstants.DASHBOARD_URI_KEY.toString());
      starTreeConfig = ThirdeyeServerUtils.getStarTreeConfig(serverUri, collection);
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
        DateTime currentEndHour = new DateTime(DateUtils.truncate(new Date(endTime.getMillis()), Calendar.HOUR));
        DateTime baselineEndHour = currentEndHour.minus(TimeUnit.MILLISECONDS.convert(baselineSize, baselineUnit));
        DateTime currentStartHour = currentEndHour.minus(TimeUnit.MILLISECONDS.convert(
            scheduleSpec.getReportWindow() * scheduleSpec.getAggregationSize(), scheduleSpec.getAggregationUnit()));
        DateTime baselineStartHour = baselineEndHour.minus(TimeUnit.MILLISECONDS.convert(
            scheduleSpec.getReportWindow() * scheduleSpec.getAggregationSize(), scheduleSpec.getAggregationUnit()));
        reportConfig.setStartTime(currentStartHour.withZone(DateTimeZone.forID(reportConfig.getTimezone())));
        reportConfig.setStartTimeString(ReportConstants.DATE_TIME_FORMATTER.print(reportConfig.getStartTime()));
        reportConfig.setEndTime(currentEndHour.withZone(DateTimeZone.forID(reportConfig.getTimezone())));
        reportConfig.setEndTimeString(ReportConstants.DATE_TIME_FORMATTER.print(reportConfig.getEndTime()));

        // check for missing data segments in thirdeye
        missingSegments = SegmentDescriptorUtils.checkSegments(serverUri, collection, reportConfig.getTimezone(),
            reportConfig.getStartTime(), reportConfig.getEndTime(),
            baselineStartHour.withZone(DateTimeZone.forID(reportConfig.getTimezone())),
            baselineEndHour.withZone(DateTimeZone.forID(reportConfig.getTimezone())));
        if (missingSegments !=null && missingSegments.size() != 0) {
          ReportEmailSender.sendErrorReport(missingSegments, scheduleSpec, reportConfig);
        }

        // generate thirdeye url
        URL thirdeyeUri = ThirdeyeUrlUtils.getThirdeyeUri(reportConfig.getDashboardUri(), collection, scheduleSpec, tableSpec,
            baselineEndHour.minus(TimeUnit.MILLISECONDS.convert(DEFAULT_AGGREGATION_GRANULARITY, DEFAULT_AGGREGATION_UNIT)).getMillis(),
            currentEndHour.minus(TimeUnit.MILLISECONDS.convert(DEFAULT_AGGREGATION_GRANULARITY, DEFAULT_AGGREGATION_UNIT)).getMillis());
        LOGGER.info("Generating Thirdeye URL {}", thirdeyeUri);

        Map<String, String> dimensionValues = DimensionKeyUtils.createDimensionValues(tableSpec);

        Map<String, List<ReportRow>> metricTableRows = new HashMap<>();
        List<TableReportRow> tableReportRows = new ArrayList<>();
        for (int i = 0; i < scheduleSpec.getReportWindow(); i++) {
          currentEndHour = currentStartHour.plus(TimeUnit.MILLISECONDS.convert(aggregationSize, aggregationUnit));
          baselineEndHour = baselineStartHour.plus(TimeUnit.MILLISECONDS.convert(aggregationSize, aggregationUnit));

          // generate report for every metric
          for (String metric : tableSpec.getMetrics()) {

            // sql query
            String currentSql = SqlUtils.getSql(metric, collection, currentStartHour, currentEndHour, dimensionValues);
            String baselineSql = SqlUtils.getSql(metric, collection, baselineStartHour, baselineEndHour, dimensionValues);

            ThirdEyeRawResponse currentQueryResult = ThirdeyeServerUtils.getReport(serverUri, currentSql);
            ThirdEyeRawResponse baselineQueryResult = ThirdeyeServerUtils.getReport(serverUri, baselineSql);

            // apply filters
            Map<DimensionKey, Number> currentReportOutput = applyFilters(currentQueryResult, tableSpec, metric, currentStartHour, currentEndHour);
            Map<DimensionKey, Number> baselineReportOutput = applyFilters(baselineQueryResult, tableSpec, metric, baselineStartHour, baselineEndHour);

            // create report rows
            List<ReportRow> reportRows;
            if (tableSpec.getGroupBy() != null) {
              reportRows = createRow(currentStartHour, currentEndHour, baselineStartHour, baselineEndHour,
                  currentReportOutput, baselineReportOutput, metric, currentQueryResult.getDimensions().indexOf(tableSpec.getGroupBy()));
            } else {
              reportRows = createRow(currentStartHour, currentEndHour, baselineStartHour, baselineEndHour,
                  currentReportOutput, baselineReportOutput, metric, -1);
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

        // compute Total row
        calculateSummaryRow(metricTableRows, tableSpec);
        tableReportRows = getGroupBy(metricTableRows, tableSpec.getMetrics());

        // find anomalies
        if (scheduleSpec.isFindAnomalies() && reportConfig.getDbconfig() != null) {
          if (anomalyReportTables == null ) {
            anomalyReportTables = new HashMap<String, AnomalyReportTable>();
          }
          LOGGER.info("Finding anomalies...");
          anomalyReportTables.putAll(
              ThirdeyeAnomalyUtils.getAnomalies(reportConfig, tableSpec, collection));
        }

        Table table = new Table(tableReportRows, tableSpec, thirdeyeUri);
        tables.add(table);
      }

      // apply alias
      AliasSpec.alias(reportConfig, tables, anomalyReportTables);

      // send data to email sender
      ReportEmailDataModel reportObjects = new ReportEmailDataModel(reportConfig, tables, anomalyReportTables, missingSegments, scheduleSpec, new ReportEmailCssSpec());
      ReportEmailSender reportEmailSender = new ReportEmailSender(reportObjects, templatePath);
      reportEmailSender.emailReport();

    } catch (IOException e) {
      LOGGER.error(e.toString());
    }
  }


  private List<TableReportRow> getGroupBy(Map<String, List<ReportRow>> metricTableRows, List<String> metrics) {
    List<TableReportRow> tableReportRows = new ArrayList<TableReportRow>();
    for (String metric : metrics) {
      List<ReportRow> rows = metricTableRows.get(metric);
      for (ReportRow row : rows) {
        String startTime;
        if (row.getDimension().equals("Total")) {
          startTime = "Total";
        } else {
          startTime = ReportConstants.HOUR_FORMATTER.print(row.getCurrentStart().withZone(DateTimeZone.forID(reportConfig.getTimezone()))).toLowerCase();
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

  /**
   * Method to compute Total row from given list of rows
   * @param metricTableRows
   * @param tableSpec
   */
  private void calculateSummaryRow(Map<String, List<ReportRow>> metricTableRows, TableSpec tableSpec) {
    Map<String, Integer> metricPosition = new HashMap<String, Integer>();
    for (int position = 0; position < starTreeConfig.getMetrics().size(); position++) {
      metricPosition.put(starTreeConfig.getMetrics().get(position).getName(), position);
    }

    for (Entry<String, List<ReportRow>> entry : metricTableRows.entrySet()) {
      List<ReportRow> tableRows = entry.getValue();
      String metric = entry.getKey();
      if (tableRows.size() == 0) {
        throw new IllegalStateException("No rows found for metric " + metric);
      }
      MetricTimeSeries currentSeries = new MetricTimeSeries(MetricSchema.fromMetricSpecs(starTreeConfig.getMetrics()));
      MetricTimeSeries baselineSeries = new MetricTimeSeries(MetricSchema.fromMetricSpecs(starTreeConfig.getMetrics()));

      ReportRow summary = new ReportRow(tableRows.get(0));

      if (metric.startsWith("RATIO")) {
        double currentSum = 0;
        double baselineSum = 0;
        for (int i = 0; i < tableRows.size(); i++) {
          currentSum += tableRows.get(i).getCurrent().doubleValue();
          baselineSum += tableRows.get(i).getBaseline().doubleValue();
        }
        summary.setCurrent(currentSum);
        summary.setBaseline(baselineSum);
      } else {
        for (int i = 0; i < tableRows.size(); i++) {
          currentSeries.set(i, metric, tableRows.get(i).getCurrent());
          baselineSeries.set(i, metric, tableRows.get(i).getBaseline());
        }
        summary.setCurrent(currentSeries.getMetricSums()[metricPosition.get(metric)]);
        summary.setBaseline(baselineSeries.getMetricSums()[metricPosition.get(metric)]);
      }

      summary.setRatio(DOUBLE_FORMAT.format((summary.getCurrent().doubleValue() - summary.getBaseline().doubleValue())/(summary.getBaseline().doubleValue()) * 100));
      summary.setDimension("Total");
      tableRows.add(summary);
    }
  }


  /**
   * Create report rows
   * @param currentStartHour
   * @param currentEndHour
   * @param baselineStartHour
   * @param baselineEndHour
   * @param currentReportOutput
   * @param baselineReportOutput
   * @param metric
   * @param dimensionPosition
   * @return
   */
  private List<ReportRow> createRow(DateTime currentStartHour, DateTime currentEndHour, DateTime baselineStartHour, DateTime baselineEndHour,
      Map<DimensionKey, Number> currentReportOutput, Map<DimensionKey, Number> baselineReportOutput, String metric, int dimensionPosition) {

    List<ReportRow> reportRows = new ArrayList<ReportRow>();
    for (Entry<DimensionKey, Number> entry : currentReportOutput.entrySet()) {
      String dimension;
      if (dimensionPosition == -1) {
        dimension = "-";
      } else {
        dimension = entry.getKey().getDimensionValues()[dimensionPosition];
      }

      ReportRow row = null;
      try {
        if (metric.startsWith("RATIO")) {
          row = new ReportRow(currentStartHour, currentEndHour,
              baselineStartHour, baselineEndHour,
              entry.getKey(), dimension, metric,
              baselineReportOutput.get(entry.getKey()) == null ? 0 : baselineReportOutput.get(entry.getKey()),
              entry.getValue());

        } else {
          row = new ReportRow(currentStartHour, currentEndHour,
              baselineStartHour, baselineEndHour,
              entry.getKey(), dimension, metric,
              baselineReportOutput.get(entry.getKey()) == null ? 0 : baselineReportOutput.get(entry.getKey()).longValue(),
              entry.getValue().longValue());
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
      row.setRatio(DOUBLE_FORMAT.format((row.getCurrent().doubleValue() - row.getBaseline().doubleValue()) / (row.getBaseline().doubleValue()) * 100));
      reportRows.add(row);
    }
    return reportRows;
  }

  /**
   * Apply filters to thirdeye raw sql response
   * @param queryResult : Thirdeye raw sql response
   * @param tableSpec
   * @param metric
   * @param startTime
   * @param endTime
   * @return
   */
  private Map<DimensionKey, Number> applyFilters(ThirdEyeRawResponse queryResult, TableSpec tableSpec, String metric, DateTime startTime, DateTime endTime) {

    Map<DimensionKey, Number> reportOutput = new HashMap<DimensionKey, Number>();

    // apply fixed dimensions
    String[] filteredDimension = new String[queryResult.getDimensions().size()];
    for (int i = 0; i < queryResult.getDimensions().size(); i++) {
      if (tableSpec.getFixedDimensions() != null && tableSpec.getFixedDimensions().containsKey(queryResult.getDimensions().get(i))) {
        filteredDimension[i] = tableSpec.getFixedDimensions().get(queryResult.getDimensions().get(i));
      } else {
        filteredDimension[i] = "*";
      }
    }

    // get dimensions to include in group by
    Set<String> includeDimensions = new HashSet<>();
    Set<String> excludeDimensions = new HashSet<String>();
    int dimensionPosition = -1;
    if (tableSpec.getGroupBy() != null) {
      dimensionPosition = queryResult.getDimensions().indexOf(tableSpec.getGroupBy());
      // include specific dimension values
      if (tableSpec.getFilter()!= null && tableSpec.getFilter().getIncludeDimensions() != null) {
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
    MetricSchema schema = MetricSchema.fromMetricSpecs(starTreeConfig.getMetrics());
    for (int i = 0; i < starTreeConfig.getMetrics().size(); i ++) {
      if (metric.equals(starTreeConfig.getMetrics().get(i).getName())) {
        metricPosition = i;
        break;
      }
    }

    if (metric.startsWith("RATIO")) {
      for (Entry<String, Map<String, Number[]>> entry : queryResult.getData().entrySet()) {
        DimensionKey dimensionKey = DimensionKeyUtils.createDimensionKey(entry.getKey());
        Map<String, Number[]> data = entry.getValue();
        double ratioSum = 0;
        for (Entry<String, Number[]> dataEntry : data.entrySet()) {
          if (!String.valueOf(endTime.getMillis()).equals(dataEntry.getKey())) {
            int position = queryResult.getMetrics().indexOf(metric);
            ratioSum += dataEntry.getValue()[position].doubleValue();
          }
        }
        reportOutput.put(dimensionKey, ratioSum);
      }
    } else {
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
    }
    return reportOutput;
  }


}
