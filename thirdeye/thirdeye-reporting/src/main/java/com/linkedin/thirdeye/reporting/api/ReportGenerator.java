package com.linkedin.thirdeye.reporting.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
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
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.anomaly.api.AnomalyDatabaseConfig;
import com.linkedin.thirdeye.anomaly.reporting.AnomalyReportGenerator;
import com.linkedin.thirdeye.anomaly.reporting.AnomalyReportTable;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import com.linkedin.thirdeye.dashboard.util.SqlUtils;


public class ReportGenerator implements Job{

  private static final Logger LOGGER = LoggerFactory.getLogger(ReportGenerator.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static DecimalFormat DOUBLE_FORMAT = new DecimalFormat("#.#");

  private String collection;
  private String serverUri;
  private String dashboardUri;
  private ReportConfig reportConfig;
  private StarTreeConfig starTreeConfig;

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {

    LOGGER.info("Executing {}", context.getJobDetail().getDescription());
    try {
      File reportConfigFile = new File(context.getJobDetail().getJobDataMap().get(ReportConstants.CONFIG_FILE_KEY).toString());
      reportConfig = ReportConfig.decode(new FileInputStream(reportConfigFile));
      collection = reportConfig.getCollection();
      serverUri = context.getJobDetail().getJobDataMap().get(ReportConstants.SERVER_URI_KEY).toString();
      dashboardUri = context.getJobDetail().getJobDataMap().getString(ReportConstants.DASHBOARD_URI_KEY.toString());
      starTreeConfig = getStarTreeConfig();

      ScheduleSpec scheduleSpec = reportConfig.getSchedules().get(context.getJobDetail().getDescription());
      int aggregationSize = scheduleSpec.getAggregationSize();
      TimeUnit aggregationUnit = scheduleSpec.getAggregationUnit();
      int lagSize = scheduleSpec.getLagSize();
      TimeUnit lagUnit = scheduleSpec.getLagUnit();

      List<Table> tables = new ArrayList<Table>();
      List<AnomalyReportTable> anomalyReportTables = new ArrayList<AnomalyReportTable>();
      for (TableSpec tableSpec : reportConfig.getTables()) {

        int baselineSize = tableSpec.getBaselineSize();
        TimeUnit baselineUnit = tableSpec.getBaselineUnit();

        DateTime endTime = new DateTime().minus(TimeUnit.MILLISECONDS.convert(lagSize, lagUnit));
        DateTime currentEndHour = new DateTime(DateUtils.truncate(new Date(endTime.getMillis()), Calendar.HOUR));
        DateTime baselineEndHour = currentEndHour.minus(TimeUnit.MILLISECONDS.convert(baselineSize, baselineUnit));
        DateTime currentStartHour = currentEndHour.minus(TimeUnit.MILLISECONDS.convert(
            scheduleSpec.getReportWindow() * scheduleSpec.getAggregationSize(), scheduleSpec.getAggregationUnit()));
        DateTime baselineStartHour = baselineEndHour.minus(TimeUnit.MILLISECONDS.convert(
            scheduleSpec.getReportWindow() * scheduleSpec.getAggregationSize(), scheduleSpec.getAggregationUnit()));
        reportConfig.setStartTime(currentStartHour);
        reportConfig.setEndTime(currentEndHour);

        Map<String, String> dimensionValues = new HashMap<String, String>();
        if (tableSpec.getGroupBy() != null) {
          dimensionValues.put(tableSpec.getGroupBy(), "!");
        }
        if (tableSpec.getFixedDimensions() != null) {
          dimensionValues.putAll(tableSpec.getFixedDimensions());
        }

        Map<String, List<ReportRow>> metricTableRows = new HashMap<>();
        List<GroupBy> groupBy = null;
        for (int i = 0; i < scheduleSpec.getReportWindow(); i++) {
          currentEndHour = currentStartHour.plus(TimeUnit.MILLISECONDS.convert(aggregationSize, aggregationUnit));
          baselineEndHour = baselineStartHour.plus(TimeUnit.MILLISECONDS.convert(aggregationSize, aggregationUnit));

          for (String metric : tableSpec.getMetrics()) {

            String currentSql = SqlUtils.getSql(metric, collection, currentStartHour, currentEndHour, dimensionValues);
            String baselineSql = SqlUtils.getSql(metric, collection, baselineStartHour, baselineEndHour, dimensionValues);

            QueryResult currentQueryResult = getReport(currentSql);
            QueryResult baselineQueryResult = getReport(baselineSql);

            Map<DimensionKey, Number> currentReportOutput = applyFilters(currentQueryResult, tableSpec, metric, currentStartHour, currentEndHour);
            Map<DimensionKey, Number> baselineReportOutput = applyFilters(baselineQueryResult, tableSpec, metric, baselineStartHour, baselineEndHour);

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

        calculateSummaryRow(metricTableRows, tableSpec);
        groupBy = getGroupBy(metricTableRows);

        if (scheduleSpec.isFindAnomalies()) {
          anomalyReportTables.addAll(getAnomalies(tableSpec));
        }

        URL thirdeyeUri = getThirdeyeURL(tableSpec, scheduleSpec);
        Table table = new Table(metricTableRows, tableSpec, groupBy, thirdeyeUri);
        tables.add(table);
      }

      if (reportConfig.getAliases() != null) {
        alias(tables);
      }


      ReportEmailSender reportEmailSender = new ReportEmailSender(tables, scheduleSpec, reportConfig, anomalyReportTables);
      reportEmailSender.emailReport();

    } catch (IOException e) {
      LOGGER.error(e.toString());
    }
  }


  private URL getThirdeyeURL(TableSpec tableSpec, ScheduleSpec scheduleSpec) throws MalformedURLException {
    ThirdeyeUri thirdeyeUri = new ThirdeyeUri(dashboardUri, collection, scheduleSpec.getAggregationSize(), scheduleSpec.getAggregationUnit(), tableSpec.getMetrics(), reportConfig.getStartTime().getMillis(), reportConfig.getEndTime().getMillis());
    return thirdeyeUri.getThirdeyeUri();
  }


  private List<AnomalyReportTable> getAnomalies(TableSpec tableSpec) throws IOException {
    List<AnomalyReportTable> anomalyTables = new ArrayList<AnomalyReportTable>();
    for (String metric : tableSpec.getMetrics()) {
      AnomalyDatabaseConfig dbConfig = new AnomalyDatabaseConfig("jhong-ld1/thirdeye", "rule", "anomaly", "alert", "", false);
      AnomalyReportGenerator anomalyReportGenerator = new AnomalyReportGenerator(dbConfig);
      anomalyTables.add(anomalyReportGenerator.getAnomalyTable(collection, metric, reportConfig.getStartTime().getMillis(), reportConfig.getEndTime().getMillis(), 20));
    }

    return anomalyTables;
  }


  private List<GroupBy> getGroupBy(Map<String, List<ReportRow>> metricTableRows) {
    List<GroupBy> groupBy = new ArrayList<GroupBy>();
    for (Entry<String, List<ReportRow>> entry : metricTableRows.entrySet()) {
      for (ReportRow row : entry.getValue()) {
        String startTime;
        if (row.getDimension().equals("Total")) {
          startTime = "Total";
        } else {
          startTime = ReportConstants.DATE_TIME_FORMATTER.print(row.getCurrentStart());
        }
        groupBy.add(new GroupBy(startTime, row.getDimension()));
      }
      break;
    }
    return groupBy;
  }


  private void calculateSummaryRow(Map<String, List<ReportRow>> metricTableRows, TableSpec tableSpec) {
    Map<String, Integer> metricPosition = new HashMap<String, Integer>();
    for (int position = 0; position < starTreeConfig.getMetrics().size(); position++) {
      metricPosition.put(starTreeConfig.getMetrics().get(position).getName(), position);
    }

    for (Entry<String, List<ReportRow>> entry : metricTableRows.entrySet()) {
      List<ReportRow> tableRows = entry.getValue();
      String metric = entry.getKey();
      MetricTimeSeries currentSeries = new MetricTimeSeries(MetricSchema.fromMetricSpecs(starTreeConfig.getMetrics()));
      MetricTimeSeries baselineSeries = new MetricTimeSeries(MetricSchema.fromMetricSpecs(starTreeConfig.getMetrics()));

      for (int i = 0; i < tableRows.size(); i++) {
        currentSeries.set(i, metric, tableRows.get(i).getCurrent());
        baselineSeries.set(i, metric, tableRows.get(i).getBaseline());
      }

      ReportRow summary = new ReportRow(tableRows.get(0));
      summary.setCurrent(currentSeries.getMetricSums()[metricPosition.get(metric)]);
      summary.setBaseline(baselineSeries.getMetricSums()[metricPosition.get(metric)]);
      summary.setRatio(DOUBLE_FORMAT.format((summary.getCurrent().doubleValue() - summary.getBaseline().doubleValue())/(summary.getBaseline().doubleValue()) * 100));
      summary.setDimension("Total");
      tableRows.add(summary);
    }
  }


  private void alias(List<Table> tables) {

    AliasSpec aliasSpec = reportConfig.getAliases();

    for (Table table : tables) {

      // Alias metric
      if (aliasSpec.getMetric() != null) {
        for (Entry<String, String> metricAlias : aliasSpec.getMetric().entrySet()) {
          if (table.getMetricReportRows().get(metricAlias.getKey()) != null) {
            List<ReportRow> reportRows = table.getMetricReportRows().remove(metricAlias.getKey());
            table.getMetricReportRows().put(metricAlias.getValue(), reportRows);
          }
        }
      }

      // alias groupBy if it exists
      String groupBy = table.tableSpec.getGroupBy();
      if (groupBy != null) {

        // alias includeDimensions
        List<String> groupByValues = table.tableSpec.getFilter() == null ? null : table.tableSpec.getFilter().getIncludeDimensions();
        Map<String, String> groupByValuesAlias = aliasSpec.getDimensionValues() == null ? null : aliasSpec.getDimensionValues().get(groupBy);

        if (groupByValues != null && groupByValuesAlias != null) {
          for (GroupBy group : table.getGroupBy()) {
            if (groupByValuesAlias.get(group.getDimension()) != null) {
              group.setDimension(groupByValuesAlias.get(group.getDimension()));
            }
          }
        }

        // alias groupBy
        Map<String, String> dimensionAlias = aliasSpec.getDimension();
        if (dimensionAlias != null && dimensionAlias.get(groupBy) != null) {
          table.tableSpec.setGroupBy(dimensionAlias.get(groupBy));
        }
      }

    }
  }


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
       row = new ReportRow(currentStartHour, currentEndHour,
          baselineStartHour, baselineEndHour,
          entry.getKey(), dimension, metric,
          baselineReportOutput.get(entry.getKey()) == null ? 0 : baselineReportOutput.get(entry.getKey()).longValue(),
          entry.getValue().longValue());
} catch (Exception e) {
e.printStackTrace();
}

      row.setRatio(DOUBLE_FORMAT.format((row.getCurrent().doubleValue() - row.getBaseline().doubleValue()) / (row.getBaseline().doubleValue()) * 100));
      reportRows.add(row);
    }
    return reportRows;
  }


  private Map<DimensionKey, Number> applyFilters(QueryResult queryResult, TableSpec tableSpec, String metric, DateTime startTime, DateTime endTime) {

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
          includeDimensions.add(createQueryKey(filteredDimension));
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
    int metricPosition = queryResult.getMetrics().indexOf(metric);

    MetricSchema schema = MetricSchema.fromMetricSpecs(starTreeConfig.getMetrics());

    for (Entry<String, Map<String, Number[]>> entry : queryResult.getData().entrySet()) {
      DimensionKey dimensionKey = createDimensionKey(entry.getKey());
      Map<String, Number[]> data = entry.getValue();
      MetricTimeSeries series = new MetricTimeSeries(schema);
      for (Entry<String, Number[]> dataEntry : data.entrySet()) {
        if (!String.valueOf(endTime.getMillis()).equals(dataEntry.getKey())) {
          series.set(Long.valueOf(dataEntry.getKey()), metric, dataEntry.getValue()[metricPosition]);
        }
      }
      Number[] seriesSums = series.getMetricSums();
      reportOutput.put(dimensionKey, seriesSums[metricPosition]);
    }
    return reportOutput;
  }


  private String createQueryKey(String[] filteredDimension) {
    StringBuilder sb = new StringBuilder();
    sb = sb.append("[\"");
    Joiner joiner = Joiner.on("\",\"");
    sb.append(joiner.join(filteredDimension));
    sb.append("\"]");
    return sb.toString();
  }

  private DimensionKey createDimensionKey(String dimension) {
    dimension = dimension.replace("[\"", "");
    dimension = dimension.replace("\"]", "");
    return new DimensionKey(dimension.split("\",\""));
  }



  private QueryResult getReport(String sql) throws IOException {

    URL url = new URL(serverUri + "/query/" + URLEncoder.encode(sql, "UTF-8"));
    return OBJECT_MAPPER.readValue((new InputStreamReader(url.openStream(), "UTF-8")), QueryResult.class);
  }

  private StarTreeConfig getStarTreeConfig() throws JsonParseException, JsonMappingException, UnsupportedEncodingException, IOException {

    URL url = new URL(serverUri + "/collections/" + collection);
    return OBJECT_MAPPER.readValue((new InputStreamReader(url.openStream(), "UTF-8")), StarTreeConfig.class);
  }

}
