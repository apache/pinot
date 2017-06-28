package com.linkedin.thirdeye.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.TimeSeriesCompareMetricView;
import com.linkedin.thirdeye.dashboard.resources.v2.pojo.ValuesContainer;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import java.awt.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.jfree.chart.ChartPanel;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.time.TimeSeriesDataItem;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.ReadablePeriod;
import org.joda.time.Weeks;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DrawTimeSeriesView {
  private static final Logger LOG = LoggerFactory.getLogger(DrawTimeSeriesView.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  public static final String PNG_FILE_EXTENSION  = ".png";
  public static final String CSV_FILE_EXTENSION  = ".csv";
  public static final String TIME_BUCKETS = "timeBuckets";
  public static final String CURRENT_START = "currentStart";
  public static final String CURRENT_END = "currentEnd";
  public static final String BASELINE_START = "baselineStart";
  public static final String BASELINE_END = "baselineEnd";
  public static final String CURRENT_VALUES = "currentValues";
  public static final String BASELINE_VALUES = "baselineValues";
  public static final ReadablePeriod DEFAULT_MONITORING_PERIOD = Days.ONE;

  public enum ComparisonMode {
    WoW(1), Wo2W(2), Wo3W(3), Wo4W(4), NONE(0);

    private int wowNum;

    ComparisonMode (int wowNum) {
      this.wowNum = wowNum;
    }

    public int getWowNum() {
      return wowNum;
    }

    @Override
    public String toString() {
      return super.toString();
    }
  }

  private String dashboardHost;
  private int dashboardPort;
  private ComparisonMode comparisonMode;
  private Map<String, TimeSeriesLineChart> figureKeyTimeSeriesChartMap;

  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private MetricConfigManager metricConfigDAO;

  public DrawTimeSeriesView (File persistenceFile, ComparisonMode comparisonMode, String dashboardHost, int dashboardPort) throws Exception {
    init(persistenceFile);

    this.comparisonMode = comparisonMode;
    this.dashboardHost = dashboardHost;
    this.dashboardPort = dashboardPort;
  }

  public void clear() {
    figureKeyTimeSeriesChartMap.clear();
  }

  public void init(File persistenceFile) throws Exception {
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    mergedAnomalyResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl.class);
    metricConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MetricConfigManagerImpl.class);

    this.figureKeyTimeSeriesChartMap = new HashMap<>();
  }

  private String jodaDateTimeToISO(DateTime dateTime) {
    return dateTime.toString(ISODateTimeFormat.dateTime());
  }

  public List<String> loadDataByAnomalyIds(List<Long> anomalyIds, ComparisonMode comparisonMode) throws Exception {
    List<String> figureKeys = new ArrayList<>();
    for(long anomalyId : anomalyIds) {
      figureKeys.add(loadDataByAnomalyId(anomalyId, comparisonMode));
    }
    return figureKeys;
  }

  public String loadDataByAnomalyId(long anomalyId, ComparisonMode comparisonMode) throws Exception {
    MergedAnomalyResultDTO mergedAnomalyResult = mergedAnomalyResultDAO.findById(anomalyId);
    if(mergedAnomalyResult == null) {
      LOG.warn("Unable to load mergeAnomalyResult {}", anomalyId);
    }
    AnomalyTimelinesView anomalyTimelinesView = getAnomalyTimelinesViewByAnomalyId(anomalyId);
    DateTime start = DateTime.now();
    DateTime end = new DateTime(0);
    for(TimeBucket timeBucket : anomalyTimelinesView.getTimeBuckets()) {
      if(timeBucket.getCurrentStart() < start.getMillis()) {
        start = new DateTime(timeBucket.getCurrentStart());
      }
      if(timeBucket.getCurrentEnd() > end.getMillis()) {
        end = new DateTime(timeBucket.getCurrentEnd());
      }
    }
    // Load WoW timeseries from ThirdEye
    TimeSeriesCompareMetricView timeSeriesCompareMetricView = getWoWComparison(mergedAnomalyResult.getFunctionId(),
        start, end, comparisonMode);

    String figureName = mergedAnomalyResult.getFunction().getFunctionName() + ":" + mergedAnomalyResult.getId();
    this.createFigureWithComparison(figureName, anomalyTimelinesView, timeSeriesCompareMetricView,
        mergedAnomalyResult.getDimensions().toJson());
    return figureName;
  }

  /**
   * Load the timeseries and the baseline data of given list of functions into class
   * @param functionIds
   * @throws Exception
   */
  public void loadDataByFunctions(List<Long> functionIds, ComparisonMode comparisonMode) throws Exception {
    for(long functionId : functionIds) {
      loadDataByFunction(functionId, null, null);
    }
  }
  /**
   * Load the timeseries and the baseline data of given list of functions into class
   * @param functionIds
   * @throws Exception
   */
  public void loadDataByFunctionsInDateTimeRange(List<Long> functionIds, DateTime start, DateTime end, ComparisonMode comparisonMode) throws Exception {
    for(long functionId : functionIds) {
      loadDataByFunction(functionId, start, end);
    }
  }

  /**
   * Load the timeseries and the baseline data into class
   * @param functionId
   * @return
   * @throws Exception
   */
  private List<String> loadDataByFunction(long functionId, DateTime start, DateTime end) throws Exception {
    if (start == null || end == null) {
      end = DateTime.now();
      start = end.minus(DEFAULT_MONITORING_PERIOD);
    }
    AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionDAO.findById(functionId);
    if(anomalyFunctionDTO == null) {
      LOG.warn("Anomaly Function {} doesn't exist", functionId);
      return Collections.emptyList();
    }
    String figureName = anomalyFunctionDTO.getFunctionName() + "-" + jodaDateTimeToISO(start);

    // load function baseline and current values from ThirdEye server
    String jsonResponse = getTimelinesViewForFunctionIdInDateTimeRange(functionId, start, end);

    Map<String, AnomalyTimelinesView> dimensionAnomalyTimelinesViewMap =
        parseJsonToAnomalyTimelinesViewMap(jsonResponse, end);

    DateTime viewStart = start;
    DateTime viewEnd = start;
    for (String dimensions : dimensionAnomalyTimelinesViewMap.keySet()) {
      AnomalyTimelinesView view = dimensionAnomalyTimelinesViewMap.get(dimensions);

      for(TimeBucket timeBucket : view.getTimeBuckets()) {
        DateTime currentStart = new DateTime(timeBucket.getCurrentStart());
        if(viewStart.isAfter(currentStart)) {
          viewStart = currentStart;
        }
        if(viewEnd.isBefore(currentStart)) {
          viewEnd = currentStart;
        }
      }
    }


    for(Map.Entry<String, AnomalyTimelinesView> entry : dimensionAnomalyTimelinesViewMap.entrySet()) {
      String dimensions = entry.getKey();
      AnomalyTimelinesView anomalyTimelinesView = entry.getValue();

      // Load WoW timeseries from ThirdEye
      TimeSeriesCompareMetricView
          timeSeriesCompareMetricView = getWoWComparison(functionId, viewStart, viewEnd, comparisonMode);

      String key = figureName + "_" + dimensions;
      createFigureWithComparison(key, anomalyTimelinesView, timeSeriesCompareMetricView, dimensions);
    }
    return new ArrayList<>(figureKeyTimeSeriesChartMap.keySet());
  }

  private TimeSeriesCompareMetricView getWoWComparison(long functionId, DateTime start, DateTime end,
      ComparisonMode comparisonMode) throws Exception {
    TimeSeriesCompareMetricView timeSeriesCompareMetricView = null;
    if (!comparisonMode.equals(ComparisonMode.NONE)) {
      String wowJsonResponse =
          getTimeSeriesComparisonForFunctionIdInDataTimeRange(functionId, start, end, comparisonMode);
      timeSeriesCompareMetricView = OBJECT_MAPPER.readValue(wowJsonResponse, TimeSeriesCompareMetricView.class);
    }
    return timeSeriesCompareMetricView;
  }
  private void createFigureWithComparison(String figureName, AnomalyTimelinesView anomalyTimelinesView,
      TimeSeriesCompareMetricView timeSeriesCompareMetricView, String dimensions) throws Exception {
    List<TimeBucket> timeBuckets = anomalyTimelinesView.getTimeBuckets();
    Map<DateTime, Double> baselineTimeSeries = new HashMap<>();
    Map<DateTime, Double> currentTimeSeries = new HashMap<>();

    double currentValue = 0.0;
    double baselineValue = 0.0;

    for (int i = 0; i < timeBuckets.size(); i++) {
      TimeBucket timeBucket = timeBuckets.get(i);
      currentValue = anomalyTimelinesView.getCurrentValues().get(i);
      baselineValue = anomalyTimelinesView.getBaselineValues().get(i);
      baselineTimeSeries.put(new DateTime(timeBucket.getCurrentStart()), baselineValue);
      currentTimeSeries.put(new DateTime(timeBucket.getCurrentStart()), currentValue);
    }

    // Initiate Figure
    TimeSeriesLineChart timeSeriesLineChart = new TimeSeriesLineChart("Time Series Chart");

    // Load WoW data
    if (timeSeriesCompareMetricView != null) {
      // Extract WoW baseline and current value from server response
      Map<DateTime, Double> wowTimeSeries = new HashMap<>();
      DimensionMap dimensionMap = OBJECT_MAPPER.readValue(dimensions, DimensionMap.class);
      String dimensionValue = "All";
      if(dimensionMap.size() > 0) {
        dimensionValue = dimensionMap.get(dimensionMap.firstKey());
      }
      List<Long> timestamps = timeSeriesCompareMetricView.getTimeBucketsCurrent();
      ValuesContainer valuesContainer = timeSeriesCompareMetricView.getSubDimensionContributionMap().get(dimensionValue);
      double[] wowValues = valuesContainer.getBaselineValues();
      double[] currentValueArray = valuesContainer.getCurrentValues();

      for(int i = 0; i < timestamps.size(); i++) {
        wowTimeSeries.put(new DateTime(timestamps.get(i)), wowValues[i]);
        currentTimeSeries.put(new DateTime(timestamps.get(i)), currentValueArray[i]);
      }
      timeSeriesLineChart.loadTimeSeries(comparisonMode.name(), wowTimeSeries);
    }

    // Draw timeseries data
    timeSeriesLineChart.loadTimeSeries("current", currentTimeSeries);
    timeSeriesLineChart.loadTimeSeries("baseline", baselineTimeSeries);
    timeSeriesLineChart.createChartPanel(dimensions, TimeSeriesLineChart.defaultSubtitle(currentValue, baselineValue));
    this.figureKeyTimeSeriesChartMap.put(figureName, timeSeriesLineChart);
  }

  /**
   * Export a given timeseries to CSV file
   * @param outputFile
   * @param timeSeriesLineChartkey
   *      The key to access to the TimeSeriesLineChart
   * @throws Exception
   */
  public void export(File outputFile, String timeSeriesLineChartkey) throws Exception {
    TimeSeriesLineChart timeSeriesLineChart = this.figureKeyTimeSeriesChartMap.get(timeSeriesLineChartkey);
    BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
    TimeSeriesCollection timeSeriesCollection = timeSeriesLineChart.getXyDataset();

    bw.write("Date Time, timestamp");
    List<String> titles = new ArrayList<>();
    int seriesCount = timeSeriesCollection.getSeriesCount();
    Map<Long, List<Double>> timeSeriesMap = new HashMap<>();

    for(int i = 0; i < seriesCount; i++) {
      TimeSeries timeSeries = timeSeriesCollection.getSeries(i);
      String timeSeriesKey = (String) timeSeries.getKey();
      titles.add(timeSeriesKey);

      for(int j = 0; j < timeSeries.getItemCount(); j++) {
        TimeSeriesDataItem dataItem = timeSeries.getDataItem(j);
        long timestamp = dataItem.getPeriod().getFirstMillisecond();
        if(!timeSeriesMap.containsKey(timestamp)) {
          List<Double> valueList = new ArrayList<>(seriesCount);
          for(int k = 0; k < seriesCount; k++) {
            valueList.add(Double.NaN);
          }
          timeSeriesMap.put(timestamp, valueList);
        }
        timeSeriesMap.get(timestamp).set(i, dataItem.getValue().doubleValue());
      }
      bw.write("," + timeSeriesKey);
    }
    bw.newLine();
    List<Long> sortedTimestamps = new ArrayList<>(timeSeriesMap.keySet());
    Collections.sort(sortedTimestamps);
    for(long timestamp : sortedTimestamps) {
      DateTime dateTime = new DateTime(timestamp);
      bw.write(String.format("%s,%d", dateTime.toString(), timestamp));

      for(int i = 0; i < titles.size(); i++) {
        bw.write("," + timeSeriesMap.get(timestamp).get(i));
      }
      bw.newLine();
    }
    bw.close();
  }

  /**
   * Save the timeseries as PNG
   * @param outputFile
   * @param timeSeriesLineChartkey
   *      The key to access to the TimeSeriesLineChart
   */
  public void saveAs(File outputFile, String timeSeriesLineChartkey) {
    TimeSeriesLineChart timeSeriesLineChart = this.figureKeyTimeSeriesChartMap.get(timeSeriesLineChartkey);
    timeSeriesLineChart.saveAsPNG(outputFile, 1280, 640);
  }

  /**
   * Pop out application window for showing the time series
   * @param title
   * @param timeSeriesLineChartkey
   */
  private void view(String title, String timeSeriesLineChartkey) {
    final Map<String, TimeSeriesLineChart> finalKeyChartMap = figureKeyTimeSeriesChartMap;
    final String innerTitle = title;
    final String innerKey = timeSeriesLineChartkey;
    new Thread(new Runnable() {
      @Override
      public void run() {
        TimeSeriesLineChart timeSeriesLineChart = finalKeyChartMap.get(innerKey);
        ApplicationFrame applicationFrame = new ApplicationFrame(innerTitle);
        final ChartPanel chartPanel = new ChartPanel(timeSeriesLineChart.getChart());
        chartPanel.setPreferredSize(new Dimension(1280, 640));
        chartPanel.setDomainZoomable(true);
        chartPanel.setMouseZoomable(true, true);
        applicationFrame.setContentPane(chartPanel);
        applicationFrame.pack();
        RefineryUtilities.positionFrameRandomly(applicationFrame);
        applicationFrame.setVisible(true);
        applicationFrame.setSize(1280, 640);
      }
    }).run();
  }

  /**
   * Pop out application window for showing all time series
   */
  public void view() {
    for (String title : figureKeyTimeSeriesChartMap.keySet()) {
      view(title, title);
    }
  }

  private AnomalyTimelinesView getAnomalyTimelinesViewByAnomalyId(long anomalyId) throws Exception {
    MergedAnomalyResultDTO mergedAnomaly = mergedAnomalyResultDAO.findById(anomalyId);
    AnomalyTimelinesView anomalyTimelinesView = null;
    if(mergedAnomaly.getProperties().containsKey("anomalyTimelinesView")) {
      anomalyTimelinesView =
          AnomalyTimelinesView.fromJsonString(mergedAnomaly.getProperties().get("anomalyTimelinesView"));
    } else {
      DateTime anomalyStart = new DateTime(mergedAnomaly.getStartTime());
      DateTime anomalyEnd = new DateTime(mergedAnomaly.getEndTime());

      String jsonString = getTimelinesViewForFunctionIdInDateTimeRange(mergedAnomaly.getFunctionId(),
          anomalyStart, anomalyEnd);
      Map<String, AnomalyTimelinesView> anomalyTimelinesViewMap =
          parseJsonToAnomalyTimelinesViewMap(jsonString, anomalyEnd);
      anomalyTimelinesView = anomalyTimelinesViewMap.get(mergedAnomaly.getDimensions().toString());
    }
    return anomalyTimelinesView;
  }


  private Map<String, AnomalyTimelinesView> parseJsonToAnomalyTimelinesViewMap(String jsonString, DateTime end) throws Exception {
    Map<String, Object> jsonMap = OBJECT_MAPPER.readValue(jsonString, HashMap.class);
    Map<String, AnomalyTimelinesView> anomalyTimelinesViewMap = new HashMap<>();
    for(String dimensions : jsonMap.keySet()) {
      AnomalyTimelinesView view = null;
      String json = OBJECT_MAPPER.writeValueAsString(jsonMap.get(dimensions));
      try {
        view = AnomalyTimelinesView.fromJsonString(json);
      } catch (Exception e) {
     //   LOG.warn("Unable to parse json String {}", json);

        view = new AnomalyTimelinesView();
        Map<String, Object> jsonAnomalyView = (Map<String, Object>) jsonMap.get(dimensions);

        ArrayList<Double> currentValues = (ArrayList<Double>) jsonAnomalyView.get(CURRENT_VALUES);
        ArrayList<Double> baselineValues = (ArrayList<Double>) jsonAnomalyView.get(BASELINE_VALUES);
        ArrayList<Object> timeBuckets = (ArrayList<Object>) jsonAnomalyView.get(TIME_BUCKETS);
        for(int i = 0; i < timeBuckets.size(); i++) {
          LinkedHashMap<String, Long> timeBucket = (LinkedHashMap<String, Long>) timeBuckets.get(i);
          TimeBucket timeBucketInstance = new TimeBucket(timeBucket.get(CURRENT_START), timeBucket.get(CURRENT_END),
              timeBucket.get(BASELINE_START), timeBucket.get(BASELINE_END));

          if(timeBucketInstance.getCurrentEnd() <= end.getMillis()) {
            view.addTimeBuckets(timeBucketInstance);
            view.addCurrentValues(currentValues.get(i));
            view.addBaselineValues(baselineValues.get(i));
          }
        }

      }
      anomalyTimelinesViewMap.put(dimensions, view);

    }
    return anomalyTimelinesViewMap;
  }
  /**
   * Load timeseries jsnon file from http get with start and end date time range
   * @param functionId
   * @return
   * @throws Exception
   */
  private String getTimelinesViewForFunctionIdInDateTimeRange (long functionId, DateTime start, DateTime end) throws Exception {
    DateTimeFormatter isoDateTimeFormatter = ISODateTimeFormat.dateTimeParser();
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append("http://" +  dashboardHost + ":" + dashboardPort);
    urlBuilder.append("/dashboard/timeseries/");
    urlBuilder.append(Long.toString(functionId));
    urlBuilder.append("?");
    urlBuilder.append("start=" + URLEncoder.encode(jodaDateTimeToISO(start)));
    urlBuilder.append("&");
    urlBuilder.append("end=" + URLEncoder.encode(jodaDateTimeToISO(end)));
    return loadJsonFromHttpGet(urlBuilder.toString());
  }

  private String getTimeSeriesComparisonForFunctionIdInDataTimeRange (long functionId, DateTime viewWindowStart,
      DateTime viewWindowEnd, ComparisonMode comparisonMode) throws Exception {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);

    MetricConfigDTO metricConfig = metricConfigDAO
        .findByMetricAndDataset(anomalyFunction.getMetric(), anomalyFunction.getCollection());
    DateTime baselineStart = viewWindowStart.minus(Weeks.weeks(comparisonMode.getWowNum()));
    DateTime baselineEnd = viewWindowStart.minus(comparisonMode.getWowNum());
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append("http://" +  dashboardHost + ":" + dashboardPort);
    urlBuilder.append("/timeseries/compare/");
    urlBuilder.append(metricConfig.getId()); // metric Id
    urlBuilder.append("/");
    urlBuilder.append(viewWindowStart.getMillis()); // currentStart
    urlBuilder.append("/");
    urlBuilder.append(viewWindowEnd.getMillis()); // currentEnd
    urlBuilder.append("/");
    urlBuilder.append(baselineStart.getMillis()); // baselineStart
    urlBuilder.append("/");
    urlBuilder.append(baselineEnd.getMillis()); // baselineEnd
    urlBuilder.append("?");
    urlBuilder.append("granularity=" + anomalyFunction.getBucketUnit().name());
    if(StringUtils.isNotBlank(anomalyFunction.getExploreDimensions())) {
      urlBuilder.append("&");
      urlBuilder.append("dimension=" + anomalyFunction.getExploreDimensions());
    }
    if(StringUtils.isNotBlank(anomalyFunction.getFilters())) {
      urlBuilder.append("&");
      urlBuilder.append("filters=" + URLEncoder.encode(filterToJson(anomalyFunction.getFilterSet()), "UTF-8"));
    }
    return loadJsonFromHttpGet(urlBuilder.toString());
  }

  private String filterToJson(Multimap<String, String> filters) {
    StringBuilder jsonBuilder = new StringBuilder();
    jsonBuilder.append("{");
    for(String key : filters.keySet()) {
      jsonBuilder.append("\"" + key + "\"");
      jsonBuilder.append(":[");
      Collection<String> values =  filters.get(key);
      for(String value : values) {
        jsonBuilder.append("\"" + value + "\",");
      }
      jsonBuilder.deleteCharAt(jsonBuilder.length() - 1);
      jsonBuilder.append("],");
    }
    jsonBuilder.deleteCharAt(jsonBuilder.length() - 1);
    jsonBuilder.append("}");
    return jsonBuilder.toString();
  }

  /**
   * Execute Http Get and fetch Json
   * @param urlToRead
   * @return
   * @throws Exception
   */
  public static String loadJsonFromHttpGet(String urlToRead) throws Exception {
    StringBuilder result = new StringBuilder();
    URL url = new URL(urlToRead);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("GET");
    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
    String line;
    while ((line = rd.readLine()) != null) {
      result.append(line);
    }
    rd.close();
    return result.toString();
  }

  /**
   * Return keys to access to the timeseries chart
   * @return
   */
  public List<String> getKeys() {
    return new ArrayList<>(this.figureKeyTimeSeriesChartMap.keySet());
  }

  public static void main(String[] args) throws Exception{
    final DrawTimeSeriesViewConfig config =
        OBJECT_MAPPER.readValue(new File(args[0]), DrawTimeSeriesViewConfig.class);

    ComparisonMode comparisonMode = config.getComparisonMode();

    List<Long> functionIds = new ArrayList<>();
    for(String functionId : config.getFunctionIds().split(",")) {
      functionIds.add(Long.valueOf(functionId));
    }

    DateTime end = DateTime.now();
    if(StringUtils.isNotBlank(config.getEndTimeISO())) {
      end = ISODateTimeFormat.dateTimeParser().parseDateTime(config.getEndTimeISO());
    }
    DateTime start = end.minus(Days.ONE);
    if(StringUtils.isNotBlank(config.getStartTimeISO())) {
      start = ISODateTimeFormat.dateTimeParser().parseDateTime(config.getStartTimeISO());
    }

    final File outputPath = new File(config.getOutputPath());
    if(!outputPath.exists()) {
      outputPath.mkdirs();
    }
    if(!outputPath.canWrite()) {
      LOG.warn("Unable to write in {}", outputPath);
    }

    final DrawTimeSeriesView
        drawTimeSeriesView = new DrawTimeSeriesView(new File(config.getPersistenceFile()), comparisonMode, config.getDashboardHost(), config.getDashboardPort());

    System.exit(0);
  }
}
