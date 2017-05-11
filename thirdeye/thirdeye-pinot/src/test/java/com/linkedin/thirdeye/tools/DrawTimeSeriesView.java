package com.linkedin.thirdeye.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.jfree.chart.ChartPanel;
import org.jfree.data.time.RegularTimePeriod;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;


public class DrawTimeSeriesView {
  private static final Logger LOG = LoggerFactory.getLogger(DrawTimeSeriesView.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());


  public static final String TIME_BUCKETS = "timeBuckets";
  public static final String CURRENT_START = "currentStart";
  public static final String CURRENT_END = "currentEnd";
  public static final String BASELINE_START = "baselineStart";
  public static final String BASELINE_END = "baselineEnd";
  public static final String CURRENT_VALUES = "currentValues";
  public static final String BASELINE_VALUES = "baselineValues";
  public static final String PNG_FILE_EXTENSION  = ".png";
  public static final String CSV_FILE_EXTENSION  = ".csv";

  private long functionId;
  private String dashboardHost;
  private int dashboardPort;
  private Map<String, TimeSeriesLineChart> functionTimeSeriesChartMap;

  private AnomalyFunctionManager anomalyFunctionDAO;

  public DrawTimeSeriesView (File persistenceFile, String dashboardHost, int dashboardPort) throws Exception {
    init(persistenceFile);

    this.dashboardHost = dashboardHost;
    this.dashboardPort = dashboardPort;
  }

  public void init(File persistenceFile) throws Exception {
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);

    this.functionTimeSeriesChartMap = new HashMap<>();
  }


  private String jodaDateTimeToISO(DateTime dateTime) {
    return dateTime.toString(ISODateTimeFormat.dateTime());
  }
  /**
   * Draw the timeseries figure of a given list of functions and output CSV and PNG files to the given path
   * @param functionIds
   * @param outputPath
   * @throws Exception
   */
  public void drawAndExport(final List<Long> functionIds, final String outputPath) throws Exception {
    for(Long id : functionIds) {
      drawAndExport(id, outputPath);
    }
  }
  public void drawAndExport(final List<Long> functionIds, DateTime start, DateTime end, final String outputPath) throws Exception {
    for(Long id : functionIds) {
      drawAndExport(id, start, end, outputPath);
    }
  }

  public void drawAndExport(long functionId, String outputPath) throws Exception {
    drawAndExport(functionId, null, null, outputPath);
  }
  /**
   * Draw the timeseries figure of a given function and output CSV and PNG files to the given path
   * @param functionId
   * @param outputPath
   * @throws Exception
   */
  public void drawAndExport(long functionId, DateTime start, DateTime end, String outputPath) throws Exception {
    AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionDAO.findById(functionId);
    if(anomalyFunctionDTO == null) {
      LOG.warn("Anomaly Function {} doesn't exist", functionId);
      return;
    }
    File outputDir = null;
    if(StringUtils.isNotEmpty(outputPath)) {
      outputDir = new File(outputPath + "/" + anomalyFunctionDTO.getCollection());
      if (!outputDir.exists()) {
        outputDir.mkdirs();
      }
    }

    List<String> keyList = loadDataByFunction(functionId, start, end);

    for(String fileName : keyList) {
      TimeSeriesLineChart timeSeriesLineChart = this.functionTimeSeriesChartMap.get(fileName);
      if (outputDir != null) {
        // Draw figure and save
        File outputFile = new File(outputDir.getAbsolutePath() + "/" + fileName + PNG_FILE_EXTENSION);
        saveAs(outputFile, timeSeriesLineChart);
        outputFile = new File(outputDir.getAbsolutePath() + "/" + fileName + CSV_FILE_EXTENSION);
        export(outputFile, timeSeriesLineChart);
      }
    }
  }

  /**
   * Load the timeseries and the baseline data of given list of functions into class
   * @param functionIds
   * @throws Exception
   */
  public void loadDataByFunctions(List<Long> functionIds) throws Exception {
    for(long functionId : functionIds) {
      loadDataByFunction(functionId, null, null);
    }
  }
  /**
   * Load the timeseries and the baseline data of given list of functions into class
   * @param functionIds
   * @throws Exception
   */
  public void loadDataByFunctionsInDateTimeRange(List<Long> functionIds, DateTime start, DateTime end) throws Exception {
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
    List<String> keyList = new ArrayList<>();

    if (start == null || end == null) {
      start = new DateTime(0);
      end = DateTime.now();
    }
    AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionDAO.findById(functionId);
    if(anomalyFunctionDTO == null) {
      LOG.warn("Anomaly Function {} doesn't exist", functionId);
      return keyList;
    }
    String fileName = anomalyFunctionDTO.getFunctionName() + "-" + jodaDateTimeToISO(start);
    String jsonResponse = getTimelinesViewForFunctionIdInDateTimeRange(functionId, start, end);
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Map<String, Object>> dimensionAnomalyTimelinesViewMap = objectMapper.readValue(jsonResponse, HashMap.class);

    for(String dimensions : dimensionAnomalyTimelinesViewMap.keySet()) {
      // Load timeseries data
      Map<DateTime, Tuple2<Double, Double>> timeSeries = new HashMap<>();
      Map<String, Object> AnomalyTimelinesView = dimensionAnomalyTimelinesViewMap.get(dimensions);
      ArrayList<Double> currentValues = (ArrayList<Double>) AnomalyTimelinesView.get(CURRENT_VALUES);
      ArrayList<Double> baselineValues = (ArrayList<Double>) AnomalyTimelinesView.get(BASELINE_VALUES);
      ArrayList timeBuckets = (ArrayList) AnomalyTimelinesView.get(TIME_BUCKETS);
      for (int i = 0; i < timeBuckets.size(); i++) {
        LinkedHashMap<String, Long> timeBucket = (LinkedHashMap<String, Long>) timeBuckets.get(i);
        Double currentValue = currentValues.get(i);
        Double baselineValue = baselineValues.get(i);
        timeSeries.put(new DateTime(timeBucket.get(CURRENT_START)),
            new Tuple2<Double, Double>(currentValue, baselineValue));
      }

      // Draw Figure
      TimeSeriesLineChart timeSeriesLineChart = new TimeSeriesLineChart("Time Series Chart");
      timeSeriesLineChart.loadData(timeSeries);
      timeSeriesLineChart.createChartPanel(dimensions);
      this.functionTimeSeriesChartMap.put(fileName + "_" + dimensions, timeSeriesLineChart);
      keyList.add(fileName + "_" + dimensions);
    }
    return keyList;
  }

  /**
   * Export a given timeseries to CSV file
   * @param outputFile
   * @param timeSeriesLineChart
   * @throws Exception
   */
  public void export(File outputFile, TimeSeriesLineChart timeSeriesLineChart) throws Exception {
    BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
    TimeSeriesCollection timeSeriesCollection = timeSeriesLineChart.getXyDataset();
    TimeSeries current = timeSeriesCollection.getSeries(0);
    TimeSeries baseline = timeSeriesCollection.getSeries(1);
    int numItems = current.getItemCount();
    for(int i = 0; i < numItems; i++) {
      RegularTimePeriod timePeriod = current.getTimePeriod(i);
      double currentVal = current.getDataItem(i).getValue().doubleValue();
      double baselineVal = baseline.getDataItem(i).getValue().doubleValue();

      bw.write(String.format("%s,%d,%.2f,%.2f", timePeriod.getStart().toString().toString(),
          timePeriod.getFirstMillisecond(),  currentVal, baselineVal));
      bw.newLine();
    }
    bw.close();
  }

  /**
   * Save the timeseries as PNG
   * @param outputFile
   * @param timeSeriesLineChart
   */
  public void saveAs(File outputFile, TimeSeriesLineChart timeSeriesLineChart) {
    timeSeriesLineChart.saveAsPNG(outputFile, 1280, 640);
  }

  /**
   * Pop out application window for showing the time series
   * @param title
   * @param timeSeriesLineChart
   */
  private void view(String title, TimeSeriesLineChart timeSeriesLineChart) {
    ApplicationFrame applicationFrame = new ApplicationFrame(title);
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

  /**
   * Pop out application window for showing all time series
   */
  public void view() {
    for (String title : functionTimeSeriesChartMap.keySet()) {
      view(title, functionTimeSeriesChartMap.get(title));
    }
  }

  /**
   * Load timeseries jsnon file from http get with start and end date time range
   * @param functionId
   * @return
   * @throws Exception
   */
  private String getTimelinesViewForFunctionIdInDateTimeRange(long functionId, DateTime start, DateTime end) throws Exception {
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
    return new ArrayList<>(this.functionTimeSeriesChartMap.keySet());
  }

  public static void main(String[] args) throws Exception {
    args = new String[]{"/home/ychung/workspace/growth_timeseries/growth-local.yml"};
    final DrawTimeSeriesViewConfig config =
        OBJECT_MAPPER.readValue(new File(args[0]), DrawTimeSeriesViewConfig.class);

    DrawTimeSeriesView drawTimeSeriesView = new DrawTimeSeriesView(new File(config.getPersistenceFile()),
        config.getDashboardHost(), config.getDashboardPort());
    List<Long> functionIds = new ArrayList<>();
    for(String functionId : config.getFunctionIds().split(",")) {
      functionIds.add(Long.valueOf(functionId));
    }

    DateTime end = ISODateTimeFormat.dateTimeParser().parseDateTime("2017-05-04");
    DateTime start = end.minus(Days.days(90));


    drawTimeSeriesView.drawAndExport(functionIds, start, end, config.getOutputPath());
    drawTimeSeriesView.view();
  }
}
