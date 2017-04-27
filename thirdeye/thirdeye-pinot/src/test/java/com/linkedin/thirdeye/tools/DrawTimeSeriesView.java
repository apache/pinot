package com.linkedin.thirdeye.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
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

  private long functionId;
  private String dashboardHost;
  private int dashboardPort;

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
  }

  public void draw(List<Long> functionIds, String outputPath) throws Exception {
    for(Long id : functionIds) {
      draw(id, outputPath);
    }
  }

  public void draw(long functionId, String outputPath) throws Exception {
    AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionDAO.findById(functionId);
    File outputDir = new File(outputPath + "/" + anomalyFunctionDTO.getCollection());
    if(!outputDir.exists()) {
      outputDir.mkdirs();
    }
    String fileName = anomalyFunctionDTO.getFunctionName() + "-" + DateTime.now().toString(ISODateTimeFormat.dateTime());
    String jsonResponse = getTimelinesViewForFunctionId(functionId);
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Map<String, Object>> dimensionAnomalyTimelinesViewMap = objectMapper.readValue(jsonResponse, HashMap.class);

    for(String dimensions : dimensionAnomalyTimelinesViewMap.keySet()) {
      // Load timeseries data
      Map<DateTime, Tuple2<Double, Double>> timeSeries = new HashMap<>();
      Map<String, Object> AnomalyTimelinesView = dimensionAnomalyTimelinesViewMap.get(dimensions);
      ArrayList<Double> currentValues = (ArrayList<Double>) AnomalyTimelinesView.get(CURRENT_VALUES);
      ArrayList<Double> baselineValues = (ArrayList<Double>) AnomalyTimelinesView.get(BASELINE_VALUES);
      ArrayList timeBuckets = (ArrayList) AnomalyTimelinesView.get(TIME_BUCKETS);
      for(int i = 0; i < timeBuckets.size(); i++) {
        LinkedHashMap<String, Long> timeBucket = (LinkedHashMap<String, Long>) timeBuckets.get(i);
        Double currentValue = currentValues.get(i);
        Double baselineValue = baselineValues.get(i);
        timeSeries.put(new DateTime(timeBucket.get(CURRENT_START)), new Tuple2<Double, Double>(currentValue, baselineValue));
      }

      // Draw figure and save
      File outputFile = new File(outputDir.getAbsolutePath() + "/" + fileName + "_" + dimensions + PNG_FILE_EXTENSION);
      TimeSeriesLineChart timeSeriesLineChart = new TimeSeriesLineChart("Time Series Chart");
      timeSeriesLineChart.loadData(timeSeries);
      timeSeriesLineChart.createChartPanel(dimensions);
      timeSeriesLineChart.saveAsPNG(outputFile, 1280, 640);
    }
  }

  private String getTimelinesViewForFunctionId(long functionId) throws Exception {
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append("http://" +  dashboardHost + ":" + dashboardPort);
    urlBuilder.append("/dashboard/timeseries/");
    urlBuilder.append(Long.toString(functionId));
    return getHTML(urlBuilder.toString());
  }

  private String getHTML(String urlToRead) throws Exception {
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

  public static void main(String[] args) throws Exception {
    args = new String[]{"/home/ychung/workspace/growth_timeseries/m2g-local.yml"};
    DrawTimeSeriesViewConfig config =
        OBJECT_MAPPER.readValue(new File(args[0]), DrawTimeSeriesViewConfig.class);
    DrawTimeSeriesView drawTimeSeriesView = new DrawTimeSeriesView(new File(config.getPersistenceFile()),
        config.getDashboardHost(), config.getDashboardPort());
    List<Long> functionIds = new ArrayList<>();
    for(String functionId : config.getFunctionIds().split(",")) {
      functionIds.add(Long.valueOf(functionId));
    }

    drawTimeSeriesView.draw(functionIds, config.getOutputPath());
  }
}
