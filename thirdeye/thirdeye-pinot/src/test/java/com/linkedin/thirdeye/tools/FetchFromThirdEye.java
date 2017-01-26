package com.linkedin.thirdeye.tools;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;


public class FetchFromThirdEye {
  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private RawAnomalyResultManager rawAnomalyResultDAO;

  public FetchFromThirdEye(File persistenceFile) throws Exception{
    init(persistenceFile);
  }

  public FetchFromThirdEye(){ }

  // Private class for storing and sorting results
  public class ResultNode implements Comparable<ResultNode>{
    long functionId;
    String functionName;
    String filters;
    DimensionMap dimensions;
    DateTime startTime;
    DateTime endTime;
    double severity;

    public ResultNode(){}

    @Override
    public int compareTo(ResultNode o){
      return this.startTime.compareTo(o.startTime);
    }

    public String dimensionString(){
      StringBuilder sb = new StringBuilder();
      sb.append("[");
      if(!dimensions.isEmpty()) {
        for (Map.Entry<String, String> entry : dimensions.entrySet()) {
          sb.append(entry.getKey() + ":");
          sb.append(entry.getValue() + "|");
        }
        sb.deleteCharAt(sb.length() - 1);
      }
      sb.append("]");
      return sb.toString();
    }

    public String[] getSchema(){
      return new String[]{
          "StartDate", "EndDate", "Dimensions", "Filters", "FunctionID", "FunctionName", "Severity"
      };
    }
    public String toString(){
      DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
      return String.format("%s,%s,%s,%s,%s,%s,%s", fmt.print(startTime), fmt.print(endTime),
          dimensionString(), (filters == null)? "":filters,
          Long.toString(functionId), functionName, Double.toString(severity*100.0));
    }
  }

  public void init(File persistenceFile) throws Exception {
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    rawAnomalyResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl.class);
    mergedAnomalyResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl.class);
  }
  public List<ResultNode> fetchMergedAnomalies (String collection, String metric, String startTimeISO, String endTimeISO){
    DateTime startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeISO);
    DateTime endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeISO);
    List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionDAO.findAllByCollection(collection);
    System.out.println("Loading merged anaomaly results from db...");
    List<ResultNode> resultNodes = new ArrayList<>();
    for(AnomalyFunctionDTO anomalyDto : anomalyFunctions){
      if(!anomalyDto.getMetric().equals(metric)) continue;

      long id = anomalyDto.getId();
      List<MergedAnomalyResultDTO> mergedResults =
          mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(startTime.getMillis(), endTime.getMillis(), id);
      for(MergedAnomalyResultDTO mergedResult : mergedResults){
        ResultNode res = new ResultNode();
        res.functionId = id;
        res.functionName = anomalyDto.getFunctionName();
        res.startTime = new DateTime(mergedResult.getStartTime());
        res.endTime = new DateTime(mergedResult.getEndTime());
        res.dimensions = mergedResult.getDimensions();
        res.filters = anomalyDto.getFilters();
        res.severity = mergedResult.getWeight();
        resultNodes.add(res);
      }
    }
    Collections.sort(resultNodes);
    return resultNodes;
  }

  public List<ResultNode> fetchRawAnomalies(String collection, String metric, String startTimeISO, String endTimeISO){
    DateTime startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeISO);
    DateTime endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeISO);
    List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionDAO.findAllByCollection(collection);
    System.out.println("Loading raw anaomaly results from db...");
    List<ResultNode> resultNodes = new ArrayList<>();


    for(AnomalyFunctionDTO anomalyDto : anomalyFunctions){
      if(!anomalyDto.getMetric().equals(metric)) continue;

      long id = anomalyDto.getId();
      List<RawAnomalyResultDTO> rawResults =
          rawAnomalyResultDAO.findAllByTimeAndFunctionId(startTime.getMillis(), endTime.getMillis(), id);
      for(RawAnomalyResultDTO rawResult : rawResults){
        ResultNode res = new ResultNode();
        res.functionId = id;
        res.functionName = anomalyDto.getFunctionName();
        res.startTime = new DateTime(rawResult.getStartTime());
        res.endTime = new DateTime(rawResult.getEndTime());
        res.dimensions = rawResult.getDimensions();
        res.filters = anomalyDto.getFilters();
        res.severity = rawResult.getWeight();
        resultNodes.add(res);
      }
    }
    Collections.sort(resultNodes);
    return resultNodes;
  }



  private final String DEFAULT_PATH_TO_TIMESERIES = "/dashboard/data/timeseries?";

  private final String DATASET = "dataset";
  private final String METRIC = "metrics";
  private final String VIEW = "view";
  private final String DEFAULT_VIEW = "timeseries";
  private final String TIME_START = "currentStart";
  private final String TIME_END = "currentEnd";
  private final String GRANULARITY = "aggTimeGranularity";
  private final String DIMENSIONS = "dimensions"; // separate by comma
  private final String FILTERS = "filters";
  public enum TimeGranularity{
    DAYS ("DAYS"),
    HOURS ("HOURS");

    private String timeGranularity = null;
    private TimeGranularity(String str){
      this.timeGranularity = str;
    }
    public String toString(){
      return this.timeGranularity;
    }
  }
  public Map<String, Map<DateTime, Integer>> fetchMetric(String host, String port, String dataset, String metric, String startTimeISO,
      String endTimeISO, TimeGranularity timeGranularity, String dimensions, String filterJson)
      throws  IOException{
    HttpClient client = HttpClientBuilder.create().build();
    DateTime startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(startTimeISO);
    DateTime endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(endTimeISO);
    // format http GET command
    StringBuilder urlBuilder = new StringBuilder(host + ":" + Integer.valueOf(port) + DEFAULT_PATH_TO_TIMESERIES);
    urlBuilder.append(DATASET + "=" + dataset + "&");
    urlBuilder.append(METRIC + "=" + metric + "&");
    urlBuilder.append(VIEW + "=" + DEFAULT_VIEW + "&");
    urlBuilder.append(TIME_START + "=" + Long.toString(startTime.getMillis()) + "&");
    urlBuilder.append(TIME_END + "=" + Long.toString(endTime.getMillis()) + "&");
    urlBuilder.append(GRANULARITY + "=" + timeGranularity.toString() + "&");
    if (dimensions != null || !dimensions.isEmpty()) {
      urlBuilder.append(DIMENSIONS + "=" + dimensions + "&");
    }
    if (filterJson != null || !filterJson.isEmpty()) {
      urlBuilder.append(FILTERS + "=" + URLEncoder.encode(filterJson, "UTF-8"));
    }

    HttpGet httpGet = new HttpGet(urlBuilder.toString());

    // Execute GET command
    httpGet.addHeader("User-Agent", "User");

    HttpResponse response = client.execute(httpGet);

    System.out.println("Response Code : " + response.getStatusLine().getStatusCode());

    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));

    StringBuffer content = new StringBuffer();
    String line = "";
    while ((line = rd.readLine()) != null) {
      content.append(line);
    }
    JSONObject jsonObject = JSONObject.parseObject(content.toString());
    JSONObject timeSeriesData = (JSONObject) jsonObject.get("timeSeriesData");
    JSONArray timeArray = (JSONArray) timeSeriesData.get("time");

    Map<String, Map<DateTime, Integer>> resultMap = new HashMap<>();
    for (String key : timeSeriesData.keySet()) {
      if (key.equalsIgnoreCase("time")) {
        continue;
      }
      Map<DateTime, Integer> entry = new HashMap<>();
      JSONArray observed = (JSONArray) timeSeriesData.get(key);
      for (int i = 0; i < timeArray.size(); i++) {
        long timestamp = (long) timeArray.get(i);
        int observedValue = (int) observed.get(i);
        entry.put(new DateTime(timestamp), observedValue);
      }
      resultMap.put(key, entry);
    }
    return resultMap;
  }
}
