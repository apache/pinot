package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.anomaly.utils.AbstractResourceHttpUtils;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyFeedback;
import com.linkedin.thirdeye.api.TimeGranularity;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpHost;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFeedbackDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import java.io.BufferedReader;
import java.io.File;
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
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FetchMetricDataAndExistingAnomaliesTool extends AbstractResourceHttpUtils{
  private static final Logger LOG = LoggerFactory.getLogger(FetchMetricDataAndExistingAnomaliesTool.class);
  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private RawAnomalyResultManager rawAnomalyResultDAO;

  public FetchMetricDataAndExistingAnomaliesTool(File persistenceFile) throws Exception{
    super(null);
    init(persistenceFile);
  }

  // Private class for storing and sorting results
  public class ResultNode implements Comparable<ResultNode>{
    long functionId;
    String functionName;
    private String filters;
    DimensionMap dimensions;
    DateTime startTime;
    DateTime endTime;
    double severity;
    double windowSize;
    AnomalyFeedbackType feedbackType;

    public ResultNode(){}

    public void setFilters(String filterStr){
      if(StringUtils.isBlank(filterStr)){
        filters = "";
        return;
      }
      String[] filterArray = filterStr.split(",");
      StringBuilder fs = new StringBuilder();
      fs.append(StringUtils.join(filterArray, ";"));
      this.filters = fs.toString();
    }

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
          "StartDate", "EndDate", "Dimensions", "Filters", "FunctionID", "FunctionName", "Severity", "WindowSize","feedbackType"
      };
    }
    public String toString(){
      DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
      return String.format("%s,%s,%s,%s,%s,%s,%s,%s", fmt.print(startTime), fmt.print(endTime),
          dimensionString(), (filters == null)? "":filters,
          Long.toString(functionId), functionName, Double.toString(severity),
          Double.toString(windowSize),
          (feedbackType == null)? "N/A" : feedbackType.toString());
    }
  }

  /**
   * Initialize DAOs
   * @param persistenceFile path to the persistence file
   * @throws Exception
   */
  public void init(File persistenceFile) throws Exception {
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    rawAnomalyResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl.class);
    mergedAnomalyResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl.class);
  }

  public AnomalyFunctionDTO getAnomalyFunctionDTO(long functionId) {
    return anomalyFunctionDAO.findById(functionId);
  }

  public List<ResultNode> fetchMergedAnomaliesInRangeByFunctionId(long functionId, DateTime startTime, DateTime endTime){
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    LOG.info("Loading merged anaomaly results of functionId {} from db...", functionId);
    List<ResultNode> resultNodes = new ArrayList<>();

    if(anomalyFunction == null){ // no such function
      return  resultNodes;
    }

    List<MergedAnomalyResultDTO> mergedResults =
        mergedAnomalyResultDAO.findByStartTimeInRangeAndFunctionId(startTime.getMillis(), endTime.getMillis(), functionId, true);
    for(MergedAnomalyResultDTO mergedResult : mergedResults){
      ResultNode res = new ResultNode();
      res.functionId = functionId;
      res.functionName = anomalyFunction.getFunctionName();
      res.startTime = new DateTime(mergedResult.getStartTime());
      res.endTime = new DateTime(mergedResult.getEndTime());
      res.dimensions = mergedResult.getDimensions();
      res.setFilters(anomalyFunction.getFilters());
      res.severity = mergedResult.getWeight();
      res.windowSize = 1.0 * (mergedResult.getEndTime() - mergedResult.getStartTime()) / 3600_000;
      AnomalyFeedback feedback = mergedResult.getFeedback();
      res.feedbackType = (feedback == null)? null : feedback.getFeedbackType();
      resultNodes.add(res);
    }
    return resultNodes;
  }
  /**
   * Fetch merged anomaly results from thirdeye db
   * @param collection database/collection name
   * @param metric metric name
   * @param startTime start time of the requested data in DateTime format
   * @param endTime end time of the requested data in DateTime format
   * @return List of merged anomaly results
   */
  public List<ResultNode> fetchMergedAnomaliesInRange (String collection, String metric, DateTime startTime, DateTime endTime){
    List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionDAO.findAllByCollection(collection);
    LOG.info("Loading merged anaomaly results from db...");
    List<ResultNode> resultNodes = new ArrayList<>();
    for(AnomalyFunctionDTO anomalyDto : anomalyFunctions){
      if(!anomalyDto.getTopicMetric().equals(metric)) continue;

      resultNodes.addAll(fetchMergedAnomaliesInRangeByFunctionId(anomalyDto.getId(), startTime, endTime));
    }
    Collections.sort(resultNodes);
    return resultNodes;
  }

  public List<ResultNode> fetchRawAnomaliesInRangeByFunctionId(long functionId, DateTime startTime, DateTime endTime){
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    LOG.info(String.format("Loading raw anaomaly results of functionId {} from db...", Long.toString(functionId)));
    List<ResultNode> resultNodes = new ArrayList<>();

    if(anomalyFunction == null){ // no such function
      return  resultNodes;
    }

    List<RawAnomalyResultDTO> rawResults =
        rawAnomalyResultDAO.findAllByTimeAndFunctionId(startTime.getMillis(), endTime.getMillis(), functionId);
    for(RawAnomalyResultDTO rawResult : rawResults){
      ResultNode res = new ResultNode();
      res.functionId = functionId;
      res.functionName = anomalyFunction.getFunctionName();
      res.startTime = new DateTime(rawResult.getStartTime());
      res.endTime = new DateTime(rawResult.getEndTime());
      res.dimensions = rawResult.getDimensions();
      res.setFilters(anomalyFunction.getFilters());
      res.severity = rawResult.getWeight();
      res.windowSize = 1.0 * (rawResult.getEndTime() - rawResult.getStartTime()) / 3600_000;
      AnomalyFeedbackDTO feedback = rawResult.getFeedback();
      res.feedbackType = (feedback == null)? null : feedback.getFeedbackType();
      resultNodes.add(res);
    }
    return resultNodes;
  }

  /**
   * Fetch raw anomaly results from thirdeye db
   * @param collection database/collection name
   * @param metric metric name
   * @param startTime start time of the requested data in DateTime format
   * @param endTime end time of the requested data in DateTime format
   * @return List of raw anomaly results
   */
  public List<ResultNode> fetchRawAnomaliesInRange(String collection, String metric, DateTime startTime, DateTime endTime){
    List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionDAO.findAllByCollection(collection);
    LOG.info("Loading raw anaomaly results from db...");
    List<ResultNode> resultNodes = new ArrayList<>();


    for(AnomalyFunctionDTO anomalyDto : anomalyFunctions){
      if(!anomalyDto.getTopicMetric().equals(metric)) continue;

      long id = anomalyDto.getId();
      resultNodes.addAll(fetchRawAnomaliesInRangeByFunctionId(id, startTime, endTime));
    }
    Collections.sort(resultNodes);
    return resultNodes;
  }

  /**
   * Fetch metric from thirdeye
   * @param host host name (includes http://)
   * @param port port number
   * @param dataset dataset/collection name
   * @param metric metric name
   * @param startTime start time of requested data in DateTime
   * @param endTime end time of requested data in DateTime
   * @param timeGranularity the time granularity
   * @param dimensions the list of dimensions
   * @param filterJson filters, in JSON
   * @return {dimension-> {DateTime: value}}
   * @throws IOException
   */
  public Map<String, Map<Long, String>> fetchMetric(String host, int port, String authToken, String dataset, String metric, DateTime startTime,
      DateTime endTime, TimeUnit timeUnit, String dimensions, String filterJson, String timezone)
      throws  Exception{
    DashboardHttpUtils httpUtils = new DashboardHttpUtils(host, port, authToken);
    String content = httpUtils.handleMetricViewRequest(dataset, metric, startTime, endTime, timeUnit, dimensions, filterJson, timezone);
    Map<String, Map<Long, String>> resultMap = null;
    try {
      JSONObject jsonObject = new JSONObject(content);
      JSONObject timeSeriesData = (JSONObject) jsonObject.get("timeSeriesData");
      JSONArray timeArray = (JSONArray) timeSeriesData.get("time");

      resultMap = new HashMap<>();
      Iterator<String> timeSeriesDataIterator = timeSeriesData.keys();
      while(timeSeriesDataIterator.hasNext()) {
        String key = timeSeriesDataIterator.next();
        if (key.equalsIgnoreCase("time")) {
          continue;
        }
        Map<Long, String> entry = new HashMap<>();
        JSONArray observed = (JSONArray) timeSeriesData.get(key);
        for (int i = 0; i < timeArray.length(); i++) {
          long timestamp = (long) timeArray.get(i);
          String observedValue = observed.get(i).toString();
          entry.put(timestamp, observedValue);
        }
        resultMap.put(key, entry);
      }
    }
    catch (JSONException e){
      LOG.error("Unable to resolve JSON string {}", e);
    }
    return resultMap;
  }
}
