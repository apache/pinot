package com.linkedin.thirdeye.tools;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;


public class FetchMetricDataInRangeAndOutputCSV {
  private static final String DEFAULT_OUTPUT_FOLDER = "/home/";
  private static final String DEFAULT_HOST = "http://localhost";
  private static final String DEFAULT_PORT = "1426";
  private static final String DEFAULT_PATH_TO_TIMESERIES = "/dashboard/data/timeseries?";

  private static final String DATASET = "dataset";
  private static final String METRIC = "metrics";
  private static final String VIEW = "view";
  private static final String DEFAULT_VIEW = "timeseries";
  private static final String TIME_START = "currentStart";
  private static final String TIME_END = "currentEnd";
  private static final String GRANULARITY = "aggTimeGranularity";
  private static final String DIMENSIONS = "dimensions"; // separate by comma
  private static final String FILTERS = "filters";

  /**
   * Fetch metric historical data from server and parse the json object
   * @param args List of arguments
   *             0: dataset/collection name
   *             1: metric name
   *             2: retrieval start time in ISO format, e.g. 2016-01-01T12:00:00
   *             3: retrieval end time in ISO format
   *             4: Aggregation time granularity, DAYS, HOURS,
   *             5: dimensions drill down, ex: product,channel
   *             6: filter in json format, ex: {"channel":["guest-email","guest-sms"]}
   */
  public static void main(String[] args){
//    if(args.length < 5){
//      System.out.println("Error: Insufficient number of arguments");
//      return;
//    }
    String path2PersistenceFile = "/home/ychung/workspace/thirdeye-configs/local-configs/persistence.yml";
    String dataset = "invite_sends_v2_additive";
    String metric = "m2g_invite_sent";
    String currentStartISO = "2016-01-01T12:00:00";
    String currentEndISO = "2017-01-01T12:00:00";
    String aggTimeGranularity = "DAYS";
    String dimensions = "product";
    String filters = "{\"channel\":[\"guest-email\",\"guest-sms\"]}";
    DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");

    if(!aggTimeGranularity.equals("DAYS") && !aggTimeGranularity.equals("HOURS")){
      System.out.println("Illegal time granularity");
      return;
    }


    String filename = metric + "_" + currentStartISO + "_" + currentEndISO + ".csv";
    Map<String, Map<DateTime, Integer>> metricContent;
    try {
      FetchMetricDataAndExistingAnomaliesTool thirdEyeDAO = new FetchMetricDataAndExistingAnomaliesTool(new File(path2PersistenceFile));
      metricContent = thirdEyeDAO.fetchMetric(DEFAULT_HOST, Integer.valueOf(DEFAULT_PORT), dataset,
          metric, currentStartISO, currentEndISO, FetchMetricDataAndExistingAnomaliesTool.TimeGranularity.DAYS, dimensions, filters);
    }
    catch (Exception e){
      System.out.println(e.getMessage());
    }




  }
}
