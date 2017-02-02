package com.linkedin.thirdeye.tools;

import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.linkedin.thirdeye.tools.FetchMetricDataAndExistingAnomaliesTool.TimeGranularity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FetchMetricDataInRangeAndOutputCSV {
  private static final Logger LOG = LoggerFactory.getLogger(FetchAnomaliesInRangeAndOutputCSV.class);
  private static final String DEFAULT_HOST = "http://localhost";
  private static final String DEFAULT_PORT = "1426";

  /**
   * Fetch metric historical data from server and parse the json object
   * @param args List of arguments
   *             0: Path to the persistence file
   *             1: dataset/collection name
   *             2: metric name
   *             3: retrieval start time in ISO format, e.g. 2016-01-01T12:00:00
   *             4: timezone code
   *             5: Duration
   *             6: Aggregation time granularity, DAYS, HOURS,
   *             7: dimensions drill down, ex: product,channel
   *             8: filter in json format, ex: {"channel":["guest-email","guest-sms"]}
   *             9: Output path
   */
  public static void main(String[] args){
    if(args.length < 10){
      System.out.println("Error: Insufficient number of arguments");
      return;
    }


    // Put arguments in Map
    Map<String, String> argMap = new HashMap<>();
    argMap.put("persistenceFile", args[0]);
    argMap.put("collectionName", args[1]);
    argMap.put("metricName", args[2]);
    argMap.put("monitoringTime", args[3]);
    argMap.put("timezone", args[4]);
    argMap.put("monitorLength", args[5]);
    argMap.put("timeGranularity", args[6]);
    argMap.put("dimensions", args[7]);
    argMap.put("filterJson", args[8]);
    argMap.put("outputPath", args[9]);

    String path2PersistenceFile = argMap.get("persistenceFile");
    String dataset = argMap.get("collectionName");;
    String metric = argMap.get("metricName");;
    String aggTimeGranularity = argMap.get("timeGranularity");;
    TimeGranularity timeGranularity = TimeGranularity.fromString(aggTimeGranularity);
    File output_folder = new File(argMap.get("outputPath"));

    if(!output_folder.exists() || !output_folder.canWrite()){
      LOG.error("{} is not accessible", output_folder.getAbsoluteFile());
      return;
    }

    if(timeGranularity == null){
      LOG.error("Illegal time granularity");
      return;
    }

    // Training data range
    Period period = null;
    switch (timeGranularity) {
      case DAYS:
        period = new Period(0, 0, 0, Integer.valueOf(argMap.get("monitorLength")), 0, 0, 0, 0);
        break;
      case HOURS:
        period = new Period(0, 0, 0, 0, Integer.valueOf(argMap.get("monitorLength")), 0, 0, 0);
        break;
      case MINUTES:
        period = new Period(0, 0, 0, 0, 0, Integer.valueOf(argMap.get("monitorLength")), 0, 0);
        break;

    }
    DateTimeZone dateTimeZone = DateTimeZone.forID(argMap.get("timezone"));
    DateTime monitoringWindowStartTime = ISODateTimeFormat.dateTimeParser()
        .parseDateTime(argMap.get("monitoringTime")).withZone(dateTimeZone);
    DateTime dataRangeStart = monitoringWindowStartTime.minus(period); // inclusive start
    DateTime dataRangeEnd = monitoringWindowStartTime; // exclusive end
    String dimensions = argMap.get("dimensions");
    String filters = argMap.get("filterJson");
    DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");



    String fname = output_folder.getAbsolutePath() + metric + "_" + fmt.print(dataRangeStart) + "_" + fmt.print(dataRangeEnd) + ".csv";
    Map<String, Map<Long, String>> metricContent;
    try {
      FetchMetricDataAndExistingAnomaliesTool thirdEyeDAO = new FetchMetricDataAndExistingAnomaliesTool(new File(path2PersistenceFile));
      metricContent = thirdEyeDAO.fetchMetric(DEFAULT_HOST, Integer.valueOf(DEFAULT_PORT), dataset,
          metric, dataRangeStart, dataRangeEnd,
          FetchMetricDataAndExistingAnomaliesTool.TimeGranularity.fromString(aggTimeGranularity), dimensions,
          filters, dateTimeZone.getID());

      BufferedWriter bw = new BufferedWriter(new FileWriter(fname));

      List<String> keys = new ArrayList<>(metricContent.keySet());
      List<Long> dateTimes = new ArrayList<>(metricContent.get(keys.get(0)).keySet());
      Collections.sort(dateTimes);

      // Print Header
      for(String str: keys){
        bw.write("," + str);
      }
      bw.newLine();

      for(Long dt : dateTimes){
        bw.write(Long.toString(dt));
        for(String key : keys){
          Map<Long, String> map = metricContent.get(key);
          bw.write("," + map.get(dt));
        }
        bw.newLine();
      }
      bw.close();
    }
    catch (Exception e){
      System.out.println(e.getMessage());
    }
  }
}
