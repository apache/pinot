package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FetchAnomaliesInRangeAndOutputCSV {
  private static final Logger LOG = LoggerFactory.getLogger(FetchAnomaliesInRangeAndOutputCSV.class);
  private static AnomalyFunctionManager anomalyFunctionDAO;
  private static MergedAnomalyResultManager mergedAnomalyResultDAO;
  private static RawAnomalyResultManager rawAnomalyResultDAO;

  public static void init(File persistenceFile) throws Exception {
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    rawAnomalyResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl.class);
    mergedAnomalyResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl.class);
  }

  public static void outputResultNodesToFile(File outputFile, List<FetchMetricDataAndExistingAnomaliesTool.ResultNode> resultNodes)
    throws IOException{
    BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));

    int rowCount = 0;
    if(resultNodes.size() > 0) {
      bw.write(String.join(",", resultNodes.get(0).getSchema()));
      bw.newLine();
      for (FetchMetricDataAndExistingAnomaliesTool.ResultNode n : resultNodes) {
        bw.write(n.toString());
        bw.newLine();
        rowCount++;
      }
      LOG.info("{} anomaly results has been written...", rowCount);
    }
    bw.close();
  }
  /**
   * Ouput merged anomaly results for given metric and time range
   * @param args List of arguments
   *             0: path to persistence file
   *             1: collection name
   *             2: metric name
   *             3: monitoring start time in ISO format
   *             4: timezone code
   *             5: monitoring length in days
   *             6: (optional): Output path
   */
  public static void main(String args[]){
    if(args.length < 7){
      LOG.error("Insufficient number of arguments");
      return;
    }

    FetchMetricDataAndExistingAnomaliesTool thirdEyeDAO = null;
    try {
      thirdEyeDAO = new FetchMetricDataAndExistingAnomaliesTool(new File(args[0]));
    }
    catch (Exception e){
      LOG.error("Error in loading the persistence file: {}", e);
      return;
    }

    String collection = args[1];
    String metric = args[2];
    String output_folder = args[6];
    DateTimeZone dateTimeZone = DateTimeZone.forID(args[4]);
    DateTime monitoringWindowStartTime = ISODateTimeFormat.dateTimeParser().parseDateTime(args[3]).withZone(dateTimeZone);
    Period period = new Period(0, 0, 0, Integer.valueOf(args[5]), 0, 0, 0, 0);
    DateTime dataRangeStart = monitoringWindowStartTime.minus(period); // inclusive start
    DateTime dataRangeEnd = monitoringWindowStartTime; // exclusive end

    if(args.length >= 6 && (new File(args[5])).exists()){
      output_folder = args[5];
    }

    DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");

    // Print Merged Results
    List<FetchMetricDataAndExistingAnomaliesTool.ResultNode> resultNodes = thirdEyeDAO.fetchMergedAnomaliesInRange(
        collection, metric, dataRangeStart, dataRangeEnd);

    LOG.info("Printing merged anaomaly results from db...");
    String outputname = output_folder +
        "merged_" + args[1] + "_" + fmt.print(dataRangeStart) + "_" + fmt.print(dataRangeEnd) + ".csv";
    try {
      outputResultNodesToFile(new File(outputname), resultNodes);
    }
    catch (IOException e){
      LOG.error("Unable to output merged anomaly results to given file {}", e);
    }
    LOG.info("Finish job of printing merged anaomaly results from db...");


    resultNodes.clear();
    // Print Raw Results
    resultNodes = thirdEyeDAO.fetchRawAnomaliesInRange(collection, metric, dataRangeStart, dataRangeEnd);

    LOG.info("Printing raw anaomaly results from db...");
    outputname = output_folder +
        "raw_" + args[1] + "_" + fmt.print(dataRangeStart) + "_" + fmt.print(dataRangeEnd) + ".csv";
    try {
      outputResultNodesToFile(new File(outputname), resultNodes);
    }
    catch (IOException e){
      LOG.error("Unable to output raw anomaly results to given file {}", e);
    }
    LOG.info("Finish job of printing raw anaomaly results from db...");

    // Print date vs dimension table
    outputname = output_folder +
        "date_dimension_" + args[1] + "_" + fmt.print(dataRangeStart) + "_" + fmt.print(dataRangeEnd) + ".csv";

    Set<String> dimensions = new HashSet<>();
    Map<String, Map<String, Double>> dimensionDateSeverity = new HashMap<>();

    LOG.info("Loading date-dimension anaomaly results from db...");
    for (FetchMetricDataAndExistingAnomaliesTool.ResultNode n : resultNodes){
      String anomalyStartTime = fmt.print(n.startTime);
      if(!dimensionDateSeverity.containsKey(n.dimensionString())){
        dimensionDateSeverity.put(n.dimensionString(), new HashMap<String, Double>());
      }
      Map<String, Double> targetMap = dimensionDateSeverity.get(n.dimensionString());
      targetMap.put(anomalyStartTime, n.severity);
      dimensions.add(n.dimensionString());
    }

    LOG.info("Printing date-dimension anaomaly results from db...");
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(outputname));
      List<String> schemas = new ArrayList<>(dimensions);
      Collections.sort(schemas);

      // Write Schema
      for(DateTime curr = dataRangeStart; curr.isBefore(dataRangeEnd); curr = curr.plusDays(1)){
        String currDate = fmt.print(curr);
        bw.write("," + currDate);
      }
      bw.newLine();

      for (String schema : schemas){
        bw.write(schema);
        Map<String, Double> targetMap = dimensionDateSeverity.get(schema);
        for (DateTime curr = dataRangeStart; curr.isBefore(dataRangeEnd); curr = curr.plusDays(1)) {
          String currDate = fmt.print(curr);
          bw.write(",");
          if(targetMap.containsKey(currDate)){
            bw.write(Double.toString(targetMap.get(currDate)*100));
          }
        }
        bw.newLine();
      }
      bw.close();
    }
    catch (IOException e){
      LOG.error("Unable to write date-dimension anomaly results to given file {}", e);
    }
    LOG.info("Finish job of printing date-dimension anaomaly results from db...");
  }

}
