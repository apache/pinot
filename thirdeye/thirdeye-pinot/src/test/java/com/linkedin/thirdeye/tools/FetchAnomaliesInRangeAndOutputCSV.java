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
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;


public class FetchAnomaliesInRangeAndOutputCSV {
  private static final Logger LOG = LoggerFactory.getLogger(FetchAnomaliesInRangeAndOutputCSV.class);
  private static AnomalyFunctionManager anomalyFunctionDAO;
  private static MergedAnomalyResultManager mergedAnomalyResultDAO;
  private static RawAnomalyResultManager rawAnomalyResultDAO;
  private static DateTime dataRangeStart;
  private static DateTime dataRangeEnd;
  private static DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm");

  public static void init(File persistenceFile) throws Exception {
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    rawAnomalyResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.RawAnomalyResultManagerImpl.class);
    mergedAnomalyResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl.class);
  }

  public static void outputResultNodesToFile(File outputFile,
      List<FetchMetricDataAndExistingAnomaliesTool.ResultNode> resultNodes){
    try{
      BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));

      int rowCount = 0;
      if(resultNodes.size() > 0) {
        bw.write(StringUtils.join(resultNodes.get(0).getSchema(), ","));
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
    catch (IOException e){
      LOG.error("Unable to write date-dimension anomaly results to given file {}", e);
    }
  }

  public static void outputDimensionDateTableToFile(File outputFile,
      List<FetchMetricDataAndExistingAnomaliesTool.ResultNode> resultNodes){
    Set<String> functionIdDimension = new HashSet<>();
    Map<String, Map<String, Double>> functionIdDimension_VS_Date_Severity = new HashMap<>();

    LOG.info("Loading date-dimension anomaly results from db...");

    for (FetchMetricDataAndExistingAnomaliesTool.ResultNode n : resultNodes){
      String key = n.functionId + "," + n.dimensionString();
      String anomalyStartTime = dateTimeFormatter.print(n.startTime);
      if(!functionIdDimension_VS_Date_Severity.containsKey(key)){
        functionIdDimension_VS_Date_Severity.put(key, new HashMap<String, Double>());
      }
      Map<String, Double> targetMap = functionIdDimension_VS_Date_Severity.get(key);
      targetMap.put(anomalyStartTime, n.severity);
      functionIdDimension.add(key);
    }

    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
      List<String> schemas = new ArrayList<>(functionIdDimension);
      Collections.sort(schemas);

      LOG.info("Printing raw anomaly results from db...");

      // Write Schema
      bw.write("functionId,dimension");
      for (DateTime curr = dataRangeStart; curr.isBefore(dataRangeEnd); curr = curr.plusDays(1)) {
        String currDate = dateTimeFormatter.print(curr);
        bw.write("," + currDate);
      }
      bw.newLine();

      for (String schema : schemas) {
        bw.write(schema);
        Map<String, Double> targetMap = functionIdDimension_VS_Date_Severity.get(schema);
        for (DateTime curr = dataRangeStart; curr.isBefore(dataRangeEnd); curr = curr.plusDays(1)) {
          String currDate = dateTimeFormatter.print(curr);
          bw.write(",");
          if (targetMap.containsKey(currDate)) {
            bw.write(Double.toString(targetMap.get(currDate) * 100));
          }
        }
        bw.newLine();
      }
      bw.close();
    }
    catch (IOException e){
      LOG.error("Unable to write date-dimension anomaly results to given file {}", e);
    }
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
   *             6: Output path
   */
  public static void main(String args[]){
    if(args.length < 7){
      LOG.error("Insufficient number of arguments");
      return;
    }

    String persistencePath = args[0];
    String collection = args[1];
    String metric = args[2];
    String monitoringDateTime = args[3];
    DateTimeZone dateTimeZone = DateTimeZone.forID(args[4]);
    int monitoringLength = Integer.valueOf(args[5]);
    File output_folder = new File(args[6]);

    FetchMetricDataAndExistingAnomaliesTool thirdEyeDAO = null;
    try {
      thirdEyeDAO = new FetchMetricDataAndExistingAnomaliesTool(new File(persistencePath));
    }
    catch (Exception e){
      LOG.error("Error in loading the persistence file: {}", e);
      return;
    }

    DateTime monitoringWindowStartTime = ISODateTimeFormat.dateTimeParser().parseDateTime(monitoringDateTime).withZone(dateTimeZone);
    Period period = new Period(0, 0, 0, monitoringLength, 0, 0, 0, 0);
    dataRangeStart = monitoringWindowStartTime.minus(period); // inclusive start
    dataRangeEnd = monitoringWindowStartTime; // exclusive end

    if(!output_folder.exists() || !output_folder.canWrite()){
      LOG.error("{} is not accessible", output_folder.getAbsoluteFile());
      return;
    }


    // Print Merged Results
    List<FetchMetricDataAndExistingAnomaliesTool.ResultNode> resultNodes = thirdEyeDAO.fetchMergedAnomaliesInRange(
        collection, metric, dataRangeStart, dataRangeEnd);

    LOG.info("Printing merged anomaly results from db...");
    String outputname = output_folder.getAbsolutePath() + "/" +
        "merged_" + metric + "_" + dateTimeFormatter.print(dataRangeStart) +
        "_" + dateTimeFormatter.print(dataRangeEnd) + ".csv";
    outputResultNodesToFile(new File(outputname), resultNodes);
    LOG.info("Finish job and print merged anomaly results from db in {}...", outputname);


    resultNodes.clear();
    // Print Raw Results
    resultNodes = thirdEyeDAO.fetchRawAnomaliesInRange(collection, metric, dataRangeStart, dataRangeEnd);

    LOG.info("Printing raw anomaly results from db...");
    outputname = output_folder.getAbsolutePath() + "/" +
        "raw_" + metric + "_" + dateTimeFormatter.print(dataRangeStart) +
        "_" + dateTimeFormatter.print(dataRangeEnd) + ".csv";
    outputResultNodesToFile(new File(outputname), resultNodes);
    LOG.info("Finish job and print raw anomaly results from db in {}...", outputname);

    // Print date vs dimension table
    outputname = output_folder.getAbsolutePath() + "/" +
        "date_dimension_" + metric + "_" + dateTimeFormatter.print(dataRangeStart) +
        "_" + dateTimeFormatter.print(dataRangeEnd) + ".csv";

    LOG.info("Printing date-dimension anomaly results from db...");
    outputDimensionDateTableToFile(new File(outputname), resultNodes);
    LOG.info("Finish job and print date-dimension anomaly results from db in {}...", outputname);
    return;
  }

}
