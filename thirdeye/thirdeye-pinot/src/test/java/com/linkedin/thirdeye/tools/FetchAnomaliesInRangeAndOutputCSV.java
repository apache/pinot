package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.integration.AnomalyApplicationEndToEndTest;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

public class FetchAnomaliesInRangeAndOutputCSV {
  private static final String DEFAULT_OUTPUT_FOLDER = "/home/";

  private static AnomalyFunctionManager anomalyFunctionDAO;
  private static MergedAnomalyResultManager mergedAnomalyResultDAO;
  private static RawAnomalyResultManager rawAnomalyResultDAO;

  public String dimensionString(DimensionMap dm){
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    if(!dm.isEmpty()) {
      for (Map.Entry<String, String> entry : dm.entrySet()) {
        sb.append(entry.getKey() + ":");
        sb.append(entry.getValue() + "|");
      }
      sb.deleteCharAt(sb.length() - 1);
    }
    sb.append("]");
    return sb.toString();
  }

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

    bw.write(String.join(",", resultNodes.get(0).getSchema()));
    bw.newLine();
    for (FetchMetricDataAndExistingAnomaliesTool.ResultNode n : resultNodes){
      bw.write(n.toString());
      bw.newLine();
    }
    bw.close();
  }
  /**
   * Ouput merged anomaly results for given metric and time range
   * @param args List of arguments
   *             0: path to persistence file
   *             1: collection name
   *             2: start time in ISO format
   *             3: end time in ISO format
   *             4: output file name
   *             5 (optional): Output path
   */
  public static void main(String args[]){
    if(args.length < 5){
      System.out.println("Insufficient number of arguments");
      return;
    }

    FetchMetricDataAndExistingAnomaliesTool thirdEyeDAO = null;
    try {
      thirdEyeDAO = new FetchMetricDataAndExistingAnomaliesTool(new File(args[0]));
    }
    catch (Exception e){
      System.out.println(e.getMessage());
      return;
    }

    String collection = args[1];
    String metric = args[2];
    String output_folder = DEFAULT_OUTPUT_FOLDER;
    DateTime startTime = ISODateTimeFormat.dateTimeParser().parseDateTime(args[3]);
    DateTime endTime = ISODateTimeFormat.dateTimeParser().parseDateTime(args[4]);
    DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd");
    if(args.length >= 5 && (new File(args[4])).exists()){
      output_folder = args[4];
    }



    // Print Merged Results
    List<FetchMetricDataAndExistingAnomaliesTool.ResultNode> resultNodes = thirdEyeDAO.fetchMergedAnomalies(collection, metric, args[3], args[4]);

    System.out.println("Printing merged anaomaly results from db...");
    String outputname = output_folder +
        "merged_" + args[1] + "_" + fmt.print(startTime) + "_" + fmt.print(endTime) + ".csv";
    try {
      outputResultNodesToFile(new File(outputname), resultNodes);
    }
    catch (IOException e){
      System.out.println(e.getMessage());
    }
    System.out.println("Finish job of printing merged anaomaly results from db...");


    resultNodes.clear();
    // Print Raw Results
    resultNodes = thirdEyeDAO.fetchRawAnomalies(collection, metric, args[3], args[4]);;

    System.out.println("Printing raw anaomaly results from db...");
    outputname = output_folder +
        "raw_" + args[1] + "_" + fmt.print(startTime) + "_" + fmt.print(endTime) + ".csv";
    try {
      outputResultNodesToFile(new File(outputname), resultNodes);
    }
    catch (IOException e){
      System.out.println(e.getMessage());
    }
    System.out.println("Finish job of printing raw anaomaly results from db...");

    // Print date vs dimension table
    outputname = output_folder +
        "date_dimension_" + args[1] + "_" + fmt.print(startTime) + "_" + fmt.print(endTime) + ".csv";

    Set<String> dimensions = new HashSet<>();
    Map<String, Map<String, Double>> dimensionDateSeverity = new HashMap<>();

    System.out.println("Loading date-dimension anaomaly results from db...");
    for (FetchMetricDataAndExistingAnomaliesTool.ResultNode n : resultNodes){
      String anomalyStartTime = fmt.print(n.startTime);
      if(!dimensionDateSeverity.containsKey(n.dimensionString())){
        dimensionDateSeverity.put(n.dimensionString(), new HashMap<String, Double>());
      }
      Map<String, Double> targetMap = dimensionDateSeverity.get(n.dimensionString());
      targetMap.put(anomalyStartTime, n.severity);
      dimensions.add(n.dimensionString());
    }

    System.out.println("Printing date-dimension anaomaly results from db...");
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(outputname));
      List<String> schemas = new ArrayList<>(dimensions);
      Collections.sort(schemas);

      // Write Schema
      for(DateTime curr = startTime; curr.isBefore(endTime); curr = curr.plusDays(1)){
        String currDate = fmt.print(curr);
        bw.write("," + currDate);
      }
      bw.newLine();

      for (String schema : schemas){
        bw.write(schema);
        Map<String, Double> targetMap = dimensionDateSeverity.get(schema);
        for (DateTime curr = startTime; curr.isBefore(endTime); curr = curr.plusDays(1)) {
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
      System.out.println(e.getMessage());
    }
    System.out.println("Finish job of printing date-dimension anaomaly results from db...");
  }

}
