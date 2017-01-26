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

public class PrintAnomaliesInRange {
  private static final String DEFAULT_OUTPUT_FOLDER = "/home/";

  private static AnomalyFunctionManager anomalyFunctionDAO;
  private static MergedAnomalyResultManager mergedAnomalyResultDAO;
  private static RawAnomalyResultManager rawAnomalyResultDAO;
  private static class ResultNode implements Comparable<ResultNode>{
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

    private String dimensionString(){
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

    try {
      init(new File(args[0]));
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

    List<AnomalyFunctionDTO> anomalyFunctions = anomalyFunctionDAO.findAllByCollection(collection);

    // Print Merged Results
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

    System.out.println("Printing merged anaomaly results from db...");
    String outputname = output_folder +
        "merged_" + args[1] + "_" + fmt.print(startTime) + "_" + fmt.print(endTime) + ".csv";
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(outputname));

      bw.write(String.join(",", resultNodes.get(0).getSchema()));
      bw.newLine();
      for (ResultNode n : resultNodes){
        bw.write(n.toString());
        bw.newLine();
      }
      bw.close();
    }
    catch (IOException e){
      System.out.println(e.getMessage());
    }
    System.out.println("Finish job of printing merged anaomaly results from db...");


    resultNodes.clear();
    // Print Raw Results
    System.out.println("Loading raw anaomaly results from db...");
    resultNodes = new ArrayList<>();
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

    System.out.println("Printing raw anaomaly results from db...");
    outputname = output_folder +
        "raw_" + args[1] + "_" + fmt.print(startTime) + "_" + fmt.print(endTime) + ".csv";
    try {
      BufferedWriter bw = new BufferedWriter(new FileWriter(outputname));

      bw.write(String.join(",", resultNodes.get(0).getSchema()));
      bw.newLine();
      for (ResultNode n : resultNodes){
        bw.write(n.toString());
        bw.newLine();
      }
      bw.close();
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
    for (ResultNode n : resultNodes){
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
