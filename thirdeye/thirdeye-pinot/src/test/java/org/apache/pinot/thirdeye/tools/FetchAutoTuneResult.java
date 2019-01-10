/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.tools;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FetchAutoTuneResult {
  private static final Logger LOG = LoggerFactory.getLogger(FetchAutoTuneResult.class);

  private static final String CSVSUFFIX = ".csv";
  private static final Integer DEFAULT_NEXPECTEDANOMALIES = 10;
  private static final String AUTOTUNE = "AUTOTUNE";
  private static final String INITTUNE = "INITTUNE";

  /**
   * Tool to fetch auto tune results using auto tune endpoint based on collection;
   * evaluate alert filter using evaluation endpoint
   * @param args
   * args[0]: Mode of AutoTune, default value is: AUTOTUNE, (If initiate auto tuning, use INITTUNE)
   * args[1]: Start time of merged anomaly in ISO time format
   * args[2]: End time of merged anomaly in ISO time format
   * args[3]: Path to Persistence File
   * args[4]: Collection Name
   * args[6]: File directory (read and write file)
   * args[7]: Holiday periods to remove from dataset. Holiday starts in ISO time format: start1,start2,...
   * args[8]: Holiday periods to remove from dataset. Holiday ends in format: end1,end2,...
   * @throws Exception
   */
  public static void main(String[] args) throws Exception{

    if(args.length < 8){
      LOG.error("Error: Insufficient number of arguments", new IllegalArgumentException());
      return;
    }

    String AUTOTUNE_MODE = args[0];
    String STARTTIME = args[1];
    String ENDTIME = args[2];
    String path2PersistenceFile = args[3];
    String Collection = args[4];
    String DIR_TO_FILE = args[5];
    String holidayStarts = args[6];
    String holidayEnds = args[7];
    String authToken = "";
    AutoTuneAlertFilterTool executor = new AutoTuneAlertFilterTool(new File(path2PersistenceFile), authToken);
    List<Long> functionIds = executor.getAllFunctionIdsByCollection(Collection);

    if(AUTOTUNE_MODE.equals(AUTOTUNE)){
      reTuneAndEvalToCSVByCollection(executor, functionIds, STARTTIME, ENDTIME, holidayStarts, holidayEnds, DIR_TO_FILE + Collection + CSVSUFFIX);
    } else if (AUTOTUNE_MODE.equals(INITTUNE)){
      initFilterAndEvalToCSVByCollection(executor, functionIds, STARTTIME, ENDTIME, holidayStarts, holidayEnds, DIR_TO_FILE + Collection + CSVSUFFIX, authToken);
    } else {
      System.out.println("Error! no such autotune mode!");
    }

  }

  /**
   * Tool to retune alert filter
   * @param executor AutoTuneAlertFilterTool class
   * @param functionIds a list of functionIds to retune
   * @param STARTTIME start time to tune in ISO format
   * @param ENDTIME end time to tune in ISO format
   * @param holidayStarts holidays anomalies can be removed: the list of holiday starts
   * @param holidayEnds holidays anomalies can be removed: the list of holiday starts
   * @param fileName the file name to write to
   * @throws Exception
   */
  public static void reTuneAndEvalToCSVByCollection(AutoTuneAlertFilterTool executor, List<Long> functionIds,
      String STARTTIME, String ENDTIME, String holidayStarts, String holidayEnds, String fileName)
      throws Exception {
    Map<String, String> outputMap = new HashMap<>();
    for(Long functionId : functionIds){
      StringBuilder outputVal = new StringBuilder();

      // evaluate current alert filter
      String origEvals = executor.evalAnomalyFunctionAlertFilterToEvalNode(functionId, STARTTIME, ENDTIME, holidayStarts, holidayEnds).toCSVString();

      Long autotuneId = Long.valueOf(executor.getTunedAlertFilterByFunctionId(functionId, STARTTIME, ENDTIME, AUTOTUNE, holidayStarts, holidayEnds));

      boolean isUpdated = autotuneId != -1;
      String tunedEvals = "";
      if(isUpdated){
        // after tuning, evaluate tuned results by autotuneId
        tunedEvals = executor.evalAutoTunedAlertFilterToEvalNode(autotuneId, STARTTIME, ENDTIME, holidayStarts, holidayEnds).toCSVString();
      }

      outputVal.append(origEvals)
          .append(isUpdated)
          .append(",")
          .append(tunedEvals);

      outputMap.put(String.valueOf(functionId), outputVal.toString());
    }

    // write to file
    String header = "FunctionId" + "," + AutoTuneAlertFilterTool.EvaluationNode.getCSVSchema() + "," + "isModelUpdated" + "," +AutoTuneAlertFilterTool.EvaluationNode.getCSVSchema();
    executor.writeMapToCSV(outputMap, fileName, header);
    LOG.info("Write evaluations to file: {}", fileName);
  }

  /**
   * Tool to initiate alert filter
   * @param executor AutoTuneAlertFilterTool class
   * @param functionIds a list of functionIds to retune
   * @param STARTTIME start time to tune in ISO format
   * @param ENDTIME end time to tune in ISO format
   * @param holidayStarts holidays anomalies can be removed: the list of holiday starts
   * @param holidayEnds holidays anomalies can be removed: the list of holiday starts
   * @param fileName the file name to write to
   * @throws Exception
   */
  public static void initFilterAndEvalToCSVByCollection(AutoTuneAlertFilterTool executor, List<Long> functionIds,
      String STARTTIME, String ENDTIME, String holidayStarts, String holidayEnds, String fileName, String authToken) throws Exception {
    Map<String, String> outputMap = new HashMap<>();
    for (Long functionId : functionIds) {
      StringBuilder outputVal = new StringBuilder();
      // evaluate current alert filter
      String origEvals =
          executor.evalAnomalyFunctionAlertFilterToEvalNode(functionId, STARTTIME, ENDTIME, holidayStarts, holidayEnds).toCSVString();

      long autotuneId = Long.valueOf(
          executor.getInitAutoTuneByFunctionId(functionId, STARTTIME, ENDTIME, AUTOTUNE, DEFAULT_NEXPECTEDANOMALIES,
              holidayStarts, holidayEnds, authToken));

      boolean isUpdated = autotuneId != -1;
      String tunedEvals = "";
      if (isUpdated) {
        // after tuning, evaluate tuned results by autotuneId
        tunedEvals =
            executor.evalAutoTunedAlertFilterToEvalNode(autotuneId, STARTTIME, ENDTIME, holidayStarts, holidayEnds).toCSVString();
      }

      outputVal.append(origEvals).append(isUpdated).append(",").append(tunedEvals);

      outputMap.put(String.valueOf(functionId), outputVal.toString());
    }
    // write to file
    String header = "FunctionId" + "," + AutoTuneAlertFilterTool.EvaluationNode.getCSVSchema() + "," + "isModelUpdated" + "," +AutoTuneAlertFilterTool.EvaluationNode.getCSVSchema();
    executor.writeMapToCSV(outputMap, fileName, header);
    LOG.info("Write evaluations to file: {}", fileName);
  }

}
