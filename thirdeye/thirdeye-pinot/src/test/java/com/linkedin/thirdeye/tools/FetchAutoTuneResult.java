package com.linkedin.thirdeye.tools;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FetchAutoTuneResult {
  private static final Logger LOG = LoggerFactory.getLogger(FetchAutoTuneResult.class);

  private static final String CLONEDMAPSUFFIX = "ClonedMap";
  private static final String CSVSUFFIX = ".csv";
  private static final String UNDERSCORE = "_";

  /**
   * Tool to fetch auto tune results using auto tune endpoint based on collection;
   * evaluate alert filter using evaluation endpoint
   * @param args
   * args[0]: Type of AutoTune, default value is: AUTOTUNE
   * args[1]: Start time of merged anomaly in milliseconds
   * args[2]: End time of mmerged anomaly in milliseconds
   * args[3]: Path to Persistence File
   * args[4]: Collection Name
   * args[5]: true if clone function is needed. if false, need to have clone map file in directory
   * args[6]: File directory (read and write file)
   * @throws Exception
   */
  public static void main(String[] args) throws Exception{

    if(args.length < 7){
      LOG.error("Error: Insufficient number of arguments", new IllegalArgumentException());
      return;
    }

    String AUTOTUNE_TYPE = args[0];
    String STARTTIMEISO = args[1];
    String ENDTIMEISO = args[2];
    String path2PersistenceFile = args[3];
    String Collection = args[4];
    Boolean isCloneFunction = Boolean.valueOf(args[5]);
    String DIR_TO_FILE = args[6];

    CloneFunctionAndAutoTuneAlertFilterTool executor = new CloneFunctionAndAutoTuneAlertFilterTool(new File(path2PersistenceFile));

    String cloneMapName = Collection + UNDERSCORE + CLONEDMAPSUFFIX + CSVSUFFIX;
    // clone functions
    if (isCloneFunction) {
      Map<String, String> clonedFunctionIds = executor.cloneFunctionsToAutoTune(Collection);
      // write relationship to csv
      executor.writeMapToCSV(clonedFunctionIds, DIR_TO_FILE + cloneMapName);
      LOG.info("Cloned the set of funcitons and write to file: {}", cloneMapName);
    }

    // read relationship from csv to map
    Map<String, String> clonedMap = executor.readClonedMap(DIR_TO_FILE + cloneMapName);
    LOG.info("Read function mappings from file: {}", DIR_TO_FILE + cloneMapName);

    // getTuned
    Map<String, String> tunedResult = new HashMap<>();
    for(Map.Entry<String, String> pair: clonedMap.entrySet()) {
      String clonedFunctionId = pair.getValue();
      String isUpdated = executor.getTunedAlertFilterByFunctionId(clonedFunctionId, STARTTIMEISO, ENDTIMEISO, AUTOTUNE_TYPE);
      tunedResult.put(clonedFunctionId, isUpdated);
    }
    LOG.info("Tuned cloned functions");

    // evaluate prev, curr alert filter
    Map<String, String> outputMap = new HashMap<>();
    for (Map.Entry<String, String> pair: clonedMap.entrySet()) {
      StringBuilder outputVal = new StringBuilder(Collection + CloneFunctionAndAutoTuneAlertFilterTool.CSVSEPERATOR);

      //origin function info
      String origFunctionId = pair.getKey();
      String origEvals = executor.evalAlertFilterToCommaSeperateString(origFunctionId, STARTTIMEISO, ENDTIMEISO);

      //tuned function info
      String tunedFunctionId = pair.getValue();
      String tunedEvals = executor.evalAlertFilterToCommaSeperateString(tunedFunctionId, STARTTIMEISO, ENDTIMEISO);

      outputVal.append(origEvals)
          .append(tunedEvals)
          .append(tunedResult.get(tunedFunctionId));
      outputMap.put(origFunctionId, outputVal.toString());
    }

    // write to file
    executor.writeMapToCSV(outputMap, DIR_TO_FILE + Collection + CSVSUFFIX);
    LOG.info("Write evaluations to file: {}", DIR_TO_FILE + Collection + CSVSUFFIX);
  }
}
