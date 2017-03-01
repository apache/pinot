package com.linkedin.thirdeye.tools;

import java.io.File;
import java.util.Collections;
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
   * args[7]: true if merged anomalies on holidays should be removed. Need to provide holiday file name
   * args[8]: Optional: holiday file name. Holiday file format: {startTime},{endTime}
   * @throws Exception
   */
  public static void main(String[] args) throws Exception{

    if(args.length < 8){
      LOG.error("Error: Insufficient number of arguments", new IllegalArgumentException());
      return;
    }

    String AUTOTUNE_TYPE = args[0];
    Long STARTTIMEISO = Long.valueOf(args[1]);
    Long ENDTIMEISO = Long.valueOf(args[2]);
    String path2PersistenceFile = args[3];
    String Collection = args[4];
    Boolean isCloneFunction = Boolean.valueOf(args[5]);
    String DIR_TO_FILE = args[6];
    Boolean isRemoveHoliday = Boolean.valueOf(args[7]);
    String holidayFileName = null;

    if (isRemoveHoliday) {
      if (args.length < 9) {
        LOG.error("Error: Should provide holiday file", new IllegalArgumentException());
      } else {
        holidayFileName = args[8];
      }
    }

    CloneFunctionAndAutoTuneAlertFilterTool executor = new CloneFunctionAndAutoTuneAlertFilterTool(new File(path2PersistenceFile));

    String cloneMapName = Collection + UNDERSCORE + CLONEDMAPSUFFIX + CSVSUFFIX;
    // clone functions
    if (isCloneFunction) {
      Map<Long, Long> clonedFunctionIds = executor.cloneFunctionsToAutoTune(Collection, isRemoveHoliday, DIR_TO_FILE + holidayFileName);
      // write relationship to csv
      executor.writeMapToCSV(Collections.unmodifiableMap(clonedFunctionIds), DIR_TO_FILE + cloneMapName);
      LOG.info("Cloned the set of funcitons and write to file: {}", cloneMapName);
    }

    // read relationship from csv to map
    Map<Long, Long> clonedMap = executor.readTwoColumnsCSVToMap(DIR_TO_FILE + cloneMapName);
    LOG.info("Read function mappings from file: {}", DIR_TO_FILE + cloneMapName);

    // getTuned and Evaluate prev, curr alert filter
    Map<Long, Boolean> tunedResult = new HashMap<>();
    Map<Long, String> outputMap = new HashMap<>();
    for(Map.Entry<Long, Long> pair: clonedMap.entrySet()) {

      StringBuilder outputVal = new StringBuilder(Collection + CloneFunctionAndAutoTuneAlertFilterTool.CSVSEPERATOR);

      Long clonedFunctionId = pair.getValue();

      //before tuning, evaluate current
      String origEvals = executor.evalAlertFilterToCommaSeperateString(clonedFunctionId, STARTTIMEISO, ENDTIMEISO);

      // tune by functionId
      Boolean isUpdated = Boolean.valueOf(executor.getTunedAlertFilterByFunctionId(clonedFunctionId, STARTTIMEISO, ENDTIMEISO, AUTOTUNE_TYPE));

      // after tuning, evaluate tuned
      String tunedEvals = executor.evalAlertFilterToCommaSeperateString(clonedFunctionId, STARTTIMEISO, ENDTIMEISO);

      outputVal.append(origEvals)
          .append(tunedEvals)
          .append(isUpdated);

      outputMap.put(pair.getKey(), outputVal.toString());
      tunedResult.put(clonedFunctionId, isUpdated);
    }
    LOG.info("Tuned cloned functions and constructed evaluations");

    // write to file
    executor.writeMapToCSV(Collections.unmodifiableMap(outputMap), DIR_TO_FILE + Collection + CSVSUFFIX);
    LOG.info("Write evaluations to file: {}", DIR_TO_FILE + Collection + CSVSUFFIX);
  }
}
