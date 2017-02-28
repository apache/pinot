package com.linkedin.thirdeye.tools;


import com.linkedin.thirdeye.anomaly.utils.DetectionResourceHttpUtils;
import com.linkedin.thirdeye.anomaly.utils.OnboardResourceHttpUtils;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class to support fetching results from autotune endpoint, fetching evaluation results from evaluation endpoint
 * Also provide ways to write map to csv, read from csv to Map of Strings
 */
public class CloneFunctionAndAutoTuneAlertFilterTool {
  private static final Logger LOG = LoggerFactory.getLogger(CloneFunctionAndAutoTuneAlertFilterTool.class);
  public static AnomalyFunctionManager anomalyFunctionDAO;

  private static final int DASHBOARD_PORT = 1426;
  private static final int APPLICATION_PORT = 1867;
  private static final String LOCALHOST = "localhost";

  private static final String AUTOTUNE_TAG = "autotune";
  private static final Boolean isCloneAnomaly = true;
  public static String CSVSEPERATOR = ",";
  public static String CSVESCAPE = ";";

  public CloneFunctionAndAutoTuneAlertFilterTool(File persistenceFile){
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);

  }

  public String cloneFunctionToAutotune(String functionid, String autoTuneTag, boolean isCloneAnomaly) {
    OnboardResourceHttpUtils httpUtils = new OnboardResourceHttpUtils(LOCALHOST, DASHBOARD_PORT);
    try{
      return httpUtils.getClonedFunctionID(functionid, autoTuneTag, isCloneAnomaly);
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
    return null;
  }

  public Map<String, String> cloneFunctionsToAutoTune(String Collection){
    List<AnomalyFunctionDTO> anomalyFunctionSpecs = anomalyFunctionDAO.findAllByCollection(Collection);
    Map<String, String> clonedFunctionIds = new HashMap<>();
    for(AnomalyFunctionDTO anomalyFunctionSpec: anomalyFunctionSpecs) {
      String functionId = String.valueOf(anomalyFunctionSpec.getId());
      String clonedFunctionId = cloneFunctionToAutotune(functionId, AUTOTUNE_TAG, isCloneAnomaly);
      clonedFunctionIds.put(functionId, clonedFunctionId);
    }
    return clonedFunctionIds;
  }

  public String getTunedAlertFilterByFunctionId(String functionId, String startTimeISO, String endTimeISO, String AUTOTUNE_TYPE) throws Exception{
    DetectionResourceHttpUtils httpUtils = new DetectionResourceHttpUtils(LOCALHOST, APPLICATION_PORT);
    try {
      return httpUtils.runAutoTune(functionId, startTimeISO, endTimeISO, AUTOTUNE_TYPE);
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
    return null;
  }


  public String getAlertFilterEvaluation(String functionId, String startTimeISO, String endTimeISO){
    DetectionResourceHttpUtils httpUtils = new DetectionResourceHttpUtils(LOCALHOST, APPLICATION_PORT);
    try{
      return httpUtils.getEvalStatsAlertFilter(functionId, startTimeISO, endTimeISO);
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
    return null;
  }


  public void writeMapToCSV(Map<String, String> map, String fileName) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
    for (Map.Entry<String, String> pair : map.entrySet()) {
      bw.write(pair.getKey() + "," + pair.getValue());
      bw.newLine();
    }
    bw.close();
  }

  public Map<String, String> readClonedMap(String fileName) throws IOException {
    Map<String, String> clonedMap = new HashMap<>();
    BufferedReader br = new BufferedReader(new FileReader(fileName));
    String line;
    while ((line = br.readLine()) != null) {
      String[] values = line.split(",");
      if (values.length != 2) {
        throw new IllegalArgumentException("Illeagal file format!");
      }
      clonedMap.put(values[0], values[1]);
    }
    return clonedMap;
  }

  public String evalAlertFilterToCommaSeperateString(String functionID, String startTimeISO, String endTimeISO)
      throws IOException, JSONException {
    StringBuilder outputVal = new StringBuilder();
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(Long.valueOf(functionID));
    String metricName = anomalyFunctionSpec.getFunctionName();
    String metricAlertFilter = (anomalyFunctionSpec.getAlertFilter() == null)? "null": anomalyFunctionSpec.getAlertFilter().toString();
    outputVal.append(functionID)
        .append(CSVSEPERATOR)
        .append(metricName)
        .append(CSVSEPERATOR)
        .append(metricAlertFilter.replaceAll(CSVSEPERATOR, CSVESCAPE))
        .append(CSVSEPERATOR);

    String evals = getAlertFilterEvaluation(functionID, startTimeISO, endTimeISO);
    if(evals != null) {
      JSONObject evalJson = new JSONObject(evals);
      Iterator<String> nameltr = evalJson.keys();
      while (nameltr.hasNext()) {
        String eval = evalJson.getString(nameltr.next());
        outputVal.append(eval).append(CSVSEPERATOR);
      }
    } else {
      outputVal.append("null").append(CSVSEPERATOR);
    }
    return outputVal.toString();
  }
}
