package com.linkedin.thirdeye.tools;


import com.linkedin.thirdeye.anomaly.utils.DetectionResourceHttpUtils;
import com.linkedin.thirdeye.anomaly.utils.OnboardResourceHttpUtils;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterEvaluationUtil;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang.StringUtils;
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
  private static final Boolean IS_CLONE_ANOMALY = true;
  public static String CSVSEPERATOR = ",";
  public static String CSVESCAPE = ";";


  public CloneFunctionAndAutoTuneAlertFilterTool(File persistenceFile){
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
  }


  public static class EvaluationNode{

    private Properties alertFilterEval;

    private static final String FUNCTIONID = "functionId";
    private static final String METRICNAME = "metricName";
    private static final String ALERTFILTERSTR = "alertFilterStr";
    private static final String COLLECTION = "collection";

    private static List<String> getHeaders(){
      List<String> headers = new ArrayList<>();
      headers.addAll(Arrays.asList(COLLECTION, METRICNAME, FUNCTIONID, ALERTFILTERSTR));
      headers.addAll(AlertFilterEvaluationUtil.getPropertyNames());
      return headers;
    }

    public EvaluationNode(Long functionId, String metricName, String collection, String alertFilterStr, Properties alertFilterEvaluations){

      alertFilterEvaluations.put(FUNCTIONID, functionId);
      alertFilterEvaluations.put(METRICNAME, metricName);
      alertFilterEvaluations.put(COLLECTION, collection);
      alertFilterEvaluations.put(ALERTFILTERSTR, alertFilterStr);

      this.alertFilterEval = alertFilterEvaluations;
    }

    public String toCSVString(){
      StringBuilder res = new StringBuilder();
      for(String header : getHeaders()){
        res.append(alertFilterEval.get(header))
        .append(CSVSEPERATOR);
      }
      return res.toString();
    }

    public static String getCSVSchema(){
      return StringUtils.join(getHeaders(), CSVSEPERATOR);
    }
  }


  public Long cloneFunctionToAutotune(Long functionid, String autoTuneTag, boolean isCloneAnomaly) {
    OnboardResourceHttpUtils httpUtils = new OnboardResourceHttpUtils(LOCALHOST, DASHBOARD_PORT);
    try{
      return Long.valueOf(httpUtils.getClonedFunctionID(functionid, autoTuneTag, isCloneAnomaly));
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
    return null;
  }

  public String callRemoveMergedAnomalies(Long functionId, Long startTime, Long endTime){
    OnboardResourceHttpUtils httpUtils = new OnboardResourceHttpUtils(LOCALHOST, DASHBOARD_PORT);
    try {
      return httpUtils.removeMergedAnomalies(functionId, startTime, endTime);
    } catch (IOException e) {
      LOG.warn(e.getMessage());
    }
    return null;
  }

  public Map<String, String> cloneFunctionsToAutoTune(String Collection, Boolean isRemoveHoliday, String holidayFileName){
    List<AnomalyFunctionDTO> anomalyFunctionSpecs = anomalyFunctionDAO.findAllByCollection(Collection);
    Map<String, String> clonedFunctionIds = new HashMap<>();
    for(AnomalyFunctionDTO anomalyFunctionSpec: anomalyFunctionSpecs) {
      Long functionId = anomalyFunctionSpec.getId();
      Long clonedFunctionId = cloneFunctionToAutotune(functionId, AUTOTUNE_TAG, IS_CLONE_ANOMALY);

      // remove holiday for cloned functionId
      if (isRemoveHoliday) {
        try {
          Map<Long, Long> holidayMap = readTwoColumnsCSVToMap(holidayFileName);
          for (Map.Entry<Long, Long> pair: holidayMap.entrySet()) {
            callRemoveMergedAnomalies(clonedFunctionId, pair.getKey(), pair.getValue());
          }
        } catch (Exception e) {
          LOG.warn("Error for holiday removal", e.getMessage());
        }
      }
      clonedFunctionIds.put(String.valueOf(functionId), String.valueOf(clonedFunctionId));
    }
    return clonedFunctionIds;
  }

  public String getTunedAlertFilterByFunctionId(Long functionId, Long startTimeISO, Long endTimeISO, String AUTOTUNE_TYPE) throws Exception{
    DetectionResourceHttpUtils httpUtils = new DetectionResourceHttpUtils(LOCALHOST, APPLICATION_PORT);
    try {
      return httpUtils.runAutoTune(functionId, startTimeISO, endTimeISO, AUTOTUNE_TYPE);
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
    return null;
  }


  public String getAlertFilterEvaluation(Long functionId, Long startTime, Long endTime){
    DetectionResourceHttpUtils httpUtils = new DetectionResourceHttpUtils(LOCALHOST, APPLICATION_PORT);
    try{
      return httpUtils.getEvalStatsAlertFilter(functionId, startTime, endTime);
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
    return null;
  }


  public void writeMapToCSV(Map<String, String> map, String fileName, String headers) throws IOException {
    BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
    if (headers != null){
      bw.write(headers);
      bw.newLine();
    }
    for (Map.Entry<String, String> pair : map.entrySet()) {
      bw.write(pair.getKey() + "," + pair.getValue());
      bw.newLine();
    }
    bw.close();
  }

  public Map<Long, Long> readTwoColumnsCSVToMap(String fileName) throws IOException {
    Map<Long, Long> longLongMap = new HashMap<>();
    BufferedReader br = new BufferedReader(new FileReader(fileName));
    String line;
    while ((line = br.readLine()) != null) {
      String[] values = line.split(",");
      if (values.length != 2) {
        throw new IllegalArgumentException("Illegal file format!");
      }
      longLongMap.put(Long.valueOf(values[0]), Long.valueOf(values[1]));
    }
    return longLongMap;
  }

  public EvaluationNode evalAlertFilterToEvalNode(Long functionID, Long startTime, Long endTime)
      throws IOException, JSONException {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(functionID);
    String metricName = anomalyFunctionSpec.getFunctionName();
    String collection = anomalyFunctionSpec.getCollection();
    String metricAlertFilter = (anomalyFunctionSpec.getAlertFilter() == null)? "null": anomalyFunctionSpec.getAlertFilter().toString().replaceAll(CSVSEPERATOR, CSVESCAPE);
    Properties alertFilterEvaluations = new Properties();

    String evals = getAlertFilterEvaluation(functionID, startTime, endTime);
    if(evals != null) {
      JSONObject evalJson = new JSONObject(evals);
      Iterator<String> nameltr = evalJson.keys();
      while (nameltr.hasNext()) {
        String key = nameltr.next();
        String eval = evalJson.getString(key);
        alertFilterEvaluations.put(key, eval);
      }
    }
    return new EvaluationNode(functionID, metricName, collection, metricAlertFilter, alertFilterEvaluations);
  }
}
