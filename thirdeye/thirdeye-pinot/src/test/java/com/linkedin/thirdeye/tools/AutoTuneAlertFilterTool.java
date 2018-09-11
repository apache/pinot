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


import com.linkedin.thirdeye.anomaly.utils.DetectionResourceHttpUtils;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.AutotuneConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.detector.email.filter.PrecisionRecallEvaluator;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
public class AutoTuneAlertFilterTool {
  private static final Logger LOG = LoggerFactory.getLogger(AutoTuneAlertFilterTool.class);
  public static AnomalyFunctionManager anomalyFunctionDAO;
  public static AutotuneConfigManager autotuneConfigDAO;

  private static final int APPLICATION_PORT = 1867;
  private static final String LOCALHOST = "localhost";
  private DetectionResourceHttpUtils httpUtils;


  public static String CSVSEPERATOR = ",";
  public static String CSVESCAPE = ";";


  public AutoTuneAlertFilterTool(File persistenceFile, String authToken){
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    autotuneConfigDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AutotuneConfigManagerImpl.class);

    httpUtils = new DetectionResourceHttpUtils(LOCALHOST, APPLICATION_PORT, authToken);
  }

  public static class EvaluationNode{

    private Properties alertFilterEval;

    private static final String ID = "Id";
    private static final String METRICNAME = "metricName";
    private static final String ALERTFILTERSTR = "alertFilterStr";
    private static final String COLLECTION = "collection";

    private static List<String> getHeaders(){
      List<String> headers = new ArrayList<>();
      headers.addAll(Arrays.asList(COLLECTION, METRICNAME, ID, ALERTFILTERSTR));
      headers.addAll(PrecisionRecallEvaluator.getPropertyNames());
      return headers;
    }

    public EvaluationNode(Long Id, String metricName, String collection, String alertFilterStr,
        Properties alertFilterEvaluations){

      alertFilterEvaluations.put(ID, Id);
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


  public String getTunedAlertFilterByFunctionId(Long functionId, String startTimeISO, String endTimeISO, String AUTOTUNE_TYPE, String holidayStarts, String holidayEnds) throws Exception{
    try {
      return httpUtils.runAutoTune(functionId, startTimeISO, endTimeISO, AUTOTUNE_TYPE, holidayStarts, holidayEnds);
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
    return null;
  }


  public String evaluateAlertFilterByFunctionId(Long functionId, String startTimeISO, String endTimeISO, String holidayStarts, String holidayEnds){
    try{
      return httpUtils.getEvalStatsAlertFilter(functionId, startTimeISO, endTimeISO, holidayStarts, holidayEnds);
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
    return null;
  }

  public String evaluateAlertFilterByAutoTuneId(Long autotuneId, String startTimeISO, String endTimeISO, String holidayStarts, String holidayEnds){
    try{
      return httpUtils.evalAutoTune(autotuneId, startTimeISO, endTimeISO, holidayStarts, holidayEnds);
    } catch (Exception e){
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


  public EvaluationNode evalAnomalyFunctionAlertFilterToEvalNode(Long functionID, String startTimeISO, String endTimeISO, String holidayStarts, String holidayEnds)
      throws IOException, JSONException {
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(functionID);
    String metricName = anomalyFunctionSpec.getFunctionName();
    String collection = anomalyFunctionSpec.getCollection();
    String metricAlertFilter = (anomalyFunctionSpec.getAlertFilter() == null)? "null": anomalyFunctionSpec.getAlertFilter().toString().replaceAll(CSVSEPERATOR, CSVESCAPE);

    String evals = evaluateAlertFilterByFunctionId(functionID, startTimeISO, endTimeISO, holidayStarts, holidayEnds);
    Properties alertFilterEvaluations = jsonStringToProperties(evals);
    return new EvaluationNode(functionID, metricName, collection, metricAlertFilter, alertFilterEvaluations);
  }

  public EvaluationNode evalAutoTunedAlertFilterToEvalNode(Long autotuneId, String startTimeISO, String endTimeISO, String holidayStarts, String holidayEnds) throws IOException, JSONException{
    AutotuneConfigDTO autotuneConfigDTO = autotuneConfigDAO.findById(autotuneId);
    long functionId = autotuneConfigDTO.getFunctionId();
    AnomalyFunctionDTO anomalyFunctionSpec = anomalyFunctionDAO.findById(functionId);
    String metricName = anomalyFunctionSpec.getFunctionName();
    String collection = anomalyFunctionSpec.getCollection();
    String metricAlertFilter = autotuneConfigDTO.getConfiguration().toString().replaceAll(CSVSEPERATOR, CSVESCAPE);

    String evals = evaluateAlertFilterByAutoTuneId(autotuneId, startTimeISO, endTimeISO, holidayStarts, holidayEnds);
    Properties alertFilterEvaluations = jsonStringToProperties(evals);
    return new EvaluationNode(autotuneId, metricName, collection, metricAlertFilter, alertFilterEvaluations);
  }

  private Properties jsonStringToProperties(String jsonStr) throws JSONException {
    Properties res = new Properties();
    if (jsonStr != null) {
      JSONObject jsonVal = new JSONObject(jsonStr);
      Iterator<String> nameltr = jsonVal.keys();
      while (nameltr.hasNext()) {
        String key = nameltr.next();
        String eval = jsonVal.getString(key);
        res.put(key, eval);
      }
    }
    return res;
  }

  public List<Long> getAllFunctionIdsByCollection(String Collection){
    List<AnomalyFunctionDTO> anomalyFunctionSpecs = anomalyFunctionDAO.findAllByCollection(Collection);
    List<Long> functionIds = new ArrayList<>();
    for(AnomalyFunctionDTO anomalyFunctionSpec: anomalyFunctionSpecs){
      functionIds.add(anomalyFunctionSpec.getId());
    }
    return functionIds;
  }

  public Boolean checkAnomaliesHasLabels(Long functionId, String startTimeISO, String endTimeISO, String holidayStarts, String holidayEnds, String authToken){
    DetectionResourceHttpUtils httpUtils = new DetectionResourceHttpUtils(LOCALHOST, APPLICATION_PORT, authToken);
    try{
      return Boolean.valueOf(httpUtils.checkHasLabels(functionId, startTimeISO, endTimeISO, holidayStarts, holidayEnds));
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
    return null;
  }

  public String getInitAutoTuneByFunctionId(Long functionId, String startTimeISO, String endTimeISO, String AUTOTUNE_TYPE, int nExpected,String holidayStarts, String holidayEnds, String authToken) throws Exception{
    DetectionResourceHttpUtils httpUtils = new DetectionResourceHttpUtils(LOCALHOST, APPLICATION_PORT, authToken);
    try {
      return httpUtils.initAutoTune(functionId, startTimeISO, endTimeISO, AUTOTUNE_TYPE, nExpected, holidayStarts, holidayEnds);
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
    return null;
  }
}

