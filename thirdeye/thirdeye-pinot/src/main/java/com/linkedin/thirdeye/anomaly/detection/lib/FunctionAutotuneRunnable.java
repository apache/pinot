package com.linkedin.thirdeye.anomaly.detection.lib;

import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.AnomalyPercentagePerformanceEvaluation;
import com.linkedin.thirdeye.dashboard.resources.OnboardResource;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.FunctionAutoTuneConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.FunctionAutoTuneConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionAutotuneRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(FunctionAutotuneRunnable.class);
  private DetectionJobScheduler detectionJobScheduler;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private RawAnomalyResultManager rawAnomalyResultDAO;
  private FunctionAutoTuneConfigManager functionAutoTuneConfigDAO;
  private long tuningFunctionId;
  private DateTime replayStart;
  private DateTime replayEnd;
  private boolean isForceBackfill;
  private Map<String, Comparable> goalRange;
  private Map<String, String> tuningParameter;

  private final String MAX_GAOL = "max";
  private final String MIN_GOAL = "min";
  private final String PERFORMANCE_VALUE = "value";

  public FunctionAutotuneRunnable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, RawAnomalyResultManager rawAnomalyResultDAO,
      FunctionAutoTuneConfigManager functionAutoTuneConfigDAO){
    this.detectionJobScheduler = detectionJobScheduler;
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.rawAnomalyResultDAO = rawAnomalyResultDAO;
    this.functionAutoTuneConfigDAO = functionAutoTuneConfigDAO;
    setForceBackfill(true);
  }

  public FunctionAutotuneRunnable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, RawAnomalyResultManager rawAnomalyResultDAO,
      FunctionAutoTuneConfigManager functionAutoTuneConfigDAO,
      Map<String, String> tuningParameter, long tuningFunctionId, DateTime replayStart, DateTime replayEnd,
      Map<String, Comparable> goalRange, boolean isForceBackfill) {
    this.detectionJobScheduler = detectionJobScheduler;
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
    this.rawAnomalyResultDAO = rawAnomalyResultDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.functionAutoTuneConfigDAO = functionAutoTuneConfigDAO;
    setTuningFunctionId(tuningFunctionId);
    setReplayStart(replayStart);
    setReplayEnd(replayEnd);
    setForceBackfill(isForceBackfill);
    setTuningParameter(tuningParameter);
    setGoalRange(goalRange);
  }

  @Override
  public void run() {
    long clonedFunctionId = 0l;
    OnboardResource onboardResource = new OnboardResource(anomalyFunctionDAO, mergedAnomalyResultDAO, rawAnomalyResultDAO);
    StringBuilder functionName = new StringBuilder();
    for (Map.Entry<String, String> entry : tuningParameter.entrySet()) {
      functionName.append("_" + entry.getKey() + "_" + entry.getValue());
    }
    try {
      clonedFunctionId = onboardResource.cloneAnomalyFunctionById(tuningFunctionId, functionName.toString(), false);
    }
    catch (Exception e) {
      LOG.error("Unable to clone function {} with given name {}", tuningFunctionId, functionName.toString(), e);
      return;
    }

    AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionDAO.findById(clonedFunctionId);
    // Remove alert filters
    anomalyFunctionDTO.setAlertFilter(null);

    // Set Properties
    Properties properties = anomalyFunctionDTO.toProperties();
    for(Map.Entry<String, String> entry : tuningParameter.entrySet()) {
      properties.setProperty(entry.getKey(), entry.getValue());
    }
    anomalyFunctionDTO.setProperties(propertiesToString(properties));
    anomalyFunctionDTO.setActive(true);

    anomalyFunctionDAO.update(anomalyFunctionDTO);

    detectionJobScheduler.runBackfill(clonedFunctionId, replayStart, replayEnd, isForceBackfill);
    List<MergedAnomalyResultDTO> detectedMergedAnomalies =
        mergedAnomalyResultDAO.findAllConflictByFunctionId(clonedFunctionId, replayStart.getMillis(), replayEnd.getMillis());

    Comparable performance = AnomalyPercentagePerformanceEvaluation.evaluate(
        new Interval(replayStart.getMillis(), replayEnd.getMillis()), detectedMergedAnomalies);
    // If the performance is in the goal range, e.g. max >= performance >= min, store the properties in the list
    if(performance.compareTo(goalRange.get(MAX_GAOL)) <= 0 && performance.compareTo(goalRange.get(MIN_GOAL)) >= 0){
      FunctionAutoTuneConfigDTO functionAutoTuneConfigDTO = new FunctionAutoTuneConfigDTO();
      functionAutoTuneConfigDTO.setFunctionId(tuningFunctionId);
      functionAutoTuneConfigDTO.setAutotuneMethod(AutotuneMethodType.EXHAUSTIVE);
      functionAutoTuneConfigDTO.setPerformanceEvaluationMethod(PerformanceEvaluationMethod.ANOMALY_PERCENTAGE);
      functionAutoTuneConfigDTO.setPerformance(performanceToMap(performance));
      functionAutoTuneConfigDTO.setConfiguration(tuningParameter);
      functionAutoTuneConfigDTO.setStartTime(replayStart.getMillis());
      functionAutoTuneConfigDTO.setEndTime(replayEnd.getMillis());

      functionAutoTuneConfigDAO.save(functionAutoTuneConfigDTO);
    }

    // clean up and kill itself
    onboardResource.deleteExistingAnomalies(Long.toString(clonedFunctionId), replayStart.getMillis(), replayEnd.getMillis());
    anomalyFunctionDAO.deleteById(clonedFunctionId);
  }

  private Map<String, String> performanceToMap(Comparable performance){
    Map<String, String> performancenMap = new HashMap<>();
    performancenMap.put(PERFORMANCE_VALUE, performance.toString());
    return performancenMap;
  }
  /**
   * Convert Properties to String following the format in TE
   * @param props
   * @return a String of given Properties
   */
  private String propertiesToString(Properties props){
    StringBuilder stringBuilder = new StringBuilder();
    for(Map.Entry entry : props.entrySet()){
      stringBuilder.append(entry.getKey() + "=" + entry.getValue() + ";");
    }
    stringBuilder.deleteCharAt(stringBuilder.length() - 1);
    return stringBuilder.toString();
  }

  public long getTuningFunctionId() {
    return tuningFunctionId;
  }

  public void setTuningFunctionId(long functionId) {
    this.tuningFunctionId = functionId;
  }

  public DateTime getReplayStart() {
    return replayStart;
  }

  public void setReplayStart(DateTime replayStart) {
    this.replayStart = replayStart;
  }

  public DateTime getReplayEnd() {
    return replayEnd;
  }

  public void setReplayEnd(DateTime replayEnd) {
    this.replayEnd = replayEnd;
  }

  public boolean isForceBackfill() {
    return isForceBackfill;
  }

  public void setForceBackfill(boolean forceBackfill) {
    isForceBackfill = forceBackfill;
  }

  public Map<String, Comparable> getGoalRange() {
    return goalRange;
  }

  public void setGoalRange(Map<String, Comparable> goalRange) {
    this.goalRange = goalRange;
  }

  public Map<String, String> getTuningParameter() {
    return tuningParameter;
  }

  public void setTuningParameter(Map<String, String> tuningParameter) {
    this.tuningParameter = tuningParameter;
  }

}
