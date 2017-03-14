package com.linkedin.thirdeye.anomaly.detection.lib;

import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluate;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluateHelper;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
import com.linkedin.thirdeye.dashboard.resources.OnboardResource;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.AutotuneConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.AutotuneConfigDTO;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
  private AutotuneMethodType autotuneMethodType;
  private AutotuneConfigManager autotuneConfigDAO;
  private PerformanceEvaluationMethod performanceEvaluationMethod;
  private long tuningFunctionId;
  private DateTime replayStart;
  private DateTime replayEnd;
  private double goal;
  private boolean isForceBackfill;
  private Map<String, String> tuningParameter;
  private Long functionAutotuneConfigId;

  public FunctionAutotuneRunnable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, RawAnomalyResultManager rawAnomalyResultDAO,
      AutotuneConfigManager autotuneConfigDAO){
    this.detectionJobScheduler = detectionJobScheduler;
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.rawAnomalyResultDAO = rawAnomalyResultDAO;
    this.autotuneConfigDAO = autotuneConfigDAO;
    setForceBackfill(true);
  }

  public FunctionAutotuneRunnable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, RawAnomalyResultManager rawAnomalyResultDAO,
      AutotuneConfigManager autotuneConfigDAO, Map<String, String> tuningParameter,
      long tuningFunctionId, DateTime replayStart, DateTime replayEnd, double goal, long functionAutotuneConfigId,
      Map<String, Comparable> goalRange, boolean isForceBackfill) {
    this.detectionJobScheduler = detectionJobScheduler;
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
    this.rawAnomalyResultDAO = rawAnomalyResultDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.autotuneConfigDAO = autotuneConfigDAO;
    setTuningFunctionId(tuningFunctionId);
    setReplayStart(replayStart);
    setReplayEnd(replayEnd);
    setForceBackfill(isForceBackfill);
    setTuningParameter(tuningParameter);
    setFunctionAutotuneConfigId(functionAutotuneConfigId);
    setGoal(goal);
  }

  @Override
  public void run() {
    long currentTime = System.currentTimeMillis();
    long clonedFunctionId = 0l;
    OnboardResource
        onboardResource = new OnboardResource(anomalyFunctionDAO, mergedAnomalyResultDAO, rawAnomalyResultDAO);
    StringBuilder functionName = new StringBuilder("clone");
    for (Map.Entry<String, String> entry : tuningParameter.entrySet()) {
      functionName.append("_");
      functionName.append(entry.getKey());
      functionName.append("_");
      functionName.append(entry.getValue());
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

    // enlarge window size so that we can speed-up the replay speed
    switch(anomalyFunctionDTO.getWindowUnit()){
      case NANOSECONDS:
      case MICROSECONDS:
      case MILLISECONDS: // In case of future use
      case SECONDS:
      case MINUTES:
        anomalyFunctionDTO.setWindowSize(12);
        anomalyFunctionDTO.setWindowUnit(TimeUnit.HOURS);
        anomalyFunctionDTO.setCron("0 0 0/2 * * ? *");
      case HOURS:
        anomalyFunctionDTO.setWindowSize(4);
        anomalyFunctionDTO.setWindowUnit(TimeUnit.DAYS);
        anomalyFunctionDTO.setCron("0 0 0 */4 * ? *");
      case DAYS:
      default:
        anomalyFunctionDTO.setWindowSize(7);
        anomalyFunctionDTO.setWindowUnit(TimeUnit.DAYS);
        anomalyFunctionDTO.setCron("0 0 0 ? * MON *");
    }

    // Set Properties
    FunctionPropertiesUtils.applyConfigurationToProperties(anomalyFunctionDTO, tuningParameter);
    anomalyFunctionDTO.setActive(true);

    anomalyFunctionDAO.update(anomalyFunctionDTO);

    detectionJobScheduler.synchronousBackFill(clonedFunctionId, replayStart, replayEnd, isForceBackfill);

    PerformanceEvaluate performanceEvaluator = PerformanceEvaluateHelper.getPerformanceEvaluator(performanceEvaluationMethod,
        tuningFunctionId, clonedFunctionId, new Interval(replayStart.getMillis(), replayEnd.getMillis()), mergedAnomalyResultDAO);
    double performance = performanceEvaluator.evaluate();

    AutotuneConfigDTO targetAutotuneDTO = autotuneConfigDAO.findById(functionAutotuneConfigId);

    double prevPerformance = targetAutotuneDTO.getPerformance().get(performanceEvaluationMethod.name());
    if(Math.abs(prevPerformance - goal) > Math.abs(performance - goal)) {
      targetAutotuneDTO.setConfiguration(tuningParameter);
      Map<String, Double> newPerformance = targetAutotuneDTO.getPerformance();
      newPerformance.put(performanceEvaluationMethod.name(), performance);
      targetAutotuneDTO.setPerformance(newPerformance);
      targetAutotuneDTO.setAvgRunningTime((System.currentTimeMillis() - currentTime) / 1000);
      targetAutotuneDTO.setLastUpdateTimestamp(System.currentTimeMillis());
    }
    String message = (targetAutotuneDTO.getMessage().isEmpty()) ? "" : (targetAutotuneDTO.getMessage() + ";");

    targetAutotuneDTO.setMessage(message + tuningParameter.toString() + ":" + performance);

    autotuneConfigDAO.update(targetAutotuneDTO);


    // clean up and kill itself
    onboardResource.deleteExistingAnomalies(Long.toString(clonedFunctionId), replayStart.getMillis(), replayEnd.getMillis());
    anomalyFunctionDAO.deleteById(clonedFunctionId);
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

  public Map<String, String> getTuningParameter() {
    return tuningParameter;
  }

  public void setTuningParameter(Map<String, String> tuningParameter) {
    this.tuningParameter = tuningParameter;
  }

  public AutotuneMethodType getAutotuneMethodType() {
    return autotuneMethodType;
  }

  public void setAutotuneMethodType(AutotuneMethodType autotuneMethodType) {
    this.autotuneMethodType = autotuneMethodType;
  }

  public PerformanceEvaluationMethod getPerformanceEvaluationMethod() {
    return performanceEvaluationMethod;
  }

  public void setPerformanceEvaluationMethod(PerformanceEvaluationMethod performanceEvaluationMethod) {
    this.performanceEvaluationMethod = performanceEvaluationMethod;
  }

  public double getGoal() {
    return goal;
  }

  public void setGoal(double goal) {
    this.goal = goal;
  }

  public Long getFunctionAutotuneConfigId() {
    return functionAutotuneConfigId;
  }

  public void setFunctionAutotuneConfigId(Long functionAutotuneConfigId) {
    this.functionAutotuneConfigId = functionAutotuneConfigId;
  }
}
