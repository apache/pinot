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


public class FunctionReplayRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(FunctionReplayRunnable.class);
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
  private boolean speedUp;
  private boolean selfKill;
  private long clonedFunctionId;

  public FunctionReplayRunnable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, RawAnomalyResultManager rawAnomalyResultDAO,
      AutotuneConfigManager autotuneConfigDAO){
    this.detectionJobScheduler = detectionJobScheduler;
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.rawAnomalyResultDAO = rawAnomalyResultDAO;
    this.autotuneConfigDAO = autotuneConfigDAO;
    setSpeedUp(true);
    setForceBackfill(true);
    setSelfKill(true);
  }

  public FunctionReplayRunnable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, RawAnomalyResultManager rawAnomalyResultDAO,
      AutotuneConfigManager autotuneConfigDAO, Map<String, String> tuningParameter,
      long tuningFunctionId, DateTime replayStart, DateTime replayEnd, double goal, long functionAutotuneConfigId,
      boolean isForceBackfill, boolean selfKill) {
    this(detectionJobScheduler, anomalyFunctionDAO, mergedAnomalyResultDAO, rawAnomalyResultDAO, autotuneConfigDAO);
    setTuningFunctionId(tuningFunctionId);
    setReplayStart(replayStart);
    setReplayEnd(replayEnd);
    setForceBackfill(isForceBackfill);
    setTuningParameter(tuningParameter);
    setFunctionAutotuneConfigId(functionAutotuneConfigId);
    setSpeedUp(true);
    setGoal(goal);
    setSelfKill(selfKill);
  }


  public FunctionReplayRunnable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, RawAnomalyResultManager rawAnomalyResultDAO,
      Map<String, String> tuningParameter, long tuningFunctionId, DateTime replayStart, DateTime replayEnd,
      boolean selfKill) {
    this(detectionJobScheduler, anomalyFunctionDAO, mergedAnomalyResultDAO, rawAnomalyResultDAO, null);
    setTuningFunctionId(tuningFunctionId);
    setReplayStart(replayStart);
    setReplayEnd(replayEnd);
    setForceBackfill(true);
    setTuningParameter(tuningParameter);
    setSpeedUp(true);
    setSelfKill(selfKill);
  }

  public static void speedup(AnomalyFunctionDTO anomalyFunctionDTO) {
    switch (anomalyFunctionDTO.getWindowUnit()) {
      case NANOSECONDS:
      case MICROSECONDS:
      case MILLISECONDS:
      case SECONDS:
      case MINUTES:       // These TimeUnits are not currently in use
      case HOURS:
      case DAYS:
          /*
          SignTest takes HOURS data, but changing to 7 days won't affect the final result
          SPLINE takes 1 DAYS data, for heuristic, we extend it to 7 days.
           */
      default:
        anomalyFunctionDTO.setWindowSize(7);
        anomalyFunctionDTO.setWindowUnit(TimeUnit.DAYS);
        anomalyFunctionDTO.setCron("0 0 0 ? * MON *");
    }
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
      this.clonedFunctionId = clonedFunctionId;
    }
    catch (Exception e) {
      LOG.error("Unable to clone function {} with given name {}", tuningFunctionId, functionName.toString(), e);
      return;
    }

    AnomalyFunctionDTO anomalyFunctionDTO = anomalyFunctionDAO.findById(clonedFunctionId);
    // Remove alert filters
    anomalyFunctionDTO.setAlertFilter(null);

    int originWindowSize = anomalyFunctionDTO.getWindowSize();
    TimeUnit originWindowUnit = anomalyFunctionDTO.getWindowUnit();
    String originCron = anomalyFunctionDTO.getCron();

    // enlarge window size so that we can speed-up the replay speed
    if(speedUp) {
      FunctionReplayRunnable.speedup(anomalyFunctionDTO);
    }

    // Set Properties
    anomalyFunctionDTO.updateProperties(tuningParameter);
    anomalyFunctionDTO.setActive(true);

    anomalyFunctionDAO.update(anomalyFunctionDTO);

    detectionJobScheduler.synchronousBackFill(clonedFunctionId, replayStart, replayEnd, isForceBackfill);

    if(autotuneConfigDAO != null) { // if no functionAutotuneId, skip update
      PerformanceEvaluate performanceEvaluator =
          PerformanceEvaluateHelper.getPerformanceEvaluator(performanceEvaluationMethod, tuningFunctionId,
              clonedFunctionId, new Interval(replayStart.getMillis(), replayEnd.getMillis()), mergedAnomalyResultDAO);
      double performance = performanceEvaluator.evaluate();

      AutotuneConfigDTO targetAutotuneDTO = autotuneConfigDAO.findById(functionAutotuneConfigId);

      double prevPerformance = targetAutotuneDTO.getPerformance().get(performanceEvaluationMethod.name());
      if (Math.abs(prevPerformance - goal) > Math.abs(performance - goal)) {
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
    }

    // clean up and kill itself
    if(selfKill) {
      onboardResource.deleteExistingAnomalies(Long.toString(clonedFunctionId), replayStart.getMillis(),
          replayEnd.getMillis());
      anomalyFunctionDAO.deleteById(clonedFunctionId);
    }
    else {
      anomalyFunctionDTO.setWindowSize(originWindowSize);
      anomalyFunctionDTO.setWindowUnit(originWindowUnit);
      anomalyFunctionDTO.setCron(originCron);
      anomalyFunctionDAO.update(anomalyFunctionDTO);
    }
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

  public boolean isSpeedUp() {
    return speedUp;
  }

  public void setSpeedUp(boolean speedUp) {
    this.speedUp = speedUp;
  }

  public boolean isSelfKill() {
    return selfKill;
  }

  public void setSelfKill(boolean selfKill) {
    this.selfKill = selfKill;
  }

  public long getClonedFunctionId(){return clonedFunctionId;}
}
