package com.linkedin.thirdeye.anomaly.detection.lib;

import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluate;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluateHelper;
import com.linkedin.thirdeye.anomalydetection.performanceEvaluation.PerformanceEvaluationMethod;
import com.linkedin.thirdeye.dashboard.resources.OnboardResource;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.FunctionAutoTuneConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FunctionAutotuneCallable implements Callable<FunctionAutotuneReturn> {
  private static final Logger LOG = LoggerFactory.getLogger(FunctionAutotuneCallable.class);
  private DetectionJobScheduler detectionJobScheduler;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private RawAnomalyResultManager rawAnomalyResultDAO;
  private AutotuneMethodType autotuneMethodType;
  private PerformanceEvaluationMethod performanceEvaluationMethod;
  private long tuningFunctionId;
  private DateTime replayStart;
  private DateTime replayEnd;
  private boolean isForceBackfill;
  private Map<String, String> tuningParameter;

  private final String MAX_GAOL = "max";
  private final String MIN_GOAL = "min";
  private final String PERFORMANCE_VALUE = "value";

  public FunctionAutotuneCallable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, RawAnomalyResultManager rawAnomalyResultDAO,
      FunctionAutoTuneConfigManager functionAutoTuneConfigDAO){
    this.detectionJobScheduler = detectionJobScheduler;
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.rawAnomalyResultDAO = rawAnomalyResultDAO;
    setForceBackfill(true);
  }

  public FunctionAutotuneCallable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, RawAnomalyResultManager rawAnomalyResultDAO,
      FunctionAutoTuneConfigManager functionAutoTuneConfigDAO,
      Map<String, String> tuningParameter, long tuningFunctionId, DateTime replayStart, DateTime replayEnd,
      Map<String, Comparable> goalRange, boolean isForceBackfill) {
    this.detectionJobScheduler = detectionJobScheduler;
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
    this.rawAnomalyResultDAO = rawAnomalyResultDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    setTuningFunctionId(tuningFunctionId);
    setReplayStart(replayStart);
    setReplayEnd(replayEnd);
    setForceBackfill(isForceBackfill);
    setTuningParameter(tuningParameter);
  }

  @Override
  public FunctionAutotuneReturn call() {
    long clonedFunctionId = 0l;
    OnboardResource onboardResource = new OnboardResource(anomalyFunctionDAO, mergedAnomalyResultDAO, rawAnomalyResultDAO);
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
      return null;
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
        anomalyFunctionDTO.setWindowSize(4);
        anomalyFunctionDTO.setWindowUnit(TimeUnit.HOURS);
        anomalyFunctionDTO.setCron("0 0 0/4 * * ? *");
      case HOURS:
        anomalyFunctionDTO.setWindowSize(3);
        anomalyFunctionDTO.setWindowUnit(TimeUnit.DAYS);
        anomalyFunctionDTO.setCron("0 0 0 */3 * ? *");
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

    detectionJobScheduler.runBackfill(clonedFunctionId, replayStart, replayEnd, isForceBackfill);
    List<MergedAnomalyResultDTO> detectedMergedAnomalies =
        mergedAnomalyResultDAO.findAllConflictByFunctionId(clonedFunctionId, replayStart.getMillis(), replayEnd.getMillis());

    PerformanceEvaluate performanceEvaluator = PerformanceEvaluateHelper.getPerformanceEvaluator(performanceEvaluationMethod,
        tuningFunctionId, clonedFunctionId, new Interval(replayStart.getMillis(), replayEnd.getMillis()), mergedAnomalyResultDAO);
    double performance = performanceEvaluator.evaluate();

    FunctionAutotuneReturn functionAutotuneReturn = new FunctionAutotuneReturn(tuningFunctionId, replayStart, replayEnd,
        tuningParameter, autotuneMethodType, performanceEvaluationMethod, performance);

    // clean up and kill itself
    onboardResource.deleteExistingAnomalies(Long.toString(clonedFunctionId), replayStart.getMillis(), replayEnd.getMillis());
    anomalyFunctionDAO.deleteById(clonedFunctionId);

    return functionAutotuneReturn;
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
}
