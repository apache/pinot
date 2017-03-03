package com.linkedin.thirdeye.anomaly.detection.lib;

import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.FunctionAutoTuneConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.FunctionAutoTuneConfigDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FunctionAutotuneExcutorRunnable implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(FunctionAutotuneExcutorRunnable.class);
  private DetectionJobScheduler detectionJobScheduler;
  private MergedAnomalyResultManager mergedAnomalyResultDAO;
  private AnomalyFunctionManager anomalyFunctionDAO;
  private RawAnomalyResultManager rawAnomalyResultDAO;
  private FunctionAutoTuneConfigManager functionAutoTuneConfigDAO;
  private long tuningFunctionId;
  private DateTime replayStart;
  private DateTime replayEnd;
  private boolean isForceBackfill;
  private double goal;
  private List<Map<String, String>> tuningParameters;

  private final String MAX_GAOL = "max";
  private final String MIN_GOAL = "min";
  private final String PERFORMANCE_VALUE = "value";

  public FunctionAutotuneExcutorRunnable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, RawAnomalyResultManager rawAnomalyResultDAO,
      FunctionAutoTuneConfigManager functionAutoTuneConfigDAO){
    this.detectionJobScheduler = detectionJobScheduler;
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.rawAnomalyResultDAO = rawAnomalyResultDAO;
    this.functionAutoTuneConfigDAO = functionAutoTuneConfigDAO;
    setForceBackfill(true);
  }

  public FunctionAutotuneExcutorRunnable(DetectionJobScheduler detectionJobScheduler, AnomalyFunctionManager anomalyFunctionDAO,
      MergedAnomalyResultManager mergedAnomalyResultDAO, RawAnomalyResultManager rawAnomalyResultDAO,
      FunctionAutoTuneConfigManager functionAutoTuneConfigDAO,
      List<Map<String, String>> tuningParameters, long tuningFunctionId, DateTime replayStart, DateTime replayEnd,
      double goal, boolean isForceBackfill) {
    this.detectionJobScheduler = detectionJobScheduler;
    this.mergedAnomalyResultDAO = mergedAnomalyResultDAO;
    this.rawAnomalyResultDAO = rawAnomalyResultDAO;
    this.anomalyFunctionDAO = anomalyFunctionDAO;
    this.functionAutoTuneConfigDAO = functionAutoTuneConfigDAO;
    setTuningFunctionId(tuningFunctionId);
    setReplayStart(replayStart);
    setReplayEnd(replayEnd);
    setForceBackfill(isForceBackfill);
    setTuningParameters(tuningParameters);
    setGoal(goal);
  }

  @Override
  public void run() {
    LOG.info("Generating threads for each configuration...");
    long executionStartTime = System.currentTimeMillis();
    ExecutorService pool = Executors.newSingleThreadExecutor();
    List<Future<FunctionAutotuneReturn>> runningThreads = new ArrayList<>();
    for(Map<String, String> config : tuningParameters) {
      LOG.info("Running backfill replay with parameter configuration: {}" + config.toString());
      FunctionAutotuneCallable backfillCallable = new FunctionAutotuneCallable(detectionJobScheduler, anomalyFunctionDAO,
          mergedAnomalyResultDAO, rawAnomalyResultDAO, functionAutoTuneConfigDAO);
      backfillCallable.setTuningFunctionId(tuningFunctionId);
      backfillCallable.setReplayStart(replayStart);
      backfillCallable.setReplayEnd(replayEnd);
      backfillCallable.setForceBackfill(true);
      backfillCallable.setPerformanceEvaluationMethod(PerformanceEvaluationMethod.ANOMALY_PERCENTAGE);
      backfillCallable.setAutotuneMethodType(AutotuneMethodType.EXHAUSTIVE);
      backfillCallable.setTuningParameter(config);

      Future future = pool.submit(backfillCallable);
      runningThreads.add(future);
    }
    FunctionAutotuneReturn bestResult = null;
    long sum = 0l;
    double bestPerformance = Double.POSITIVE_INFINITY;
    for(Future<FunctionAutotuneReturn> t : runningThreads){
      FunctionAutotuneReturn backfillResult = null;
      try {
        backfillResult = t.get();
      }
      catch (InterruptedException e){
        LOG.warn("Thread {} is interrupted", t.toString(), e);
        continue;
      }
      catch (ExecutionException e){
        LOG.warn("Thread {} has an execution exception", t.toString(), e);
        continue;
      }

      // Summing total time usage for replay
      sum += System.currentTimeMillis() - executionStartTime;

      // Compare the performance with goal
      if(similarity(goal, bestPerformance) > similarity(goal, backfillResult.getPerformance())) {
        bestResult = backfillResult;
        bestPerformance = backfillResult.getPerformance();
      }
    }

    LOG.info("Average replay time is {} second(s)", (sum/runningThreads.size())/1000);
    LOG.info("Total running time is {} second(s)", (System.currentTimeMillis() - executionStartTime)/1000);

    if(bestResult != null) {
      FunctionAutoTuneConfigDTO functionAutoTuneConfigDTO = new FunctionAutoTuneConfigDTO();
      functionAutoTuneConfigDTO.setFunctionId(tuningFunctionId);
      functionAutoTuneConfigDTO.setStartTime(replayStart.getMillis());
      functionAutoTuneConfigDTO.setEndTime(replayEnd.getMillis());
      functionAutoTuneConfigDTO.setAutotuneMethod(bestResult.getAutotuneMethod());
      functionAutoTuneConfigDTO.setPerformanceEvaluationMethod(bestResult.getPerformanceEvaluationMethod());
      functionAutoTuneConfigDTO.setPerformance(bestResult.getPerformance());
      functionAutoTuneConfigDTO.setConfiguration(bestResult.getConfiguration());
      functionAutoTuneConfigDTO.setAvgRunningTime((sum/runningThreads.size())/1000l);
      functionAutoTuneConfigDTO.setOverallRunningTime((System.currentTimeMillis() - executionStartTime)/1000);

      functionAutoTuneConfigDAO.save(functionAutoTuneConfigDTO);
    }
    // TODO: send email or notification out
  }

  private double similarity(double d1, double d2){
    return Math.abs(d1 - d2);
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

  public List<Map<String, String>> getTuningParameters() {
    return tuningParameters;
  }

  public void setTuningParameters(List<Map<String, String>> tuningParameters) {
    this.tuningParameters = tuningParameters;
  }

  public double getGoal() {
    return goal;
  }

  public void setGoal(double goal) {
    this.goal = goal;
  }
}
