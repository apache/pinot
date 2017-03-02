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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.joda.time.DateTime;
import org.joda.time.Interval;
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
  private Map<String, Comparable> goalRange;
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
    setTuningParameters(tuningParameters);
    setGoalRange(goalRange);
  }

  @Override
  public void run() {
    LOG.info("Generating threads for each configuration...");
    List<Thread> runningThreads = new ArrayList<>();
    for(Map<String, String> config : tuningParameters) {
      LOG.info("Running backfill replay with parameter configuration: {}" + config.toString());
      FunctionAutotuneRunnable backfillRunnable = new FunctionAutotuneRunnable(detectionJobScheduler, anomalyFunctionDAO,
          mergedAnomalyResultDAO, rawAnomalyResultDAO, functionAutoTuneConfigDAO);
      backfillRunnable.setTuningFunctionId(tuningFunctionId);
      backfillRunnable.setReplayStart(replayStart);
      backfillRunnable.setReplayEnd(replayEnd);
      backfillRunnable.setForceBackfill(true);
      backfillRunnable.setTuningParameter(config);
      backfillRunnable.setGoalRange(goalRange);

      Thread thread = new Thread(backfillRunnable);
      thread.setName(config.toString());
      runningThreads.add(thread);
      thread.start();
    }
    for(Thread t : runningThreads){
      try {
        t.join();
      }
      catch (InterruptedException e){
        LOG.warn("Thread {} with config {} is interrupted", t.toString(), t.getName(), e);
      }
    }
    // TODO: send email or notification out
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

  public List<Map<String, String>> getTuningParameters() {
    return tuningParameters;
  }

  public void setTuningParameters(List<Map<String, String>> tuningParameters) {
    this.tuningParameters = tuningParameters;
  }
}
