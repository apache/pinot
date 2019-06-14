package org.apache.pinot.thirdeye.detection;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.thirdeye.anomaly.task.TaskConstants;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.TaskManager;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.TaskDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.EvaluationBean;
import org.apache.pinot.thirdeye.datalayer.pojo.MergedAnomalyResultBean;
import org.apache.pinot.thirdeye.datalayer.pojo.TaskBean;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.joda.time.Interval;


/**
 * The detection health metric and status
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DetectionHealth {
  // overall health for a detection config
  @JsonProperty
  private HealthStatus overallHealth;

  // the regression metrics and status for a detection config
  @JsonProperty
  private RegressionStatus regressionStatus;

  // the anomaly coverage status for a detection config
  @JsonProperty
  private AnomalyCoverageStatus anomalyCoverageStatus;

  // the detection task status for a detection config
  @JsonProperty
  private DetectionTaskStatus detectionTaskStatus;

  public enum HealthStatus {
    GOOD, MODERATE, BAD
  }

  public static class RegressionStatus {
    // the average mape for each detector
    @JsonProperty
    private Map<String, Double> detectorMapes;

    // the health status for each detector
    @JsonProperty
    private Map<String, HealthStatus> detectorHealthStatus;

    // the overall regression health for the detection config
    @JsonProperty
    private HealthStatus healthStatus;

    public Map<String, Double> getDetectorMapes() {
      return detectorMapes;
    }

    public Map<String, HealthStatus> getDetectorHealthStatus() {
      return detectorHealthStatus;
    }

    public HealthStatus getHealthStatus() {
      return healthStatus;
    }
  }

  public static class AnomalyCoverageStatus {
    // the anomaly coverage ratio. the percentage of anomalous duration in the duration of the whole window
    @JsonProperty
    private double anomalyCoverageRatio;

    // the health status of the anomaly coverage ratio
    @JsonProperty
    private HealthStatus healthStatus;

    public double getAnomalyCoverageRatio() {
      return anomalyCoverageRatio;
    }

    public HealthStatus getHealthStatus() {
      return healthStatus;
    }
  }

  public static class DetectionTaskStatus {

    // the task success rate for the detection config
    @JsonProperty
    private double taskSuccessRate;

    // the health status for the detection tasks
    @JsonProperty
    private HealthStatus healthStatus;

    // the list of tasks for the detection config
    @JsonProperty
    private List<TaskDTO> tasks;

    public double getTaskSuccessRate() {
      return taskSuccessRate;
    }

    public HealthStatus getHealthStatus() {
      return healthStatus;
    }

    public List<TaskDTO> getTasks() {
      return tasks;
    }
  }

  public HealthStatus getOverallHealth() {
    return overallHealth;
  }

  public RegressionStatus getRegressionStatus() {
    return regressionStatus;
  }

  public AnomalyCoverageStatus getAnomalyCoverageStatus() {
    return anomalyCoverageStatus;
  }

  public DetectionTaskStatus getDetectionTaskStatus() {
    return detectionTaskStatus;
  }

  /**
   * Builder for the detection health
   */
  public static class Builder {
    private final long startTime;
    private final long endTime;
    private final long detectionConfigId;
    private EvaluationManager evaluationDAO;
    private MergedAnomalyResultManager anomalyDAO;
    private TaskManager taskDAO;
    private long taskLimit;
    private boolean provideOverallHealth;

    // database column name constants
    private static String COL_NAME_START_TIME = "startTime";
    private static String COL_NAME_END_TIME = "endTime";
    private static String COL_NAME_DETECTION_CONFIG_ID = "detectionConfigId";
    private static String COL_NAME_TASK_NAME = "name";
    private static String COL_NAME_TASK_STATUS = "status";
    private static String COL_NAME_TASK_TYPE = "type";

    public Builder(long detectionConfigId, long startTime, long endTime) {
      Preconditions.checkArgument(endTime >= startTime);
      this.startTime = startTime;
      this.endTime = endTime;
      this.detectionConfigId = detectionConfigId;
    }

    /**
     * Add the regression health status in the health report built by the builder
     * @param evaluationDAO the evaluation dao
     * @return the builder
     */
    public Builder addRegressionStatus(EvaluationManager evaluationDAO) {
      this.evaluationDAO = evaluationDAO;
      return this;
    }

    /**
     * Add the anomaly coverage health status in the health report built by the builder
     * @param anomalyDAO the anomaly dao
     * @return the builder
     */
    public Builder addAnomalyCoverageStatus(MergedAnomalyResultManager anomalyDAO) {
      this.anomalyDAO = anomalyDAO;
      return this;
    }

    /**
     * Add the detection task health status in the health report built by the builder
     * @param taskDAO the task dao
     * @param limit the maximum number of tasks returned in the health report (ordered by task start time, latest task first)
     * @return the builder
     */
    public Builder addDetectionTaskStatus(TaskManager taskDAO, long limit) {
      this.taskDAO = taskDAO;
      this.taskLimit = limit;
      return this;
    }

    /**
     * Add the global health status in the report built by the builder, consider regression health, coverage ratio and task health
     * @return the builder
     */
    public Builder addOverallHealth() {
      this.provideOverallHealth = true;
      return this;
    }

    /**
     * Build the health status object
     * @return the health status object
     */
    public DetectionHealth build() {
      DetectionHealth health = new DetectionHealth();
      if (this.evaluationDAO != null) {
        health.regressionStatus = buildRegressionStatus();
      }
      if (this.anomalyDAO != null) {
        health.anomalyCoverageStatus = buildAnomalyCoverageStatus();
      }
      if (this.taskDAO != null) {
        health.detectionTaskStatus = buildTaskStatus();
      }
      if (this.provideOverallHealth) {
        health.overallHealth = classifyOverallHealth(health);
      }
      return health;
    }

    private RegressionStatus buildRegressionStatus() {
      // fetch evaluations
      List<EvaluationDTO> evaluations = this.evaluationDAO.findByPredicate(
          Predicate.AND(Predicate.LT(COL_NAME_START_TIME, endTime), Predicate.GT(COL_NAME_END_TIME, startTime),
              Predicate.EQ(COL_NAME_DETECTION_CONFIG_ID, detectionConfigId)));

      // calculate average mapes for each detector
      Map<String, Double> detectorMapes = evaluations.stream()
          .filter(eval -> Objects.nonNull(eval.getMape()))
          .collect(Collectors.groupingBy(EvaluationBean::getDetectorName,
              Collectors.averagingDouble(EvaluationBean::getMape)));

      // construct regression status
      RegressionStatus status = new RegressionStatus();
      status.detectorMapes = detectorMapes;
      status.detectorHealthStatus = detectorMapes.entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> classifyMapeHealth(e.getValue())));
      status.healthStatus = classifyOverallRegressionStatus(status.detectorHealthStatus);
      return status;
    }

    private static HealthStatus classifyMapeHealth(double mape) {
      if (mape < 0.2) {
        return HealthStatus.GOOD;
      }
      if (mape < 0.5) {
        return HealthStatus.MODERATE;
      }
      return HealthStatus.BAD;
    }

    /**
     * Classify the regression status of the detection config based on the health status for each detector
     * @param detectorHealthStatus the health status for each detector
     * @return the overall regression status
     */
    private static HealthStatus classifyOverallRegressionStatus(Map<String, HealthStatus> detectorHealthStatus) {
      if (detectorHealthStatus.values().contains(HealthStatus.GOOD)) {
        return HealthStatus.GOOD;
      }
      if (detectorHealthStatus.values().contains(HealthStatus.MODERATE)) {
        return HealthStatus.MODERATE;
      }
      return HealthStatus.BAD;
    }

    private AnomalyCoverageStatus buildAnomalyCoverageStatus() {
      List<MergedAnomalyResultDTO> anomalies = this.anomalyDAO.findByPredicate(
          Predicate.AND(Predicate.LT(COL_NAME_START_TIME, this.endTime),
              Predicate.GT(COL_NAME_END_TIME, this.startTime),
              Predicate.EQ(COL_NAME_DETECTION_CONFIG_ID, detectionConfigId)));
      anomalies = anomalies.stream().filter(anomaly -> !anomaly.isChild()).collect(Collectors.toList());

      // the anomalies can come from different sub-dimensions, merge the anomaly range if possible
      List<Interval> intervals = new ArrayList<>();
      if (!anomalies.isEmpty()) {
        anomalies.sort(Comparator.comparingLong(MergedAnomalyResultBean::getStartTime));
        long start = anomalies.stream().findFirst().get().getStartTime();
        long end = anomalies.stream().findFirst().get().getEndTime();
        for (MergedAnomalyResultDTO anomaly : anomalies) {
          if (anomaly.getStartTime() <= end) {
            end = Math.max(end, anomaly.getEndTime());
          } else {
            intervals.add(new Interval(start, end));
            start = anomaly.getStartTime();
            end = anomaly.getEndTime();
          }
        }
        intervals.add(new Interval(start, end));
      }
      long totalAnomalyCoverage =
          intervals.stream().map(interval -> interval.getEndMillis() - interval.getStartMillis()).reduce(0L, Long::sum);

      AnomalyCoverageStatus coverageStatus = new AnomalyCoverageStatus();
      coverageStatus.anomalyCoverageRatio = (double) totalAnomalyCoverage / (this.endTime - this.startTime);
      coverageStatus.healthStatus = classifyCoverageStatus(coverageStatus.anomalyCoverageRatio);
      return coverageStatus;
    }

    private static HealthStatus classifyCoverageStatus(double anomalyCoverageRatio) {
      if (anomalyCoverageRatio > 0.85 || anomalyCoverageRatio < 0.01) {
        return HealthStatus.BAD;
      }
      if (anomalyCoverageRatio > 0.5) {
        return HealthStatus.MODERATE;
      }
      return HealthStatus.GOOD;
    }

    private DetectionTaskStatus buildTaskStatus() {
      // fetch tasks
      List<TaskDTO> tasks = this.taskDAO.findByPredicate(
          Predicate.AND(Predicate.EQ(COL_NAME_TASK_NAME, "DETECTION_" + this.detectionConfigId),
              Predicate.LT(COL_NAME_START_TIME, endTime), Predicate.GT(COL_NAME_END_TIME, startTime),
              Predicate.EQ(COL_NAME_TASK_TYPE, TaskConstants.TaskType.DETECTION.toString()),
              Predicate.IN(COL_NAME_TASK_STATUS, new String[]{TaskConstants.TaskStatus.COMPLETED.toString(),
                  TaskConstants.TaskStatus.FAILED.toString(), TaskConstants.TaskStatus.TIMEOUT.toString()})));
      tasks.sort(Comparator.comparingLong(TaskBean::getStartTime).reversed());

      DetectionTaskStatus taskStatus = new DetectionTaskStatus();
      taskStatus.tasks = tasks.stream().limit(this.taskLimit).collect(Collectors.toList());

      // count the number of tasks by task status
      Map<TaskConstants.TaskStatus, Long> count =
          tasks.stream().collect(Collectors.groupingBy(TaskBean::getStatus, Collectors.counting()));
      if (count.size() != 0) {
        long completedTasks = count.getOrDefault(TaskConstants.TaskStatus.COMPLETED, 0L);
        long failedTasks = count.getOrDefault(
            TaskConstants.TaskStatus.FAILED, 0L);
        long timeoutTasks = count.getOrDefault(TaskConstants.TaskStatus.TIMEOUT, 0L);
        taskStatus.taskSuccessRate = (double) completedTasks / (failedTasks +  timeoutTasks + completedTasks);
      }
      taskStatus.healthStatus = classifyTaskStatus(taskStatus.taskSuccessRate);
      return taskStatus;
    }

    private static HealthStatus classifyTaskStatus(double taskSuccessRate) {
      if (taskSuccessRate < 0.5) {
        return HealthStatus.BAD;
      }
      if (taskSuccessRate < 0.8) {
        return HealthStatus.MODERATE;
      }
      return HealthStatus.GOOD;
    }

    private static HealthStatus classifyOverallHealth(DetectionHealth health) {
      HealthStatus taskHealth = health.detectionTaskStatus.healthStatus;
      HealthStatus regressionHealth = health.regressionStatus.healthStatus;
      HealthStatus coverageHealth = health.anomalyCoverageStatus.healthStatus;

      Preconditions.checkNotNull(taskHealth);
      Preconditions.checkNotNull(regressionHealth);
      Preconditions.checkNotNull(coverageHealth);

      // if task fail ratio is high or both regression and coverage are bad, we say the overall status is bad
      if (taskHealth.equals(HealthStatus.BAD) || (regressionHealth.equals(HealthStatus.BAD) && coverageHealth.equals(
          HealthStatus.BAD))) {
        return HealthStatus.BAD;
      }

      Set<HealthStatus> statusSet = ImmutableSet.of(taskHealth, regressionHealth, coverageHealth);
      if (statusSet.contains(HealthStatus.MODERATE) || statusSet.contains(HealthStatus.BAD)) {
        return HealthStatus.MODERATE;
      }
      return HealthStatus.GOOD;
    }
  }
}
