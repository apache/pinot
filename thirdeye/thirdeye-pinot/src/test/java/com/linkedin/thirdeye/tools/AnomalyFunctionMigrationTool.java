package com.linkedin.thirdeye.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.anomaly.task.TaskConstants.TaskStatus;
import com.linkedin.thirdeye.anomaly.utils.DetectionResourceHttpUtils;
import com.linkedin.thirdeye.constant.AnomalyFeedbackType;
import com.linkedin.thirdeye.datalayer.bao.AnomalyFunctionManager;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.TaskManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.TaskDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.tools.migrate.ConfidenceIntervalSignTestFunctionMigrater;
import com.linkedin.thirdeye.tools.migrate.GenericSplineFunctionMigrater;
import com.linkedin.thirdeye.tools.migrate.MovingAverageSignTestFunctionMigrater;
import com.linkedin.thirdeye.tools.migrate.RegressianGaussianScanFunctionMigrater;
import com.linkedin.thirdeye.tools.migrate.SignTestFunctionMigrater;
import com.linkedin.thirdeye.tools.migrate.SplineRegressionFunctionMigrater;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.StringUtils;
import org.jfree.data.time.Day;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Duration;
import org.joda.time.Months;
import org.joda.time.ReadablePeriod;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnomalyFunctionMigrationTool {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyFunctionMigrationTool.class);
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  private static final String COMMA = ",";
  private static final ReadablePeriod MAX_ANOMALY_CLONE_OFFSET = Months.THREE;
  private static final int TASK_WAITING_LATENCY_IN_MINUTES = 1;
  private static final int TASK_WAITING_LATENCY_IN_SECONDS = 10;

  public enum Action {
      CLONE, DELETE, MIGRATE, REPLAY, REPLAY_ORIGIN, EXPORT
  }

  private DateTime monitoringWindowStartTime;
  private DateTime monitoringWindowEndTime;
  private List<Long> originalFunctionIds;
  private List<Long> functionIds;
  private File exportPath;

  private AnomalyFunctionManager anomalyFunctionDAO;
  private MergedAnomalyResultManager mergedResultDAO;
  private TaskManager taskDAO;
  private DetectionResourceHttpUtils detectionResourceHttpUtils;
  private Map<Long, List<TaskDTO>> functionReplayTaskMap;

  public AnomalyFunctionMigrationTool(File persistenceFile, DateTime startTime, DateTime endTime, List<Long> functionIds,
      String detectionHost, int detectionPort, File exportPath, String token) {
    init(persistenceFile);
    this.monitoringWindowStartTime = startTime;
    this.monitoringWindowEndTime = endTime;
    this.originalFunctionIds = new ArrayList<>(functionIds);
    this.functionIds = functionIds;
    this.exportPath = exportPath;
    this.functionReplayTaskMap = new HashMap<>();

    detectionResourceHttpUtils = new DetectionResourceHttpUtils(detectionHost, detectionPort, token);
  }

  public void init(File persistenceFile) {
    DaoProviderUtil.init(persistenceFile);
    anomalyFunctionDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.AnomalyFunctionManagerImpl.class);
    mergedResultDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.MergedAnomalyResultManagerImpl.class);
    taskDAO = DaoProviderUtil
        .getInstance(com.linkedin.thirdeye.datalayer.bao.jdbc.TaskManagerImpl.class);
  }

  public void run(List<Action> actions) throws IOException {
    boolean isClonedSuccessful = false;
    boolean isReplayed = false;
    try {
      for (Action action : actions) {
        switch (action) {
          case CLONE:
            LOG.info("Clone functions and anomalies");
            cloneFunctions();
            isClonedSuccessful = true;
            break;
          case DELETE:
            LOG.info("Delete anomalies in range");
            deleteAnomalies();
            break;
          case MIGRATE:
            LOG.info("Migrate anomaly functions");
            migrageAnomalyFunctions();
            break;
          case REPLAY_ORIGIN:
            LOG.info("Replay anomaly functions in range");
            for (long functionId : originalFunctionIds) {
              deleteAnomaliesForFunction(functionId, this.monitoringWindowStartTime.minus(Days.days(120)), this.monitoringWindowEndTime);
              replayByFunction(functionId, this.monitoringWindowStartTime, this.monitoringWindowEndTime, true);
            }
            isReplayed = true;
            break;
          case REPLAY:
            LOG.info("Replay anomaly functions in range");
            repaly();
            isReplayed = true;
            break;
          case EXPORT:
            if (isReplayed) {
              waitForFunctionTasksDone();
            }
            LOG.info("Export anomaly results");
            exportAnomalyResultComparison();
            break;
          default:
            throw new UnsupportedOperationException(String.format("Action %s is not supported", action.name()));
        }
      }
    } catch (Exception e) {
      // Clean up cloned functions and anomalies
      if (isClonedSuccessful) {
        LOG.info("Encounter exception, cleaning up everything");
        for (long functionId : this.functionIds) {
          removeFunction(functionId);
        }
      }
      throw new IOException(e);
    }
  }

  private void waitForFunctionTasksDone() {
    Queue<Entry<Long, List<TaskDTO>>> taskEntryQueue = new LinkedList<>(functionReplayTaskMap.entrySet());
    int numActiveTasks = taskEntryQueue.size();
    while(!taskEntryQueue.isEmpty()) {
      for (int i = 0; i < numActiveTasks; i++) {
        Entry<Long, List<TaskDTO>> entry = taskEntryQueue.poll();
        waitForTasksDone(entry.getValue());
        if (entry.getValue().isEmpty()) {
          LOG.info("Tasks of Function {} is done", entry.getKey());
          numActiveTasks--;
        } else {
          LOG.info("Tasks of Function {} is still running", entry.getKey());
          taskEntryQueue.offer(entry);
        }
      }
      // sleep
      if (taskEntryQueue.isEmpty()) {
        break;
      }
      try {
        LOG.info("Wait for {} seconds(s)", TASK_WAITING_LATENCY_IN_SECONDS);
        TimeUnit.SECONDS.sleep(TASK_WAITING_LATENCY_IN_SECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Sleep is interrupted by InterruptedException", e);
      }
    }
  }

  private void waitForTasksDone(Collection<TaskDTO> tasks) {
    Iterator<TaskDTO> taskIterator = tasks.iterator();
    while (taskIterator.hasNext()) {
      TaskDTO task = taskIterator.next();
      TaskStatus taskStatus = getTaskStatus(task.getId());
      if (!taskStatus.equals(TaskStatus.WAITING) && !taskStatus.equals(TaskStatus.RUNNING)) {
        LOG.info("Task {} is done with Status {}", task.getId(), taskStatus);
        taskIterator.remove();
      } else {
        LOG.info("Task {} is still running/waiting, sleep for {} second(s)", task.getId(),
            TASK_WAITING_LATENCY_IN_SECONDS);
      }
    }
  }

  private List<Long> cloneFunctions() throws IOException {
    this.functionIds.clear();
    DateTime end = this.monitoringWindowStartTime;
    DateTime start = end.minus(MAX_ANOMALY_CLONE_OFFSET);
    for (long functionId : this.originalFunctionIds) {
      this.functionIds.add(cloneAnomalyFunction(functionId, "clone", start, end, true));
    }
    return functionIds;
  }

  private void deleteAnomalies() throws IOException {
    for (long functionId : this.functionIds) {
      deleteAnomaliesForFunction(functionId, monitoringWindowStartTime.minus(Days.days(120)), monitoringWindowEndTime);
    }
  }

  private void migrageAnomalyFunctions() {
    for (long functionId : this.functionIds) {
      migrateAnomalyFunction(functionId);
    }
  }

  private void repaly() throws IOException {
    for (long functionId : this.functionIds) {
      replayByFunction(functionId, monitoringWindowStartTime, monitoringWindowEndTime, true);
    }
  }

  private void exportAnomalyResultComparison() throws IOException {
    for (int i = 0; i < this.functionIds.size(); i++) {
      exportAnomalyResultComparisonOfFunctions(this.originalFunctionIds.get(i), this.functionIds.get(i),
          monitoringWindowStartTime, monitoringWindowEndTime);
    }
  }

  public void exportAnomalyResultComparisonOfFunctions(long originalFunctionId, long clonedFunctionId, DateTime startTime,
      DateTime endTime) throws IOException {
    long start = startTime.getMillis();
    long end = endTime.getMillis();
    boolean sameFunction = Long.compare(originalFunctionId, clonedFunctionId) == 0;
    List<MergedAnomalyResultDTO> originalResults = mergedResultDAO.findByStartTimeInRangeAndFunctionId(
        start, end, originalFunctionId);

    List<MergedAnomalyResultDTO> clonedResults = Collections.emptyList();
    if (!sameFunction) {
      clonedResults = mergedResultDAO.findByStartTimeInRangeAndFunctionId(
          start, end, clonedFunctionId);
    }

    int n = Math.max(originalResults.size(), clonedResults.size());
    String fname = String.format("%d-%d-%d_%d.csv", originalFunctionId, clonedFunctionId, start/1000, end/1000);
    BufferedWriter bw = new BufferedWriter(new FileWriter(new File(exportPath, fname)));
    bw.write(String.format("Original Function %d Results,,,,,,,", originalFunctionId));
    if (!sameFunction) {
      bw.write(String.format(",Cloned Function %d Results,,,,,,,", clonedFunctionId));
    }
    bw.newLine();
    bw.write("Start,End,Filter,Observed,Expected,Severity,P-Value,Label");
    if (!sameFunction) {
      bw.write(",Start,End,Filter,Observed,Expected,Severity,P-Value,Label");
    }
    bw.newLine();
    for (int i = 0; i < n; i++) {
      if (i < originalResults.size()) {
        MergedAnomalyResultDTO resultDTO = originalResults.get(i);
        AnomalyFeedbackType feedbackType = AnomalyFeedbackType.NO_FEEDBACK;
        if (resultDTO.getFeedback() != null && resultDTO.getFeedback().getFeedbackType() != null) {
          feedbackType = resultDTO.getFeedback().getFeedbackType();
        }
        bw.write(String.format("%d,%d,%s,%f,%f,%f,%f,%s", resultDTO.getStartTime(), resultDTO.getEndTime(),
            resultDTO.getDimensions().toString(), resultDTO.getAvgCurrentVal(), resultDTO.getAvgBaselineVal(),
            resultDTO.getWeight(), resultDTO.getScore(), feedbackType.toString()));
      } else {
        bw.write(",,,,,,,");
      }
      if (!sameFunction) {
        if (i < clonedResults.size()) {
          MergedAnomalyResultDTO resultDTO = clonedResults.get(i);
          AnomalyFeedbackType feedbackType = AnomalyFeedbackType.NO_FEEDBACK;
          if (resultDTO.getFeedback() != null && resultDTO.getFeedback().getFeedbackType() != null) {
            feedbackType = resultDTO.getFeedback().getFeedbackType();
          }
          bw.write(String.format(",%d,%d,%s,%f,%f,%f,%f,%s", resultDTO.getStartTime(), resultDTO.getEndTime(),
              resultDTO.getDimensions().toString(), resultDTO.getAvgCurrentVal(), resultDTO.getAvgBaselineVal(),
              resultDTO.getWeight(), resultDTO.getScore(), feedbackType.toString()));
        } else {
          bw.write(",,,,,,,,");
        }
      }
      bw.newLine();
    }
    bw.flush();
    bw.close();
  }

  public void migrateAnomalyFunction(long functionId) {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    if (anomalyFunction == null) {
      LOG.error("Function with ID: {} is not found", functionId);
      return;
    }
    switch (anomalyFunction.getType()) {
      case "SIGN_TEST_VANILLA":
        new SignTestFunctionMigrater().migrate(anomalyFunction);
        LOG.info("Function migrated: {}", anomalyFunction);
        break;
      case "CONFIDENCE_INTERVAL_SIGN_TEST":
        new ConfidenceIntervalSignTestFunctionMigrater().migrate(anomalyFunction);
        LOG.info("Function migrated: {}", anomalyFunction);
        break;
      case "MOVING_AVERAGE_SIGN_TEST":
        new MovingAverageSignTestFunctionMigrater().migrate(anomalyFunction);
        LOG.info("Function migrated: {}", anomalyFunction);
        break;
      case "REGRESSION_GAUSSIAN_SCAN":
        new RegressianGaussianScanFunctionMigrater().migrate(anomalyFunction);
        LOG.info("Function migrated: {}", anomalyFunction);
        break;
      case "SPLINE_REGRESSION_VANILLA":
        new GenericSplineFunctionMigrater().migrate(anomalyFunction);
        LOG.info("Function migrated: {}", anomalyFunction);
        break;
      default:
        LOG.warn("Anomaly function {} with function type {} is not supported for migration", functionId, anomalyFunction.getType());
    }
    anomalyFunctionDAO.update(anomalyFunction);

  }


  public void deleteAnomaliesForFunction(long functionId, DateTime start, DateTime end) {
    List<MergedAnomalyResultDTO> mergedAnomalies = mergedResultDAO
        .findByStartTimeInRangeAndFunctionId(start.getMillis(), end.getMillis(), functionId);

    for (MergedAnomalyResultDTO mergedAnomaly : mergedAnomalies) {
      LOG.info("Remove {} from DB", mergedAnomaly);
      mergedResultDAO.delete(mergedAnomaly);
    }
  }

  public long cloneAnomalyFunction(long functionId, String prefix, DateTime anomalyCloneStart,
      DateTime anomalyCloneEnd, boolean removeDuplicate) throws IOException {
    if (prefix == null || StringUtils.isBlank(prefix)) {
      prefix = "clone";
    }
    AnomalyFunctionDTO originFunction = anomalyFunctionDAO.findById(functionId);
    if (originFunction == null) {
      throw new IOException(String.format("Anomaly function id %d is not found", functionId));
    }
    String newFunctionName = String.format("%s_%s", prefix, originFunction.getFunctionName());

    AnomalyFunctionDTO duplicate = findFunctionByName(newFunctionName);
    if (duplicate != null) {
      removeFunction(duplicate.getId());
    }

    originFunction.setId(null);
    originFunction.setActive(true);
    originFunction.setFunctionName(newFunctionName);
    long newId = anomalyFunctionDAO.save(originFunction);
    cloneAnomalyResults(functionId, newId, anomalyCloneStart, anomalyCloneEnd);
    return newId;
  }

  public void cloneAnomalyResults(long srcId, long destId, DateTime anomalyCloneStart, DateTime anomalyCloneEnd)
      throws IOException {
    AnomalyFunctionDTO srcAnomalyFunction = anomalyFunctionDAO.findById(srcId);
    if (srcAnomalyFunction == null) {
      throw new IOException(String.format("Source anomaly function id %d is not found", srcId));
    }

    AnomalyFunctionDTO destAnomalyFunction = anomalyFunctionDAO.findById(destId);
    if (destAnomalyFunction == null) {
      throw new IOException(String.format("Destination anomaly function id %d is not found", destId));
    }

    LOG.info("clone merged anomaly results from source anomaly function id {} to id {}", srcId, destId);

    List<MergedAnomalyResultDTO> mergedAnomalies = mergedResultDAO
        .findByStartTimeInRangeAndFunctionId(anomalyCloneStart.getMillis(), anomalyCloneEnd.getMillis(), srcId);
    if (mergedAnomalies == null || mergedAnomalies.isEmpty()) {
      LOG.warn("No merged anomaly results found for anomaly function Id: {}", srcId);
    }

    for (MergedAnomalyResultDTO mergedAnomalyResultDTO : mergedAnomalies) {
      long oldId = mergedAnomalyResultDTO.getId();
      mergedAnomalyResultDTO.setId(null);  // clean the Id, then will create a new Id when save
      mergedAnomalyResultDTO.setFunctionId(destId);
      mergedAnomalyResultDTO.setFunction(destAnomalyFunction);
      long newId = mergedResultDAO.save(mergedAnomalyResultDTO);
      LOG.debug("clone merged anomaly from {} to {}", oldId, newId);
    }
  }

  public void replayByFunction(long functionId, DateTime replayStart, DateTime replayEnd, boolean forceBackfill) throws IOException {
    AnomalyFunctionDTO anomalyFunction = anomalyFunctionDAO.findById(functionId);
    if (!anomalyFunction.getIsActive()) {
      LOG.info("Skipping deactivated function {}", functionId);
//      return;
      anomalyFunction.setIsActive(true);
      anomalyFunctionDAO.update(anomalyFunction);
    }

    LOG.info("Sending backfill function {} for range {} to {}", functionId, replayStart, replayEnd);
    boolean speedup = false;

    if (anomalyFunction.getBucketUnit().equals(TimeUnit.MINUTES)) {
      speedup = true;
    }
    String response =
        detectionResourceHttpUtils.runBackfillAnomalyFunction(String.valueOf(functionId),
            replayStart.toString(),
            replayEnd.toString(), forceBackfill, speedup);
    Map<String, Integer> functionTaskMap = JSON_MAPPER.readValue(response, HashMap.class);
    long jobId = (long) functionTaskMap.get(String.valueOf(functionId));
    List<TaskDTO> tasksOfJob = taskDAO.findByJobIdStatusNotIn(jobId, TaskStatus.COMPLETED);
    functionReplayTaskMap.put(functionId, tasksOfJob);
    LOG.info("Function {} with {} task(s)", functionId, tasksOfJob.size());
  }

  private TaskStatus getTaskStatus(long taskId) {
    TaskDTO taskDTO = taskDAO.findById(taskId);
    if (taskDTO == null) {
      LOG.warn("No task with ID {}", taskId);
      return TaskStatus.FAILED;
    }
    return taskDTO.getStatus();
  }

  private AnomalyFunctionDTO findFunctionByName(String functionName) {
    return anomalyFunctionDAO.findWhereNameEquals(functionName);
  }

  private void removeFunction(long functionId)  {
    AnomalyFunctionDTO functionDTO = anomalyFunctionDAO.findById(functionId);
    if (functionDTO == null) {
      return;
    }

    deleteAnomaliesForFunction(functionId, new DateTime(0), DateTime.now());
    anomalyFunctionDAO.deleteById(functionId);
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("USAGE AnomalyFunctionMigrationTool <token> <config_yml_file> <action> \n "
          + "Please take note: \n"
          + "CLONE action will clone all anomaly functions listed in yml file, \n"
          + "DELETE action will delete all anomalies for that functionIds,\n"
          + "MIGRATE action will migrate the anomaly function to the new designed anomaly function, and\n"
          + "REPLAY action will run replay on all the anomaly functions (if cloned, then run on the cloned anomaly functions)\n"
          + "EXPORT action will export the anomaly results of the original anomalies and the new cloned anomalies\n"
          + "ALL action will run the above actions one by one"
      );
      System.exit(1);
    }

    String token = args[0];
    File configFile = new File(args[1]);
    AnomalyFunctionMigrationConfigs config =
        YAML_MAPPER.readValue(configFile, AnomalyFunctionMigrationConfigs.class);

    List<Action> actions = new ArrayList<>();
    boolean isAll = false;
    for (int i = 2; i < args.length; i++) {
      if (args[i].toUpperCase().equals("ALL")) {
        isAll = true;
        break;
      }
      actions.add(Action.valueOf(args[i].toUpperCase()));
    }
    if (isAll) {
      actions.clear();
      actions.addAll(Arrays.asList(Action.values()));
    }
    Collections.sort(actions, new Comparator<Action>() {
      @Override
      public int compare(Action o1, Action o2) {
        return Integer.compare(o1.ordinal(), o2.ordinal());
      }
    });

    List<Long> functionIds = new ArrayList<>();
    for (String functionId : config.getFunctionIds().split(COMMA)) {
      functionIds.add(Long.valueOf(functionId));
    }

    DateTimeFormatter dateTimeFormatter = ISODateTimeFormat.dateTimeParser();

    File exportPath = new File(config.getExportPath());
    if (!exportPath.exists()) {
      if(!exportPath.mkdirs()) {
        throw new IOException("Unable to make directories for path:" + exportPath.getAbsolutePath());
      }
    }
    if (exportPath.isFile()) {
      exportPath = exportPath.getParentFile();
    }
    if (!exportPath.canWrite()) {
      throw new IOException(String.format("No permission to write on %s", config.getExportPath()));
    }

    DateTime monitorStart = dateTimeFormatter.parseDateTime(config.getStartTimeIso());
    DateTime monitorEnd = dateTimeFormatter.parseDateTime(config.getEndTimeIso());
    AnomalyFunctionMigrationTool tool = new AnomalyFunctionMigrationTool(
        new File(config.getPersistenceFile()),
        monitorStart,
        monitorEnd,
        functionIds,
        config.getDetectorHost(),
        config.getDetectorPort(),
        exportPath,
        token);

    tool.run(actions);
    System.exit(0);
  }
}
