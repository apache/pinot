package com.linkedin.thirdeye.anomaly.detectionex;

import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionEx;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExContext;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExFactory;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExResult;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DetectionExTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionExTaskRunner.class);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private AnomalyFunctionExDTO funcSpec;
  private AnomalyFunctionExFactory funcFactory;
  private long monitoringWindowStart;
  private long monitoringWindowEnd;
  private long mergeWindow;

  protected void setupTask(TaskInfo taskInfo, TaskContext context) throws Exception {
    DetectionExTaskInfo task = (DetectionExTaskInfo) taskInfo;
    funcSpec = task.getAnomalyFunctionExSpec();
    funcFactory = context.getAnomalyFunctionExFactory();
    monitoringWindowStart = task.getMonitoringWindowStart();
    monitoringWindowEnd = task.getMonitoringWindowEnd();
    mergeWindow = task.getMergeWindow();
  }

  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    LOG.info("Begin executing task {}", taskInfo);

    setupTask(taskInfo, taskContext);

    // create function context
    long timestamp = DateTime.now(DateTimeZone.UTC).getMillis();
    long functionId = -funcSpec.getId(); // avoid interference with regular anomalies

    AnomalyFunctionExContext context = new AnomalyFunctionExContext();
    context.setMonitoringWindowStart(monitoringWindowStart);
    context.setMonitoringWindowEnd(monitoringWindowEnd);

    context.setClassName(funcSpec.getClassName());
    context.setConfig(funcSpec.getConfig());
    context.setIdentifier(funcSpec.getName());

    // instantiate function
    AnomalyFunctionEx func = funcFactory.fromContext(context);

    // apply
    AnomalyFunctionExResult result = func.apply();

    LOG.info("{} detected {} anomalies", funcSpec.getName(), result.getAnomalies().size());

    // transform output into thirdeye anomalies
    List<MergedAnomalyResultDTO> generated = new ArrayList<>();

    for(AnomalyFunctionExResult.Anomaly a : result.getAnomalies()) {
      MergedAnomalyResultDTO dto = new MergedAnomalyResultDTO();
      dto.setMessage(a.getMessage());
      dto.setStartTime(a.getStart());
      dto.setEndTime(a.getEnd());
      dto.setCreatedTime(timestamp);
      dto.setFunctionId(functionId);
      dto.setMetric(funcSpec.getDisplayMetric());
      dto.setCollection(funcSpec.getDisplayCollection());
      dto.setDimensions(new DimensionMap());
      dto.setRawAnomalyIdList(Collections.EMPTY_LIST);

      // TODO avoid magic field names, refactor anomalies?
      DataFrame df = a.getData();
      if(!df.isEmpty()) {
        if (df.contains("collection")) {
          dto.setCollection(df.toStrings("collection").first());
        }
        if (df.contains("metric")) {
          dto.setMetric(df.toStrings("metric").first());
        }
        if (df.contains("dimension")) {
          DimensionMap m = new DimensionMap();
          String[] s = df.toStrings("dimension").first().split("=", 2);
          m.put(s[0], s[1]);
          dto.setDimensions(m);
        }
        if (df.contains("score")) {
          dto.setScore(df.toDoubles("score").mean());
        }
        if (df.contains("weight")) {
          dto.setWeight(df.toDoubles("weight").mean());
        }
        if (df.contains("current")) {
          dto.setAvgCurrentVal(df.toDoubles("current").mean());
        }
        if (df.contains("baseline")) {
          dto.setAvgBaselineVal(df.toDoubles("baseline").mean());
        }
      }

      generated.add(dto);
    }

    // TODO plugable merging policy or actually use merge task
    // see also: com.linkedin.thirdeye.anomaly.merge.AnomalyTimeBasedSummarizer.java

    LOG.info("Generated {} anomalies", generated.size());
    if(generated.isEmpty())
      return Collections.EMPTY_LIST;

    List<MergedAnomalyResultDTO> existing = DAO_REGISTRY.getMergedAnomalyResultDAO().findByFunctionId(functionId);

    List<MergedAnomalyResultDTO> all = new ArrayList<>();
    all.addAll(generated);
    all.addAll(existing);

    Collections.sort(all, new Comparator<MergedAnomalyResultDTO>() {
      @Override
      public int compare(MergedAnomalyResultDTO o1, MergedAnomalyResultDTO o2) {
        return o1.getStartTime().compareTo(o2.getStartTime());
      }
    });

    // merge minimal set
    Set<MergedAnomalyResultDTO> merged = new HashSet<>();

    MergedAnomalyResultDTO active = all.get(0);
    for(MergedAnomalyResultDTO candidate : all) {
      if(candidate.getStartTime() <= active.getEndTime() + mergeWindow) {
        active.setEndTime(Math.max(active.getEndTime(), candidate.getEndTime()));
        active.setCreatedTime(Math.min(active.getCreatedTime(), candidate.getCreatedTime()));
        // TODO merge any other fields
      } else {
        merged.add(active);
        active = candidate;
      }
    }
    merged.add(active);

    // store
    for(MergedAnomalyResultDTO m : all) {
      if(merged.contains(m)) {
        if(m.getId() == null) {
          // active, but does not exist
          DAO_REGISTRY.getMergedAnomalyResultDAO().save(m);
        } else {
          // active, and exists
          DAO_REGISTRY.getMergedAnomalyResultDAO().update(m);
        }
      } else {
        if(m.getId() != null) {
          // not active, but exists
          DAO_REGISTRY.getMergedAnomalyResultDAO().delete(m);
        }
      }
    }

    return Collections.EMPTY_LIST;
  }
}
