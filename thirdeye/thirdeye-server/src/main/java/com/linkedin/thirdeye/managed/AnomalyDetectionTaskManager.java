package com.linkedin.thirdeye.managed;

import com.linkedin.thirdeye.anomaly.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomaly.AnomalyDetectionTask;
import com.linkedin.thirdeye.anomaly.AnomalyResultHandler;
import com.linkedin.thirdeye.anomaly.AnomalyResultHandlerLoggerImpl;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.impl.storage.StorageUtils;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public class AnomalyDetectionTaskManager implements Managed
{
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionTaskManager.class);

  private final StarTreeManager starTreeManager;
  private final ScheduledExecutorService scheduler;
  private final TimeGranularity executionInterval;
  private final Set<ScheduledFuture> tasks;
  private final File rootDir;

  public AnomalyDetectionTaskManager(StarTreeManager starTreeManager,
                                     ScheduledExecutorService scheduler,
                                     TimeGranularity executionInterval,
                                     File rootDir)
  {
    this.starTreeManager = starTreeManager;
    this.scheduler = scheduler;
    this.executionInterval = executionInterval;
    this.tasks = new HashSet<ScheduledFuture>();
    this.rootDir = rootDir;
  }

  @Override
  public void start() throws Exception
  {
    if (executionInterval == null)
    {
      // NOP
      return;
    }

    synchronized (tasks)
    {
      for (String collection : starTreeManager.getCollections())
      {
        Map<File, StarTree> starTrees = starTreeManager.getStarTrees(collection);
        if (starTrees == null)
        {
          LOG.warn("No star trees available for {}", collection);
          continue;
        }

        File latestDataDir = StorageUtils.findLatestDataDir(new File(rootDir, collection));
        if (latestDataDir == null)
        {
          LOG.warn("No latest data dir for {}", collection);
          continue;
        }

        StarTree starTree = starTrees.get(latestDataDir);
        if (starTree == null)
        {
          LOG.error("Manager does not have star tree for data dir {}", latestDataDir);
          continue;
        }

        String functionClass = starTree.getConfig().getAnomalyDetectionFunctionClass();

        if (functionClass != null)
        {
          // Function
          AnomalyDetectionFunction function
              = (AnomalyDetectionFunction) Class.forName(functionClass).getConstructor().newInstance();
          function.init(starTree.getConfig(), starTree.getConfig().getAnomalyDetectionFunctionConfig());

          // Handler
          String handlerClass = starTree.getConfig().getAnomalyHandlerClass();
          if (handlerClass == null)
          {
            handlerClass = AnomalyResultHandlerLoggerImpl.class.getCanonicalName();
          }
          AnomalyResultHandler handler
              = (AnomalyResultHandler) Class.forName(handlerClass).getConstructor().newInstance();
          handler.init(starTree.getConfig(), starTree.getConfig().getAnomalyHandlerConfig());

          // Mode
          AnomalyDetectionTask.Mode mode = starTree.getConfig().getAnomalyDetectionMode() == null
              ? AnomalyDetectionTask.Mode.LEAF_PREFIX
              : AnomalyDetectionTask.Mode.valueOf(starTree.getConfig().getAnomalyDetectionMode());

          LOG.info("Starting anomaly detection for {} using function {} and handler {} at interval of {} {}",
                   collection, functionClass, handlerClass, executionInterval.getSize(), executionInterval.getUnit());

          // Task
          tasks.add(scheduler.scheduleAtFixedRate(
              new AnomalyDetectionTask(starTree, function, handler, mode),
              0, executionInterval.getSize(), executionInterval.getUnit()));
        }
      }
    }
  }

  @Override
  public void stop() throws Exception
  {
    if (executionInterval == null)
    {
      // NOP
      return;
    }

    synchronized (tasks)
    {
      LOG.info("Resetting all anomaly detection tasks");

      for (ScheduledFuture task : tasks)
      {
        task.cancel(true);
      }
    }
  }

  public void reset() throws Exception
  {
    synchronized (tasks)
    {
      stop();
      start();
    }
  }
}
