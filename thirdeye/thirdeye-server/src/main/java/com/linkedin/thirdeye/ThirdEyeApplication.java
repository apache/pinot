package com.linkedin.thirdeye;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.healthcheck.TimeRangeContiguityHealthCheck;
import com.linkedin.thirdeye.healthcheck.KafkaConsumerLagHealthCheck;
import com.linkedin.thirdeye.healthcheck.KafkaDataLagHealthCheck;
import com.linkedin.thirdeye.healthcheck.StarTreeHealthCheck;
import com.linkedin.thirdeye.healthcheck.TimeRangeHealthCheck;
import com.linkedin.thirdeye.managed.AnomalyDetectionTaskManager;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.healthcheck.CollectionConsistencyHealthCheck;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import com.linkedin.thirdeye.impl.storage.DataUpdateManager;
import com.linkedin.thirdeye.managed.KafkaConsumerManager;
import com.linkedin.thirdeye.query.ThirdEyeQueryExecutor;
import com.linkedin.thirdeye.resource.*;
import com.linkedin.thirdeye.task.*;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;

import org.apache.commons.io.FileUtils;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class ThirdEyeApplication extends Application<ThirdEyeApplication.Config>
{
  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeApplication.class);

  @Override
  public String getName()
  {
    return "thirdeye";
  }

  @Override
  public void initialize(Bootstrap<Config> bootstrap)
  {
    bootstrap.addBundle(new ViewBundle());
    bootstrap.addBundle(new AssetsBundle("/assets/stylesheets", "/assets/stylesheets", null, "stylesheets"));
    bootstrap.addBundle(new AssetsBundle("/assets/javascripts", "/assets/javascripts", null, "javascripts"));
    bootstrap.addBundle(new AssetsBundle("/assets/images", "/assets/images", null, "images"));
  }

  @Override
  public void run(final Config config, Environment environment) throws Exception
  {
    final File rootDir = new File(config.getRootDir());

    if (!rootDir.exists())
    {
      FileUtils.forceMkdir(rootDir);
    }

    ExecutorService parallelQueryExecutor =
            environment.lifecycle()
                       .executorService("parallel_query_executor")
                       .minThreads(Runtime.getRuntime().availableProcessors() / 2)
                       .maxThreads(Runtime.getRuntime().availableProcessors() / 2)
                       .build();

    ScheduledExecutorService anomalyDetectionTaskScheduler =
            environment.lifecycle()
                       .scheduledExecutorService("anomaly_detection_task_scheduler")
                       .build();

    final StarTreeManager starTreeManager = new StarTreeManagerImpl();

    final DataUpdateManager dataUpdateManager = new DataUpdateManager(rootDir);

    final KafkaConsumerManager kafkaConsumerManager = new KafkaConsumerManager(
        rootDir,
        starTreeManager,
        dataUpdateManager,
        environment.metrics());

    final AnomalyDetectionTaskManager anomalyDetectionTaskManager =
            new AnomalyDetectionTaskManager(starTreeManager,
                                            anomalyDetectionTaskScheduler,
                                            config.getAnomalyDetectionInterval(),
                                            rootDir);

    environment.lifecycle().manage(anomalyDetectionTaskManager);
    environment.lifecycle().manage(new Managed()
    {
      @Override
      public void start() throws Exception
      {
        if (config.isAutoRestore())
        {
          String[] collections = rootDir.list();
          if (collections != null)
          {
            for (String collection : collections)
            {
              starTreeManager.restore(rootDir, collection);
            }
          }

          if (config.isAutoConsume())
          {
            kafkaConsumerManager.start();
            LOGGER.info("Started kafka consumer manager");
          }
        }
      }

      @Override
      public void stop() throws Exception
      {
        try
        {
          kafkaConsumerManager.stop();
          LOGGER.info("Stopped kafka consumer manager");
        }
        catch (Exception e)
        {
          LOGGER.error("{}", e);
        }

        try
        {
          Set<String> collections = new HashSet<String>(starTreeManager.getCollections());
          for (String collection : collections)
          {
            starTreeManager.close(collection);
          }
          LOGGER.info("Closed star tree manager");
        }
        catch (Exception e)
        {
          LOGGER.error("{}", e);
        }
      }
    });

    // Health checks
    environment.healthChecks().register(CollectionConsistencyHealthCheck.NAME,
                                        new CollectionConsistencyHealthCheck(rootDir, starTreeManager));
    environment.healthChecks().register(TimeRangeHealthCheck.NAME,
                                        new TimeRangeHealthCheck(rootDir, starTreeManager));
    environment.healthChecks().register(StarTreeHealthCheck.NAME,
                                        new StarTreeHealthCheck(rootDir, starTreeManager));
    environment.healthChecks().register(TimeRangeContiguityHealthCheck.NAME,
                                        new TimeRangeContiguityHealthCheck(rootDir, starTreeManager));
    environment.healthChecks().register(KafkaDataLagHealthCheck.NAME,
                                        new KafkaDataLagHealthCheck(kafkaConsumerManager));
    environment.healthChecks().register(KafkaConsumerLagHealthCheck.NAME,
                                        new KafkaConsumerLagHealthCheck(kafkaConsumerManager));

    // Resources
    TimeSeriesResource timeSeriesResource = new TimeSeriesResource(starTreeManager);
    FunnelResource funnelResource = new FunnelResource(starTreeManager);
    HeatMapResource heatMapResource = new HeatMapResource(starTreeManager, parallelQueryExecutor);
    environment.jersey().register(new CollectionsResource(
        starTreeManager, environment.metrics(), dataUpdateManager, rootDir));
    environment.jersey().register(new AdminResource());
    environment.jersey().register(new AggregateResource(starTreeManager));
    environment.jersey().register(timeSeriesResource);
    environment.jersey().register(funnelResource);
    environment.jersey().register(heatMapResource);
    environment.jersey().register(new DashboardResource(
        starTreeManager, timeSeriesResource, funnelResource, heatMapResource, config.getFeedbackAddress()));
    environment.jersey().register(new QueryResource(new ThirdEyeQueryExecutor(parallelQueryExecutor, starTreeManager)));

    // Tasks
    environment.admin().addTask(new RestoreTask(starTreeManager, rootDir));
    environment.admin().addTask(new ViewTreeTask(starTreeManager));
    environment.admin().addTask(new ViewDimensionIndexTask(rootDir));
    environment.admin().addTask(new ViewMetricIndexTask(rootDir));
    environment.admin().addTask(new KafkaTask(kafkaConsumerManager));
  }

  public static class Config extends Configuration
  {
    @NotEmpty
    private String rootDir;

    private boolean autoRestore;

    private boolean autoConsume;

    private TimeGranularity anomalyDetectionInterval;

    private String feedbackAddress;

    public void setRootDir(String rootDir)
    {
      this.rootDir = rootDir;
    }

    public void setAutoRestore(boolean autoRestore)
    {
      this.autoRestore = autoRestore;
    }

    public void setAutoConsume(boolean autoConsume)
    {
      this.autoConsume = autoConsume;
    }

    public void setAnomalyDetectionInterval(TimeGranularity anomalyDetectionInterval)
    {
      this.anomalyDetectionInterval = anomalyDetectionInterval;
    }

    public void setFeedbackAddress(String feedbackAddress)
    {
      this.feedbackAddress = feedbackAddress;
    }

    @JsonProperty
    public String getRootDir()
    {
      return rootDir;
    }

    @JsonProperty
    public boolean isAutoRestore()
    {
      return autoRestore;
    }

    public boolean isAutoConsume()
    {
      return autoConsume;
    }

    @JsonProperty
    public TimeGranularity getAnomalyDetectionInterval()
    {
      return anomalyDetectionInterval;
    }

    @JsonProperty
    public String getFeedbackAddress()
    {
      return feedbackAddress;
    }
  }

  public static void main(String[] args) throws Exception
  {
    new ThirdEyeApplication().run(args);
  }
}
