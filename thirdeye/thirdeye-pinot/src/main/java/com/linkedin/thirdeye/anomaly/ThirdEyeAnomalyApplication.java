package com.linkedin.thirdeye.anomaly;

import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.migrations.MigrationsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.detection.DetectionJobResource;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import com.linkedin.thirdeye.anomaly.task.TaskDriver;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.linkedin.thirdeye.detector.db.HibernateSessionWrapper;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

public class ThirdEyeAnomalyApplication
    extends BaseThirdEyeApplication<ThirdEyeAnomalyConfiguration> {

  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeAnomalyApplication.class);
  private DetectionJobScheduler detectionJobScheduler = null;
  private TaskDriver taskDriver = null;

  public static void main(final String[] args) throws Exception {
    List<String> argList = new ArrayList<String>(Arrays.asList(args));
    if (argList.size() == 1) {
      argList.add(0, "server");
    }
    int lastIndex = argList.size() - 1;
    String thirdEyeConfigDir = argList.get(lastIndex);
    System.setProperty("dw.rootDir", thirdEyeConfigDir);
    String detectorApplicationConfigFile = thirdEyeConfigDir + "/" + "detector.yml";
    argList.set(lastIndex, detectorApplicationConfigFile); // replace config dir with the
                                                           // actual config file
    new ThirdEyeAnomalyApplication().run(argList.toArray(new String[argList.size()]));
  }

  @Override
  public String getName() {
    return "Thirdeye Detector";
  }

  @Override
  public void initialize(final Bootstrap<ThirdEyeAnomalyConfiguration> bootstrap) {
    bootstrap.addBundle(new MigrationsBundle<ThirdEyeAnomalyConfiguration>() {
      @Override
      public DataSourceFactory getDataSourceFactory(ThirdEyeAnomalyConfiguration config) {
        return config.getDatabase();
      }
    });

    bootstrap.addBundle(hibernateBundle);

    bootstrap.addBundle(new AssetsBundle("/assets/", "/", "index.html"));
  }

  @Override
  public void run(final ThirdEyeAnomalyConfiguration config, final Environment environment)
      throws Exception {

    LOG.info("Starting ThirdeyeAnomalyApplication : Scheduler {} Worker {}", config.isScheduler(), config.isWorker());
    super.initDetectorRelatedDAO();
    ThirdEyeCacheRegistry.initializeDetectorCaches(config);

    final AnomalyFunctionFactory anomalyFunctionFactory =
        new AnomalyFunctionFactory(config.getFunctionConfigPath());

    if (config.isScheduler()) {
      detectionJobScheduler = new DetectionJobScheduler(anomalyJobSpecDAO, anomalyTaskSpecDAO,
          anomalyFunctionSpecDAO, hibernateBundle.getSessionFactory());
    }
    if (config.isWorker()) {
      taskDriver =
        new TaskDriver(config.getId(), anomalyTaskSpecDAO, anomalyResultDAO, anomalyFunctionRelationDAO,
            hibernateBundle.getSessionFactory(), anomalyFunctionFactory);
    }


    environment.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {
        new HibernateSessionWrapper<Void>(hibernateBundle.getSessionFactory())
            .execute(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            if (config.isWorker()) {
              taskDriver.start();
            }
            if (config.isScheduler()) {
              detectionJobScheduler.start();
              environment.jersey()
              .register(new DetectionJobResource(detectionJobScheduler, anomalyFunctionSpecDAO));
            }
            return null;
          }
        });
      }

      @Override
      public void stop() throws Exception {
        if (config.isWorker()) {
          taskDriver.stop();
        }
        if (config.isScheduler()) {
          detectionJobScheduler.stop();
        }
      }
    });


  }

}
