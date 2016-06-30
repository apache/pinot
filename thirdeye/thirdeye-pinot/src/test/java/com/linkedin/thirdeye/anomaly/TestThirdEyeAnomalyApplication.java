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

import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.linkedin.thirdeye.detector.ThirdEyeDetectorConfiguration;
import com.linkedin.thirdeye.detector.db.HibernateSessionWrapper;

public class TestThirdEyeAnomalyApplication
    extends BaseThirdEyeApplication<ThirdEyeDetectorConfiguration> {

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
    new TestThirdEyeAnomalyApplication().run(argList.toArray(new String[argList.size()]));
  }

  @Override
  public String getName() {
    return "Thirdeye Detector";
  }

  @Override
  public void initialize(final Bootstrap<ThirdEyeDetectorConfiguration> bootstrap) {
    bootstrap.addBundle(new MigrationsBundle<ThirdEyeDetectorConfiguration>() {
      @Override
      public DataSourceFactory getDataSourceFactory(ThirdEyeDetectorConfiguration config) {
        return config.getDatabase();
      }
    });

    bootstrap.addBundle(hibernateBundle);

    bootstrap.addBundle(new AssetsBundle("/assets/", "/", "index.html"));
  }

  @Override
  public void run(final ThirdEyeDetectorConfiguration config, final Environment environment)
      throws Exception {

    super.initDetectorRelatedDAO();

    final JobScheduler jobScheduler = new JobScheduler(anomalyJobSpecDAO, anomalyTaskSpecDAO,
        anomalyFunctionSpecDAO, hibernateBundle.getSessionFactory());
    final TaskDriver taskDriver =
        new TaskDriver(anomalyTaskSpecDAO, hibernateBundle.getSessionFactory());

    environment.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {
        new HibernateSessionWrapper<Void>(hibernateBundle.getSessionFactory())
            .execute(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            taskDriver.start();
            jobScheduler.start();
            return null;
          }
        });
      }

      @Override
      public void stop() throws Exception {
        taskDriver.stop();
        jobScheduler.stop();
      }
    });
  }

}
