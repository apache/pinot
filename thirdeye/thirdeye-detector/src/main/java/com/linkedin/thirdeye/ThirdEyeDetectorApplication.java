package com.linkedin.thirdeye;

import com.linkedin.thirdeye.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.api.AnomalyResult;
import com.linkedin.thirdeye.api.ContextualEvent;
import com.linkedin.thirdeye.client.DefaultThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.db.AnomalyResultDAO;
import com.linkedin.thirdeye.db.ContextualEventDAO;
import com.linkedin.thirdeye.driver.AnomalyDetectionJobManager;
import com.linkedin.thirdeye.resources.AnomalyFunctionSpecResource;
import com.linkedin.thirdeye.resources.AnomalyResultResource;
import com.linkedin.thirdeye.resources.ContextualEventResource;
import com.linkedin.thirdeye.resources.MetricsGraphicsTimeSeriesResource;
import com.linkedin.thirdeye.task.AdHocAnomalyDetectionJobTask;
import com.linkedin.thirdeye.task.ScheduleAnomalyDetectionJobTask;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.migrations.MigrationsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

public class ThirdEyeDetectorApplication extends Application<ThirdEyeDetectorConfiguration> {

  private final HibernateBundle<ThirdEyeDetectorConfiguration> hibernate =
      new HibernateBundle<ThirdEyeDetectorConfiguration>(
          AnomalyFunctionSpec.class,
          AnomalyResult.class,
          ContextualEvent.class) {
        @Override
        public DataSourceFactory getDataSourceFactory(ThirdEyeDetectorConfiguration config) {
          return config.getDatabase();
        }
      };

  public static void main(final String[] args) throws Exception {
    new ThirdEyeDetectorApplication().run(args);
  }

  @Override
  public String getName() {
    return "ThirdEyeDetector";
  }

  @Override
  public void initialize(final Bootstrap<ThirdEyeDetectorConfiguration> bootstrap) {
    bootstrap.addBundle(new MigrationsBundle<ThirdEyeDetectorConfiguration>() {
      @Override
      public DataSourceFactory getDataSourceFactory(ThirdEyeDetectorConfiguration config) {
        return config.getDatabase();
      }
    });

    bootstrap.addBundle(hibernate);

    bootstrap.addBundle(new AssetsBundle("/assets/", "/", "index.html"));
  }

  @Override
  public void run(final ThirdEyeDetectorConfiguration config, final Environment environment) throws Exception {
    // DAO
    final AnomalyFunctionSpecDAO anomalyFunctionSpecDAO = new AnomalyFunctionSpecDAO(hibernate.getSessionFactory());
    final AnomalyResultDAO anomalyResultDAO = new AnomalyResultDAO(hibernate.getSessionFactory());
    final ContextualEventDAO contextualEventDAO = new ContextualEventDAO(hibernate.getSessionFactory());

    // ThirdEye client
    final ThirdEyeClient thirdEyeClient = new DefaultThirdEyeClient(config.getThirdEyeHost(), config.getThirdEyePort());
    environment.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {
        // NOP
      }

      @Override
      public void stop() throws Exception {
        thirdEyeClient.close();
      }
    });

    // Quartz Scheduler
    SchedulerFactory schedulerFactory = new StdSchedulerFactory();
    final Scheduler quartzScheduler = schedulerFactory.getScheduler();
    environment.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {
        quartzScheduler.start();
      }

      @Override
      public void stop() throws Exception {
        quartzScheduler.shutdown();
      }
    });

    // ThirdEye driver
    AnomalyDetectionJobManager jobManager = new AnomalyDetectionJobManager(
        quartzScheduler,
        thirdEyeClient,
        anomalyFunctionSpecDAO,
        anomalyResultDAO,
        hibernate.getSessionFactory());

    // Jersey resources
    environment.jersey().register(new AnomalyFunctionSpecResource(anomalyFunctionSpecDAO));
    environment.jersey().register(new AnomalyResultResource(anomalyResultDAO));
    environment.jersey().register(new ContextualEventResource(contextualEventDAO));
    environment.jersey().register(new MetricsGraphicsTimeSeriesResource(thirdEyeClient, anomalyResultDAO));

    // Tasks
    environment.admin().addTask(new ScheduleAnomalyDetectionJobTask(jobManager));
    environment.admin().addTask(new AdHocAnomalyDetectionJobTask(jobManager));
  }
}
