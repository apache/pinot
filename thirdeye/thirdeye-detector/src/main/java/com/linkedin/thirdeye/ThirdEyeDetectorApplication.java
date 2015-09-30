package com.linkedin.thirdeye;

import com.linkedin.thirdeye.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.api.AnomalyResult;
import com.linkedin.thirdeye.api.ContextualEvent;
import com.linkedin.thirdeye.api.EmailConfiguration;
import com.linkedin.thirdeye.client.DefaultThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.db.AnomalyResultDAO;
import com.linkedin.thirdeye.db.ContextualEventDAO;
import com.linkedin.thirdeye.db.EmailConfigurationDAO;
import com.linkedin.thirdeye.db.HibernateSessionWrapper;
import com.linkedin.thirdeye.driver.AnomalyDetectionJobManager;
import com.linkedin.thirdeye.email.EmailReportJobManager;
import com.linkedin.thirdeye.resources.AnomalyDetectionJobResource;
import com.linkedin.thirdeye.resources.AnomalyFunctionSpecResource;
import com.linkedin.thirdeye.resources.AnomalyResultResource;
import com.linkedin.thirdeye.resources.ContextualEventResource;
import com.linkedin.thirdeye.resources.EmailConfigurationResource;
import com.linkedin.thirdeye.resources.MetricsGraphicsTimeSeriesResource;
import com.linkedin.thirdeye.task.EmailReportJobManagerTask;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

public class ThirdEyeDetectorApplication extends Application<ThirdEyeDetectorConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeDetectorApplication.class);

  private final HibernateBundle<ThirdEyeDetectorConfiguration> hibernate =
      new HibernateBundle<ThirdEyeDetectorConfiguration>(
          AnomalyFunctionSpec.class,
          AnomalyResult.class,
          ContextualEvent.class,
          EmailConfiguration.class) {
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
    final EmailConfigurationDAO emailConfigurationDAO = new EmailConfigurationDAO(hibernate.getSessionFactory());

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
    final AnomalyDetectionJobManager jobManager = new AnomalyDetectionJobManager(
        quartzScheduler,
        thirdEyeClient,
        anomalyFunctionSpecDAO,
        anomalyResultDAO,
        hibernate.getSessionFactory(),
        environment.metrics());

    // Start all active jobs on startup
    environment.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {
        new HibernateSessionWrapper<Void>(hibernate.getSessionFactory()).execute(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            List<AnomalyFunctionSpec> functions;
            functions = anomalyFunctionSpecDAO.findAll();
            for (AnomalyFunctionSpec function : functions) {
              if (function.isActive()) {
                jobManager.start(function.getId());
                LOG.info("Starting {}", function);
              }
            }
            return null;
          }
        });
      }

      @Override
      public void stop() throws Exception {
        // NOP (quartz scheduler just dies)
      }
    });

    // Email reports
    final EmailReportJobManager emailReportJobManager = new EmailReportJobManager(
        quartzScheduler,
        emailConfigurationDAO,
        anomalyResultDAO,
        hibernate.getSessionFactory());
    environment.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {
        new HibernateSessionWrapper<Void>(hibernate.getSessionFactory()).execute(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            emailReportJobManager.start();
            return null;
          }
        });
      }

      @Override
      public void stop() throws Exception {
        new HibernateSessionWrapper<Void>(hibernate.getSessionFactory()).execute(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            emailReportJobManager.stop();
            return null;
          }
        });
      }
    });

    // Jersey resources
    environment.jersey().register(new AnomalyFunctionSpecResource(anomalyFunctionSpecDAO));
    environment.jersey().register(new AnomalyResultResource(anomalyResultDAO));
    environment.jersey().register(new ContextualEventResource(contextualEventDAO));
    environment.jersey().register(new MetricsGraphicsTimeSeriesResource(thirdEyeClient, anomalyResultDAO));
    environment.jersey().register(new AnomalyDetectionJobResource(jobManager, anomalyFunctionSpecDAO));
    environment.jersey().register(new EmailConfigurationResource(emailConfigurationDAO));

    // Tasks
    environment.admin().addTask(new EmailReportJobManagerTask(emailReportJobManager, hibernate.getSessionFactory()));
  }
}
