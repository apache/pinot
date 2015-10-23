package com.linkedin.thirdeye;

import com.linkedin.thirdeye.api.*;
import com.linkedin.thirdeye.client.DefaultThirdEyeClient;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.db.*;
import com.linkedin.thirdeye.driver.AnomalyDetectionJobManager;
import com.linkedin.thirdeye.email.EmailReportJobManager;
import com.linkedin.thirdeye.resources.*;
import com.linkedin.thirdeye.task.EmailReportJobManagerTask;
import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.migrations.MigrationsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

public class ThirdEyeDetectorApplication extends Application<ThirdEyeDetectorConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeDetectorApplication.class);

  private final HibernateBundle<ThirdEyeDetectorConfiguration> hibernate =
      new HibernateBundle<ThirdEyeDetectorConfiguration>(
          AnomalyFunctionSpec.class,
          AnomalyFunctionRelation.class,
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
    final AnomalyFunctionRelationDAO anomalyFunctionRelationDAO = new AnomalyFunctionRelationDAO(hibernate.getSessionFactory());

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
        LOG.info("Starting Quartz scheduler");
        quartzScheduler.start();
      }

      @Override
      public void stop() throws Exception {
        LOG.info("Stopping Quartz scheduler");
        quartzScheduler.shutdown();
      }
    });

    // ThirdEye driver
    final AnomalyDetectionJobManager jobManager = new AnomalyDetectionJobManager(
        quartzScheduler,
        thirdEyeClient,
        anomalyFunctionSpecDAO,
        anomalyFunctionRelationDAO,
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
              if (function.getIsActive()) {
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
    final AtomicInteger applicationPort = new AtomicInteger(-1);
    final EmailReportJobManager emailReportJobManager = new EmailReportJobManager(
        quartzScheduler,
        emailConfigurationDAO,
        anomalyResultDAO,
        hibernate.getSessionFactory(),
        applicationPort);
    environment.lifecycle().addServerLifecycleListener(new ServerLifecycleListener() {
      @Override
      public void serverStarted(Server server) {
        for (Connector connector : server.getConnectors()) {
          if (connector instanceof ServerConnector) {
            ServerConnector serverConnector = (ServerConnector) connector;
            applicationPort.set(serverConnector.getLocalPort());
          }
        }

        if (applicationPort.get() == -1) {
          throw new IllegalStateException("Could not determine application port");
        }

        // Start the email report job manager once we know the application port
        try {
          new HibernateSessionWrapper<Void>(hibernate.getSessionFactory()).execute(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              LOG.info("Starting email report job manager");
              emailReportJobManager.start();
              return null;
            }
          });
        } catch (Exception e) {
          throw new IllegalStateException(e);
        }
      }
    });


    // Jersey resources
    environment.jersey().register(new AnomalyFunctionSpecResource(anomalyFunctionSpecDAO));
    environment.jersey().register(new AnomalyFunctionRelationResource(anomalyFunctionRelationDAO));
    environment.jersey().register(new AnomalyResultResource(anomalyResultDAO));
    environment.jersey().register(new ContextualEventResource(contextualEventDAO));
    environment.jersey().register(new MetricsGraphicsTimeSeriesResource(thirdEyeClient, anomalyResultDAO));
    environment.jersey().register(new AnomalyDetectionJobResource(jobManager, anomalyFunctionSpecDAO));
    environment.jersey().register(new EmailReportResource(emailConfigurationDAO, emailReportJobManager));

    // Tasks
    environment.admin().addTask(new EmailReportJobManagerTask(emailReportJobManager, hibernate.getSessionFactory()));
  }
}
