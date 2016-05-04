package com.linkedin.thirdeye.detector;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.server.AbstractNetworkConnector;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.QueryCache;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.comparison.TimeOnTimeComparisonHandler;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesHandler;
import com.linkedin.thirdeye.client.timeseries.TimeSeriesResponseConverter;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionRelation;
import com.linkedin.thirdeye.detector.api.AnomalyFunctionSpec;
import com.linkedin.thirdeye.detector.api.AnomalyResult;
import com.linkedin.thirdeye.detector.api.ContextualEvent;
import com.linkedin.thirdeye.detector.api.EmailConfiguration;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionRelationDAO;
import com.linkedin.thirdeye.detector.db.AnomalyFunctionSpecDAO;
import com.linkedin.thirdeye.detector.db.AnomalyResultDAO;
import com.linkedin.thirdeye.detector.db.ContextualEventDAO;
import com.linkedin.thirdeye.detector.db.EmailConfigurationDAO;
import com.linkedin.thirdeye.detector.db.HibernateSessionWrapper;
import com.linkedin.thirdeye.detector.driver.AnomalyDetectionJobManager;
import com.linkedin.thirdeye.detector.email.EmailReportJobManager;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.resources.AnomalyDetectionJobResource;
import com.linkedin.thirdeye.detector.resources.AnomalyFunctionRelationResource;
import com.linkedin.thirdeye.detector.resources.AnomalyFunctionSpecResource;
import com.linkedin.thirdeye.detector.resources.AnomalyResultResource;
import com.linkedin.thirdeye.detector.resources.ContextualEventResource;
import com.linkedin.thirdeye.detector.resources.EmailReportResource;
import com.linkedin.thirdeye.detector.resources.MetricsGraphicsTimeSeriesResource;
import com.linkedin.thirdeye.detector.task.EmailReportJobManagerTask;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.HibernateBundle;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.lifecycle.ServerLifecycleListener;
import io.dropwizard.migrations.MigrationsBundle;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.server.SimpleServerFactory;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class ThirdEyeDetectorApplication extends Application<ThirdEyeDetectorConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeDetectorApplication.class);

  private final HibernateBundle<ThirdEyeDetectorConfiguration> hibernate =
      new HibernateBundle<ThirdEyeDetectorConfiguration>(AnomalyFunctionSpec.class,
          AnomalyFunctionRelation.class, AnomalyResult.class, ContextualEvent.class,
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
  public void run(final ThirdEyeDetectorConfiguration config, final Environment environment)
      throws Exception {
    // DAO
    final AnomalyFunctionSpecDAO anomalyFunctionSpecDAO =
        new AnomalyFunctionSpecDAO(hibernate.getSessionFactory());
    final AnomalyResultDAO anomalyResultDAO = new AnomalyResultDAO(hibernate.getSessionFactory());
    final ContextualEventDAO contextualEventDAO =
        new ContextualEventDAO(hibernate.getSessionFactory());
    final EmailConfigurationDAO emailConfigurationDAO =
        new EmailConfigurationDAO(hibernate.getSessionFactory());
    final AnomalyFunctionRelationDAO anomalyFunctionRelationDAO =
        new AnomalyFunctionRelationDAO(hibernate.getSessionFactory());

    LOG.info("Loading client configs from: {}", config.getClientConfigRoot());
    // TODO fix this up to be more standardized
    // ThirdEye client
    final ThirdEyeClient thirdEyeClient =
        // CollectionMapThirdEyeClient.fromFolder(config.getClientConfigRoot());
        PinotThirdEyeClient.getDefaultTestClient(); // TODO make this configurable
    QueryCache queryCache = new QueryCache(thirdEyeClient, Executors.newFixedThreadPool(10));
    final TimeSeriesHandler timeSeriesHandler = new TimeSeriesHandler(queryCache);
    TimeOnTimeComparisonHandler timeOnTimeComparisonHandler =
        new TimeOnTimeComparisonHandler(queryCache);
    TimeSeriesResponseConverter timeSeriesResponseConverter =
        TimeSeriesResponseConverter.getInstance();
    // LOG.info("CollectionMapThirdEyeClient loaded: {}", thirdEyeClient.getCollections());
    // TODO fix logging
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

    final AnomalyFunctionFactory anomalyFunctionFactory =
        new AnomalyFunctionFactory(config.getFunctionConfigPath());

    // ThirdEye driver
    final AnomalyDetectionJobManager jobManager = new AnomalyDetectionJobManager(quartzScheduler,
        timeSeriesHandler, timeSeriesResponseConverter, anomalyFunctionSpecDAO,
        anomalyFunctionRelationDAO, anomalyResultDAO, hibernate.getSessionFactory(),
        environment.metrics(), anomalyFunctionFactory);

    // Start all active jobs on startup
    environment.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {
        new HibernateSessionWrapper<Void>(hibernate.getSessionFactory())
            .execute(new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                List<AnomalyFunctionSpec> functions;
                functions = anomalyFunctionSpecDAO.findAll();
                LinkedList<AnomalyFunctionSpec> failedToStart =
                    new LinkedList<AnomalyFunctionSpec>();
                for (AnomalyFunctionSpec function : functions) {
                  if (function.getIsActive()) {
                    try {
                      LOG.info("Starting {}", function);
                      jobManager.start(function.getId());
                    } catch (Exception e) {
                      LOG.error("Failed to schedule function " + function.getId(), e);
                      failedToStart.add(function);
                    }
                  }
                }
                if (!failedToStart.isEmpty()) {
                  LOG.warn("{} functions failed to start!: {}", failedToStart.size(),
                      failedToStart);
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
    int dropwizardConfigPort = getApplicationPortNumber(config);
    LOG.info("Dropwizard config port: {}", dropwizardConfigPort);
    if (dropwizardConfigPort > 0) {
      applicationPort.set(dropwizardConfigPort);
    }

    final EmailReportJobManager emailReportJobManager =
        new EmailReportJobManager(quartzScheduler, emailConfigurationDAO, anomalyResultDAO,
            hibernate.getSessionFactory(), applicationPort, timeOnTimeComparisonHandler);

    environment.lifecycle().addServerLifecycleListener(new ServerLifecycleListener() {
      @Override
      public void serverStarted(Server server) {
        LOG.info("{} server connectors found", server.getConnectors().length);
        for (Connector connector : server.getConnectors()) {
          LOG.info("Connector: {}", connector.getName());
          if (connector instanceof ServerConnector) {
            ServerConnector serverConnector = (ServerConnector) connector;
            int localPort = serverConnector.getLocalPort();
            applicationPort.set(localPort);
            LOG.info("application port set to {} from server connector", localPort);
            break;
          } else if (connector instanceof AbstractNetworkConnector) {
            AbstractNetworkConnector networkConnector = (AbstractNetworkConnector) connector;
            int localPort = networkConnector.getLocalPort();
            applicationPort.set(localPort);
            LOG.info("application port set to {} from network connector", localPort);
            break;
          }
        }
        LOG.info("Port from jetty server: {}", applicationPort.get());

        if (applicationPort.get() == -1) {
          throw new IllegalStateException("Could not determine application port");
        }

        // Start the email report job manager once we know the application port
        try {
          new HibernateSessionWrapper<Void>(hibernate.getSessionFactory())
              .execute(new Callable<Void>() {
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
    environment.jersey().register(
        new MetricsGraphicsTimeSeriesResource(timeOnTimeComparisonHandler, anomalyResultDAO));
    environment.jersey()
        .register(new AnomalyDetectionJobResource(jobManager, anomalyFunctionSpecDAO));
    environment.jersey()
        .register(new EmailReportResource(emailConfigurationDAO, emailReportJobManager));

    // Tasks
    environment.admin().addTask(
        new EmailReportJobManagerTask(emailReportJobManager, hibernate.getSessionFactory()));
  }

  public static int getApplicationPortNumber(ThirdEyeDetectorConfiguration config) {
    int httpPort = 0;
    try {
      // https://github.com/dropwizard/dropwizard/issues/745
      ServerFactory serverFactory = config.getServerFactory();
      if (serverFactory instanceof DefaultServerFactory) {
        DefaultServerFactory defaultFactory = (DefaultServerFactory) config.getServerFactory();
        for (ConnectorFactory connectorFactory : defaultFactory.getApplicationConnectors()) {
          if (connectorFactory.getClass().isAssignableFrom(HttpConnectorFactory.class)) {
            httpPort = ((HttpConnectorFactory) connectorFactory).getPort();
            break;
          }
        }
      } else if (serverFactory instanceof SimpleServerFactory) {
        SimpleServerFactory simpleFactory = (SimpleServerFactory) serverFactory;
        HttpConnectorFactory connector = (HttpConnectorFactory) simpleFactory.getConnector();
        if (connector.getClass().isAssignableFrom(HttpConnectorFactory.class)) {
          httpPort = connector.getPort();
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to retrieve app port number from dropwizard server factory", e);
    }

    return httpPort;
  }
}
