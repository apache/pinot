package com.linkedin.thirdeye.detector;


import java.util.ArrayList;
import java.util.Arrays;

import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.db.DataSourceFactory;
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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.server.AbstractNetworkConnector;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.EmailConfiguration;
import com.linkedin.thirdeye.detector.db.HibernateSessionWrapper;
import com.linkedin.thirdeye.detector.driver.AnomalyDetectionJobManager;
import com.linkedin.thirdeye.detector.email.EmailReportJobManager;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.lib.util.JobUtils;
import com.linkedin.thirdeye.detector.resources.AnomalyDetectionJobResource;
import com.linkedin.thirdeye.detector.resources.AnomalyFunctionRelationResource;
import com.linkedin.thirdeye.detector.resources.AnomalyFunctionSpecResource;
import com.linkedin.thirdeye.detector.resources.AnomalyResultResource;
import com.linkedin.thirdeye.detector.resources.EmailFunctionDependencyResource;
import com.linkedin.thirdeye.detector.resources.EmailReportJobResource;
import com.linkedin.thirdeye.detector.resources.EmailReportResource;
import com.linkedin.thirdeye.detector.resources.MetricsGraphicsTimeSeriesResource;

public class ThirdEyeDetectorApplication
    extends BaseThirdEyeApplication<ThirdEyeDetectorConfiguration> {
  private static final String QUARTZ_MISFIRE_THRESHOLD = "3600000"; // 1 hour
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeDetectorApplication.class);

  /**
   * Entry point for the detector application, used to start up the app server and handle database
   * migrations. There must be at least one argument, with the root directory for all application
   * configs (including <code>detector.yml</code>) provided as the last argument. If no preceding
   * arguments are provided, the application server will startup using <code>detector.yml</code>.
   * <br>
   * If additional arguments do precede the root configuration directory, they are passed as
   * arguments to the Dropwizard application along with the <code>detector.yml</code> file. As an
   * example, one could pass in the arguments:
   * <br>
   * <br>
   * <code>db migrate <i>config_folder</i></code>
   * <br>
   * <br>
   * to run database migrations using configurations in <code>detector.yml</code>.
   * @throws Exception
   */
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
    new ThirdEyeDetectorApplication().run(argList.toArray(new String[argList.size()]));
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

    try {
      ThirdEyeCacheRegistry.initializeDetectorCaches(config);
    } catch (Exception e) {
      LOG.error("Exception while loading caches", e);
    }

    // Quartz Scheduler
    System.setProperty("org.quartz.jobStore.misfireThreshold", QUARTZ_MISFIRE_THRESHOLD);
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
    final AnomalyDetectionJobManager anomalyDetectionJobManager = new AnomalyDetectionJobManager(quartzScheduler,
        anomalyFunctionSpecDAO, anomalyFunctionRelationDAO, anomalyResultDAO, hibernateBundle.getSessionFactory(),
        environment.metrics(), anomalyFunctionFactory, config.getFailureEmailConfig());

    // Start all active jobs on startup
    environment.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {
        new HibernateSessionWrapper<Void>(hibernateBundle.getSessionFactory())
            .execute(new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                List<AnomalyFunctionSpec> functions = anomalyFunctionSpecDAO.findAll();
                LinkedList<AnomalyFunctionSpec> failedToStart = new LinkedList<AnomalyFunctionSpec>();
                for (AnomalyFunctionSpec function : functions) {
                  if (function.getIsActive()) {
                    try {
                      LOG.info("Starting {}", function);
                      anomalyDetectionJobManager.start(function.getId());
                    } catch (Exception e) {
                      LOG.error("Failed to schedule function " + function.getId(), e);
                      failedToStart.add(function);
                    }
                  }
                }
                if (!failedToStart.isEmpty()) {
                  LOG.warn("{} functions failed to start!: {}", failedToStart.size(),
                      failedToStart);
                  String subject = String.format("Startup failed to initialize %d functions",
                      failedToStart.size());
                  String body = StringUtils.join(failedToStart, "\n");
                  JobUtils.sendFailureEmail(config.getFailureEmailConfig(), subject, body);
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
            hibernateBundle.getSessionFactory(), applicationPort,
            config.getDashboardHost(), config.getFailureEmailConfig());

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
          new HibernateSessionWrapper<Void>(hibernateBundle.getSessionFactory())
              .execute(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                  List<EmailConfiguration> configs = emailConfigurationDAO.findAll();
                  LinkedList<EmailConfiguration> failedToStart = new LinkedList<EmailConfiguration>();
                  for (EmailConfiguration config : configs) {
                    if (config.getIsActive()) {
                      try {
                        LOG.info("Starting {}", config);
                        emailReportJobManager.start(config.getId());
                      } catch (Exception e) {
                        LOG.error("Failed to schedule report " + config.getId(), e);
                        failedToStart.add(config);
                      }
                    }
                  }
                  if (!failedToStart.isEmpty()) {
                    LOG.warn("{} reports failed to start!: {}", failedToStart.size(), failedToStart);
                    String subject = String.format("Startup failed to initialize %d reports",
                        failedToStart.size());
                    String body = StringUtils.join(failedToStart, "\n");
                    JobUtils.sendFailureEmail(config.getFailureEmailConfig(), subject, body);
                  }
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
    environment.jersey().register(new MetricsGraphicsTimeSeriesResource(anomalyResultDAO));
    environment.jersey().register(new AnomalyDetectionJobResource(anomalyDetectionJobManager, anomalyFunctionSpecDAO));
    environment.jersey().register(new EmailReportJobResource(emailReportJobManager, emailConfigurationDAO));
    environment.jersey().register(new EmailReportResource(emailConfigurationDAO, emailReportJobManager));
    environment.jersey().register(new EmailFunctionDependencyResource(emailFunctionDependencyDAO));

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
