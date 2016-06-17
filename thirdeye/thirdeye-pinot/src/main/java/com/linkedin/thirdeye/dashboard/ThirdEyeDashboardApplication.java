package com.linkedin.thirdeye.dashboard;

import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.linkedin.thirdeye.dashboard.resources.AnomalyResource;
import com.linkedin.thirdeye.dashboard.resources.CacheResource;
import com.linkedin.thirdeye.dashboard.resources.DashboardResource;
import com.linkedin.thirdeye.detector.driver.AnomalyDetectionJobManager;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;

import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;

public class ThirdEyeDashboardApplication
    extends BaseThirdEyeApplication<ThirdEyeDashboardConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeDashboardApplication.class);

  public static final String WEBAPP_CONFIG = "/webapp-config";

  public ThirdEyeDashboardApplication() {
  }

  @Override
  public String getName() {
    return "Thirdeye Dashboard";
  }

  @Override
  public void initialize(Bootstrap<ThirdEyeDashboardConfiguration> bootstrap) {
    bootstrap.addBundle(new ViewBundle());
    bootstrap.addBundle(new HelperBundle());
    bootstrap.addBundle(hibernateBundle);
    bootstrap.addBundle(new AssetsBundle("/assets", "/assets"));
    bootstrap.addBundle(new AssetsBundle("/assets/css", "/assets/css", null, "css"));
    bootstrap.addBundle(new AssetsBundle("/assets/js", "/assets/js", null, "js"));
    bootstrap.addBundle(new AssetsBundle("/assets/lib", "/assets/lib", null, "lib"));
    bootstrap.addBundle(new AssetsBundle("/assets/img", "/assets/img", null, "img"));
    bootstrap.addBundle(new AssetsBundle("/assets/data", "/assets/data", null, "data"));
  }

  @Override
  public void run(ThirdEyeDashboardConfiguration config, Environment env) throws Exception {
    super.initDetectorRelatedDAO();

    try {
      ThirdEyeCacheRegistry.initializeWebappCaches(config);
    } catch (Exception e) {
      LOG.error("Exception while loading caches", e);
    }

    // Quartz Scheduler
    SchedulerFactory schedulerFactory = new StdSchedulerFactory();
    final Scheduler quartzScheduler = schedulerFactory.getScheduler();
    env.lifecycle().manage(new Managed() {
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

    env.jersey().register(new DashboardResource(BaseThirdEyeApplication.getDashboardConfigDAO(config)));
    env.jersey().register(new CacheResource());
    AnomalyDetectionJobManager anomalyDetectionJobManager = new AnomalyDetectionJobManager(quartzScheduler,
        anomalyFunctionSpecDAO,  anomalyFunctionRelationDAO, anomalyResultDAO,
        hibernateBundle.getSessionFactory(), env.metrics(),
        new AnomalyFunctionFactory(config.getFunctionConfigPath()),
        config.getFailureEmailConfig());
    env.jersey().register(new AnomalyResource(anomalyDetectionJobManager, anomalyFunctionSpecDAO,
        anomalyResultDAO, emailConfigurationDAO));
  }

  public static void main(String[] args) throws Exception {
    String thirdEyeConfigDir = args[0];
    System.setProperty("dw.rootDir", thirdEyeConfigDir);
    String dashboardApplicationConfigFile = thirdEyeConfigDir + "/" + "dashboard.yml";
    new ThirdEyeDashboardApplication().run(new String[] {
        "server", dashboardApplicationConfigFile
    });
  }
}
