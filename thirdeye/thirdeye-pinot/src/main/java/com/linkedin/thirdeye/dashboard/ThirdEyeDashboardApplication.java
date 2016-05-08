package com.linkedin.thirdeye.dashboard;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.client.QueryCache;
import com.linkedin.thirdeye.client.ThirdEyeClient;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClient;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientFactory;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.dashboard.configs.AbstractConfigDAO;
import com.linkedin.thirdeye.dashboard.configs.DashboardConfig;
import com.linkedin.thirdeye.dashboard.configs.FileBasedConfigDAOFactory;
import com.linkedin.thirdeye.dashboard.resources.DashboardResource;
import com.linkedin.thirdeye.detector.ThirdEyeDetectorApplication;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;

public class ThirdEyeDashboardApplication extends Application<ThirdEyeDashboardConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeDashboardApplication.class);

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
    bootstrap.addBundle(new AssetsBundle("/assets", "/assets"));
    bootstrap.addBundle(new AssetsBundle("/assets/css", "/assets/css", null, "css"));
    bootstrap.addBundle(new AssetsBundle("/assets/js", "/assets/js", null, "js"));
    bootstrap.addBundle(new AssetsBundle("/assets/lib", "/assets/lib", null, "lib"));
    bootstrap.addBundle(new AssetsBundle("/assets/img", "/assets/img", null, "img"));
    bootstrap.addBundle(new AssetsBundle("/assets/data", "/assets/data", null, "data"));
  }

  @Override
  public void run(ThirdEyeDashboardConfiguration config, Environment env) throws Exception {
    ThirdEyeClient thirdEyeClient = PinotThirdEyeClientFactory.createThirdEyeClient(config);
    ExecutorService queryExecutor = env.lifecycle().executorService("query_executor").build();
    QueryCache queryCache = createQueryCache(thirdEyeClient, queryExecutor);
    AbstractConfigDAO<DashboardConfig> configDAO = getDashboardConfig(config);

    env.jersey().register(new DashboardResource(queryCache, configDAO));
  }

  private QueryCache createQueryCache(ThirdEyeClient thirdEyeClient,
      ExecutorService queryExecutor) {
    QueryCache queryCache = new QueryCache(thirdEyeClient, Executors.newFixedThreadPool(10));
    return queryCache;
  }



  private AbstractConfigDAO<DashboardConfig> getDashboardConfig(
      ThirdEyeDashboardConfiguration config) {
    String configRootDir = config.getRootDir();
    String dashboardConfigDir = configRootDir + "/dashboard-config";
    FileBasedConfigDAOFactory configDAOFactory = new FileBasedConfigDAOFactory(dashboardConfigDir);
    AbstractConfigDAO<DashboardConfig> configDAO = configDAOFactory.getDashboardConfigDAO();
    return configDAO;
  }

  public static void main(String[] args) throws Exception {
    String thirdEyeConfigDir = args[0];
    System.setProperty("dw.rootDir", thirdEyeConfigDir);
    String dashboardApplicationConfigFile = thirdEyeConfigDir +"/" + "dashboard.yml";
    new ThirdEyeDashboardApplication().run(new String[]{"server", dashboardApplicationConfigFile});
  }
}
