package com.linkedin.thirdeye.dashboard;

import com.linkedin.thirdeye.dashboard.resources.AnomalyFunctionResource;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.linkedin.thirdeye.dashboard.resources.AnomalyResource;
import com.linkedin.thirdeye.dashboard.resources.AnomalySummaryResource;
import com.linkedin.thirdeye.dashboard.resources.CacheResource;
import com.linkedin.thirdeye.dashboard.resources.DashboardResource;
import com.linkedin.thirdeye.dashboard.resources.WebappConfigResource;

import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;

public class ThirdEyeDashboardApplication
    extends BaseThirdEyeApplication<ThirdEyeDashboardConfiguration> {
  public static final String WEBAPP_CONFIG = "/webapp-config";

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
    super.initDAOs();
    try {
      ThirdEyeCacheRegistry.initializeWebappCaches(config);
    } catch (Exception e) {
      LOG.error("Exception while loading caches", e);
    }
    env.jersey().register(new AnomalyFunctionResource(config));
    env.jersey().register(new AnomalySummaryResource(anomalyResultDAO, anomalyMergedResultDAO));
    env.jersey().register(new DashboardResource(BaseThirdEyeApplication.getDashboardConfigDAO(config)));
    env.jersey().register(new CacheResource());
    env.jersey().register(
        new AnomalyResource(config, anomalyFunctionDAO, anomalyResultDAO, emailConfigurationDAO, anomalyMergedResultDAO));
    env.jersey().register(new WebappConfigResource(webappConfigDAO));
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      throw new IllegalArgumentException("Please provide config directory as parameter");
    }
    String thirdEyeConfigDir = args[0];
    System.setProperty("dw.rootDir", thirdEyeConfigDir);
    String dashboardApplicationConfigFile = thirdEyeConfigDir + "/" + "dashboard.yml";
    new ThirdEyeDashboardApplication().run(new String[] {
        "server", dashboardApplicationConfigFile
    });
  }

}
