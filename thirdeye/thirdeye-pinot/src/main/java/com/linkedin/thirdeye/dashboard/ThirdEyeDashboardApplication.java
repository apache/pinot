package com.linkedin.thirdeye.dashboard;

import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.linkedin.thirdeye.dashboard.resources.AdminResource;
import com.linkedin.thirdeye.dashboard.resources.AnomalyFunctionResource;
import com.linkedin.thirdeye.dashboard.resources.AnomalyResource;
import com.linkedin.thirdeye.dashboard.resources.CacheResource;
import com.linkedin.thirdeye.dashboard.resources.DashboardResource;
import com.linkedin.thirdeye.dashboard.resources.DatasetConfigResource;
import com.linkedin.thirdeye.dashboard.resources.EmailResource;
import com.linkedin.thirdeye.dashboard.resources.EntityManagerResource;
import com.linkedin.thirdeye.dashboard.resources.IngraphDashboardConfigResource;
import com.linkedin.thirdeye.dashboard.resources.IngraphMetricConfigResource;
import com.linkedin.thirdeye.dashboard.resources.JobResource;
import com.linkedin.thirdeye.dashboard.resources.MetricConfigResource;
import com.linkedin.thirdeye.dashboard.resources.OverrideConfigResource;
import com.linkedin.thirdeye.dashboard.resources.SummaryResource;
import com.linkedin.thirdeye.dashboard.resources.ThirdEyeResource;
import com.linkedin.thirdeye.dashboard.resources.v2.AnomaliesResource;
import com.linkedin.thirdeye.dashboard.resources.OnboardResource;
import com.linkedin.thirdeye.dashboard.resources.v2.DataResource;

import com.linkedin.thirdeye.dashboard.resources.v2.EventResource;
import com.linkedin.thirdeye.dashboard.resources.v2.TimeSeriesResource;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;

public class ThirdEyeDashboardApplication
    extends BaseThirdEyeApplication<ThirdEyeDashboardConfiguration> {

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
  public void run(ThirdEyeDashboardConfiguration config, Environment env)
      throws Exception {
    super.initDAOs();
    try {
      ThirdEyeCacheRegistry.initializeCaches(config);
    } catch (Exception e) {
      LOG.error("Exception while loading caches", e);
    }

    AnomalyFunctionFactory anomalyFunctionFactory = new AnomalyFunctionFactory(config.getFunctionConfigPath());
    AlertFilterFactory alertFilterFactory = new AlertFilterFactory(config.getAlertFilterConfigPath());

    env.jersey().register(new AnomalyFunctionResource(config.getFunctionConfigPath()));
    env.jersey().register(new DashboardResource());
    env.jersey().register(new CacheResource());
    env.jersey().register(new AnomalyResource(anomalyFunctionFactory, alertFilterFactory));
    env.jersey().register(new EmailResource(config));
    env.jersey().register(new EntityManagerResource());
    env.jersey().register(new IngraphMetricConfigResource());
    env.jersey().register(new MetricConfigResource());
    env.jersey().register(new DatasetConfigResource());
    env.jersey().register(new IngraphDashboardConfigResource());
    env.jersey().register(new JobResource());
    env.jersey().register(new AdminResource());
    env.jersey().register(new SummaryResource());
    env.jersey().register(new ThirdEyeResource());
    env.jersey().register(new OverrideConfigResource());
    env.jersey().register(new DataResource(anomalyFunctionFactory, alertFilterFactory));
    env.jersey().register(new AnomaliesResource(anomalyFunctionFactory, alertFilterFactory));
    env.jersey().register(new TimeSeriesResource());
    env.jersey().register(new OnboardResource());
    env.jersey().register(new EventResource(config.getInformedApiUrl()));
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      throw new IllegalArgumentException("Please provide config directory as parameter");
    }
    String thirdEyeConfigDir = args[0];
    System.setProperty("dw.rootDir", thirdEyeConfigDir);
    String dashboardApplicationConfigFile = thirdEyeConfigDir + "/" + "dashboard.yml";
    new ThirdEyeDashboardApplication().run("server", dashboardApplicationConfigFile);
  }

}
