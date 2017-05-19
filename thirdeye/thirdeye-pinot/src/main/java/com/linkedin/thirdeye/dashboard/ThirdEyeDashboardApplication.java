package com.linkedin.thirdeye.dashboard;

import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.linkedin.thirdeye.dashboard.resources.AdminResource;
import com.linkedin.thirdeye.dashboard.resources.AnomalyFunctionResource;
import com.linkedin.thirdeye.dashboard.resources.AnomalyResource;
import com.linkedin.thirdeye.dashboard.resources.CacheResource;
import com.linkedin.thirdeye.dashboard.resources.DashboardResource;
import com.linkedin.thirdeye.dashboard.resources.DataCompletenessResource;
import com.linkedin.thirdeye.dashboard.resources.DatasetConfigResource;
import com.linkedin.thirdeye.dashboard.resources.EmailResource;
import com.linkedin.thirdeye.dashboard.resources.EntityManagerResource;
import com.linkedin.thirdeye.dashboard.resources.EntityMappingResource;
import com.linkedin.thirdeye.dashboard.resources.IngraphDashboardConfigResource;
import com.linkedin.thirdeye.dashboard.resources.IngraphMetricConfigResource;
import com.linkedin.thirdeye.dashboard.resources.JobResource;
import com.linkedin.thirdeye.dashboard.resources.MetricConfigResource;
import com.linkedin.thirdeye.dashboard.resources.OnboardDatasetMetricResource;
import com.linkedin.thirdeye.dashboard.resources.OnboardResource;
import com.linkedin.thirdeye.dashboard.resources.OverrideConfigResource;
import com.linkedin.thirdeye.dashboard.resources.SummaryResource;
import com.linkedin.thirdeye.dashboard.resources.ThirdEyeResource;
import com.linkedin.thirdeye.dashboard.resources.v2.AnomaliesResource;
import com.linkedin.thirdeye.dashboard.resources.v2.DataResource;
import com.linkedin.thirdeye.dashboard.resources.v2.EventResource;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseResource;
import com.linkedin.thirdeye.dashboard.resources.v2.TimeSeriesResource;
import com.linkedin.thirdeye.dashboard.resources.v2.rootcause.DefaultEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.rootcause.FormatterLoader;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.RCAFramework;
import com.linkedin.thirdeye.rootcause.impl.RCAFrameworkLoader;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import java.io.File;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Executors;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ThirdEyeDashboardApplication
    extends BaseThirdEyeApplication<ThirdEyeDashboardConfiguration> {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeDashboardApplication.class);

  @Override
  public String getName() {
    return "Thirdeye Dashboard";
  }

  @Override
  public void initialize(Bootstrap<ThirdEyeDashboardConfiguration> bootstrap) {
    bootstrap.addBundle(new ViewBundle());
    bootstrap.addBundle(new HelperBundle());
    bootstrap.addBundle(new AssetsBundle("/app/", "/app", "index.html", "app"));
    bootstrap.addBundle(new AssetsBundle("/assets", "/assets", null, "assets"));
    bootstrap.addBundle(new AssetsBundle("/assets/css", "/assets/css", null, "css"));
    bootstrap.addBundle(new AssetsBundle("/assets/js", "/assets/js", null, "js"));
    bootstrap.addBundle(new AssetsBundle("/assets/lib", "/assets/lib", null, "lib"));
    bootstrap.addBundle(new AssetsBundle("/assets/img", "/assets/img", null, "img"));
    bootstrap.addBundle(new AssetsBundle("/assets/data", "/assets/data", null, "data"));
  }

  @Override
  public void run(ThirdEyeDashboardConfiguration config, Environment env)
      throws Exception {
    LOG.info("isCors value {}", config.isCors());
    if (config.isCors()) {
      FilterRegistration.Dynamic corsFilter = env.servlets().addFilter("CORS", CrossOriginFilter.class);
      corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_METHODS_PARAM, "GET,PUT,POST,DELETE,OPTIONS");
      corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
      corsFilter.setInitParameter(CrossOriginFilter.ALLOWED_HEADERS_PARAM, "Content-Type,Authorization,X-Requested-With,Content-Length,Accept,Origin");
      corsFilter.addMappingForUrlPatterns(EnumSet.allOf(DispatcherType.class), true, "/*");
    }

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
    env.jersey().register(new EventResource(config));
    env.jersey().register(new DataCompletenessResource(DAO_REGISTRY.getDataCompletenessConfigDAO()));
    env.jersey().register(new EntityMappingResource());
    env.jersey().register(new RootCauseResource(makeRCAFramework(config), makeRCAFormatters(config)));
    env.jersey().register(new OnboardDatasetMetricResource());
  }

  private static RCAFramework makeRCAFramework(ThirdEyeDashboardConfiguration config) throws Exception {
    if(config.getRcaConfigPath() == null)
      throw new IllegalArgumentException("rcaConfigPath must not be null");
    File rcaConfigFile = new File(config.getRcaConfigPath());
    if(!rcaConfigFile.isAbsolute())
      rcaConfigFile = new File(config.getRootDir() + File.separator + rcaConfigFile);
    List<Pipeline> pipelines = RCAFrameworkLoader.getPipelinesFromConfig(rcaConfigFile, config.getRcaDefaultFramework());
    return new RCAFramework(pipelines, Executors.newFixedThreadPool(config.getRcaParallelism()));
  }

  private static List<RootCauseEntityFormatter> makeRCAFormatters(ThirdEyeDashboardConfiguration config) throws Exception {
    List<RootCauseEntityFormatter> formatters = new ArrayList<>();
    if(config.getRcaFormatters() != null) {
      for(String className : config.getRcaFormatters()) {
        try {
          formatters.add(FormatterLoader.fromClassName(className));
        } catch(ClassNotFoundException e) {
          LOG.warn("Could not find formatter class '{}'. Skipping.", className, e);
        }
      }
    }
    formatters.add(new DefaultEntityFormatter());
    return formatters;
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
