package com.linkedin.thirdeye.dashboard;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.cache.CacheBuilder;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobScheduler;
import com.linkedin.thirdeye.anomaly.onboard.FunctionOnboardingResource;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import com.linkedin.thirdeye.auth.AuthCookieSerializer;
import com.linkedin.thirdeye.auth.Credentials;
import com.linkedin.thirdeye.auth.ThirdEyeAuthenticatorDisabled;
import com.linkedin.thirdeye.auth.ThirdEyeAuthenticatorLdap;
import com.linkedin.thirdeye.auth.ThirdEyePrincipal;
import com.linkedin.thirdeye.auth.ThirdEyeAuthFilter;
import com.linkedin.thirdeye.common.BaseThirdEyeApplication;
import com.linkedin.thirdeye.dashboard.configs.AuthConfiguration;
import com.linkedin.thirdeye.dashboard.configs.ResourceConfiguration;
import com.linkedin.thirdeye.dashboard.resources.AdminResource;
import com.linkedin.thirdeye.dashboard.resources.AnomalyFunctionResource;
import com.linkedin.thirdeye.dashboard.resources.AnomalyResource;
import com.linkedin.thirdeye.dashboard.resources.AutoOnboardResource;
import com.linkedin.thirdeye.dashboard.resources.CacheResource;
import com.linkedin.thirdeye.dashboard.resources.DashboardResource;
import com.linkedin.thirdeye.dashboard.resources.DataCompletenessResource;
import com.linkedin.thirdeye.dashboard.resources.DatasetConfigResource;
import com.linkedin.thirdeye.dashboard.resources.DetectionJobResource;
import com.linkedin.thirdeye.dashboard.resources.EmailResource;
import com.linkedin.thirdeye.dashboard.resources.EntityManagerResource;
import com.linkedin.thirdeye.dashboard.resources.EntityMappingResource;
import com.linkedin.thirdeye.dashboard.resources.JobResource;
import com.linkedin.thirdeye.dashboard.resources.MetricConfigResource;
import com.linkedin.thirdeye.dashboard.resources.OnboardDatasetMetricResource;
import com.linkedin.thirdeye.dashboard.resources.OnboardResource;
import com.linkedin.thirdeye.dashboard.resources.OverrideConfigResource;
import com.linkedin.thirdeye.dashboard.resources.SummaryResource;
import com.linkedin.thirdeye.dashboard.resources.ThirdEyeResource;
import com.linkedin.thirdeye.dashboard.resources.v2.AggregationResource;
import com.linkedin.thirdeye.dashboard.resources.v2.AnomaliesResource;
import com.linkedin.thirdeye.dashboard.resources.v2.AuthResource;
import com.linkedin.thirdeye.dashboard.resources.v2.ConfigResource;
import com.linkedin.thirdeye.dashboard.resources.v2.DataResource;
import com.linkedin.thirdeye.dashboard.resources.v2.EventResource;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseResource;
import com.linkedin.thirdeye.dashboard.resources.v2.RootCauseSessionResource;
import com.linkedin.thirdeye.dashboard.resources.v2.TimeSeriesResource;
import com.linkedin.thirdeye.dashboard.resources.v2.aggregation.DefaultAggregationLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.rootcause.DefaultEntityFormatter;
import com.linkedin.thirdeye.dashboard.resources.v2.rootcause.FormatterLoader;
import com.linkedin.thirdeye.dashboard.resources.v2.timeseries.DefaultTimeSeriesLoader;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.detector.email.filter.AlertFilterFactory;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.rootcause.RCAFramework;
import com.linkedin.thirdeye.rootcause.impl.RCAFrameworkLoader;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.auth.Authenticator;
import io.dropwizard.auth.CachingAuthenticator;
import io.dropwizard.bundles.redirect.PathRedirect;
import io.dropwizard.bundles.redirect.RedirectBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.views.ViewBundle;
import java.io.File;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.crypto.spec.SecretKeySpec;
import javax.servlet.DispatcherType;
import javax.servlet.FilterRegistration;
import javax.servlet.ServletRegistration;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.proxy.ProxyServlet;
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

  @SuppressWarnings("unchecked")
  @Override
  public void initialize(Bootstrap<ThirdEyeDashboardConfiguration> bootstrap) {
    bootstrap.addBundle(new ViewBundle());
    bootstrap.addBundle(new HelperBundle());
    bootstrap.addBundle(new RedirectBundle(new PathRedirect("/", "/thirdeye")));
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
    EmailResource emailResource = new EmailResource(config);

    env.jersey().register(new AnomalyFunctionResource(config.getFunctionConfigPath()));
    env.jersey().register(new DashboardResource());
    env.jersey().register(new CacheResource());
    env.jersey().register(new AnomalyResource(anomalyFunctionFactory, alertFilterFactory));
    env.jersey().register(emailResource);
    env.jersey().register(new EntityManagerResource(config));
    env.jersey().register(new MetricConfigResource());
    env.jersey().register(new DatasetConfigResource());
    env.jersey().register(new JobResource());
    env.jersey().register(new AdminResource());
    env.jersey().register(new SummaryResource());
    env.jersey().register(new ThirdEyeResource());
    env.jersey().register(new OverrideConfigResource());
    env.jersey().register(new DataResource(anomalyFunctionFactory, alertFilterFactory));
    env.jersey().register(new AnomaliesResource(anomalyFunctionFactory, alertFilterFactory));
    env.jersey().register(new TimeSeriesResource(Executors.newCachedThreadPool(),
        new DefaultTimeSeriesLoader(DAO_REGISTRY.getMetricConfigDAO(), DAO_REGISTRY.getDatasetConfigDAO(), ThirdEyeCacheRegistry.getInstance().getQueryCache())));
    env.jersey().register(new AggregationResource(Executors.newCachedThreadPool(),
        new DefaultAggregationLoader(DAO_REGISTRY.getMetricConfigDAO(), DAO_REGISTRY.getDatasetConfigDAO(), ThirdEyeCacheRegistry.getInstance().getQueryCache())));
    env.jersey().register(new OnboardResource());
    env.jersey().register(new EventResource(config));
    env.jersey().register(new DataCompletenessResource(DAO_REGISTRY.getDataCompletenessConfigDAO()));
    env.jersey().register(new EntityMappingResource());
    env.jersey().register(new OnboardDatasetMetricResource());
    env.jersey().register(new AutoOnboardResource(config));
    env.jersey().register(new ConfigResource(DAO_REGISTRY.getConfigDAO()));
    env.jersey().register(new RootCauseSessionResource(DAO_REGISTRY.getRootcauseSessionDAO(), new ObjectMapper()));
    env.jersey().register(new FunctionOnboardingResource());

    if (config.getOnboardingHost() != null) {
      LOG.info("Setting up onboarding proxy for '{}'", config.getOnboardingHost());
      ServletRegistration.Dynamic proxy = env.servlets().addServlet("detection-onboard", ProxyServlet.Transparent.class);
      proxy.setInitParameter("proxyTo", config.getOnboardingHost());
      proxy.setInitParameter("prefix", "/detection-onboard");
      proxy.addMapping("/detection-onboard/*");
    }

    env.getObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    /*
      Adding DetectionJobResource in Dashboard to allow replay/autotune from ui.
      creating a lightweight detection scheduler instance
      Do not call start() as this instance is only meant to run replay/autotune
     */
    DetectionJobScheduler detectionJobScheduler = new DetectionJobScheduler();
    AlertFilterAutotuneFactory alertFilterAutotuneFactory = new AlertFilterAutotuneFactory(config.getFilterAutotuneConfigPath());
    env.jersey().register(new DetectionJobResource(detectionJobScheduler, alertFilterFactory, alertFilterAutotuneFactory));

    try {
      // root cause resource
      if (config.getRootCause() != null) {
        env.jersey().register(makeRootCauseResource(config));
      }

      // Load external resources
      if (config.getResourceConfig() != null) {
        List<ResourceConfiguration> resourceConfigurations = config.getResourceConfig();
        for (ResourceConfiguration resourceConfiguration : resourceConfigurations) {
          env.jersey().register(Class.forName(resourceConfiguration.getClassName()));
          LOG.info("Registering resource [{}]", resourceConfiguration.getClassName());
        }
      }
    } catch (Exception e) {
      LOG.error("Error loading the resource", e);
    }

    if (config.getAuthConfig() != null) {
      final AuthConfiguration authConfig = config.getAuthConfig();
      final SecretKeySpec aesKey = new SecretKeySpec(authConfig.getAuthKey().getBytes("UTF-8"), "AES");
      final AuthCookieSerializer serializer = new AuthCookieSerializer(aesKey, new ObjectMapper());

      // default permissive authenticator
      Authenticator<Credentials, ThirdEyePrincipal> authenticator = new ThirdEyeAuthenticatorDisabled();

      // ldap authenticator
      if (authConfig.isAuthEnabled()) {
        final ThirdEyeAuthenticatorLdap authenticatorLdap = new ThirdEyeAuthenticatorLdap(authConfig.getDomainSuffix(), authConfig.getLdapUrl());
        authenticator = new CachingAuthenticator<>(env.metrics(), authenticatorLdap, CacheBuilder.newBuilder().expireAfterWrite(authConfig.getCacheTTL(), TimeUnit.SECONDS));
      }

      // auth filter
      env.jersey().register(new ThirdEyeAuthFilter(authenticator, serializer, authConfig.getAllowedPaths()));

      // auth resource
      env.jersey().register(new AuthResource(authenticator, serializer, authConfig.getCookieTTL() * 1000));
    }
  }

  private static RootCauseResource makeRootCauseResource(ThirdEyeDashboardConfiguration config) throws Exception {
    File definitionsFile = getRootCauseDefinitionsFile(config);
    if(!definitionsFile.exists())
      throw new IllegalArgumentException(String.format("Could not find definitions file '%s'", definitionsFile));

    RootCauseConfiguration rcConfig = config.getRootCause();
    return new RootCauseResource(
        makeRootCauseFrameworks(rcConfig, definitionsFile),
        makeRootCauseFormatters(rcConfig));
  }

  private static Map<String, RCAFramework> makeRootCauseFrameworks(RootCauseConfiguration config, File definitionsFile) throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(config.getParallelism());
    return RCAFrameworkLoader.getFrameworksFromConfig(definitionsFile, executor);
  }

  private static List<RootCauseEntityFormatter> makeRootCauseFormatters(RootCauseConfiguration config) throws Exception {
    List<RootCauseEntityFormatter> formatters = new ArrayList<>();
    if(config.getFormatters() != null) {
      for(String className : config.getFormatters()) {
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

  private static File getRootCauseDefinitionsFile(ThirdEyeDashboardConfiguration config) {
    if(config.getRootCause().getDefinitionsPath() == null)
      throw new IllegalArgumentException("definitionsPath must not be null");
    File rcaConfigFile = new File(config.getRootCause().getDefinitionsPath());
    if(!rcaConfigFile.isAbsolute())
      return new File(config.getRootDir() + File.separator + rcaConfigFile);
    return rcaConfigFile;
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
