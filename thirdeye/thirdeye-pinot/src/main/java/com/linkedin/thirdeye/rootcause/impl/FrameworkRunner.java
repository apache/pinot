package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.events.DefaultDeploymentEventProvider;
import com.linkedin.thirdeye.anomaly.events.DefaultHolidayEventProvider;
import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.anomaly.events.HistoricalAnomalyEventProvider;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.rootcause.Aggregator;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Framework;
import com.linkedin.thirdeye.rootcause.FrameworkResult;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.SearchContext;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;


public class FrameworkRunner {
  private static final String CLI_CONFIG_DIR = "config-dir";
  private static final String CLI_WINDOW_SIZE = "window-size";
  private static final String CLI_BASELINE_OFFSET = "baseline-offset";

  private static final long DAY_IN_MS = 24 * 3600 * 1000;

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    Option optConfig = new Option("c", CLI_CONFIG_DIR, true, "ThirdEye configuration file");
    optConfig.setRequired(true);
    options.addOption(optConfig);

    options.addOption(null, CLI_WINDOW_SIZE, true, "window size for search window (in days)");
    options.addOption(null, CLI_BASELINE_OFFSET, true, "baseline offset (in days)");

    Parser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(FrameworkRunner.class.getSimpleName(), options);
      System.exit(1);
    }

    // config
    File config = new File(cmd.getOptionValue(CLI_CONFIG_DIR));

    File daoConfig = new File(config.getAbsolutePath() + "/persistence.yml");
    DaoProviderUtil.init(daoConfig);

    ThirdEyeConfiguration thirdEyeConfig = new ThirdEyeAnomalyConfiguration();
    thirdEyeConfig.setRootDir(config.getAbsolutePath());
    ThirdEyeCacheRegistry.initializeCaches(thirdEyeConfig);

    List<Pipeline> pipelines = new ArrayList<>();

    // EventTime pipeline
    EventDataProviderManager eventProvider = EventDataProviderManager.getInstance();
    eventProvider.registerEventDataProvider(EventType.HOLIDAY, new DefaultHolidayEventProvider());
    eventProvider.registerEventDataProvider(EventType.HISTORICAL_ANOMALY, new HistoricalAnomalyEventProvider());
    pipelines.add(new EventTimePipeline(eventProvider));

    // EventMetric pipeline
    QueryCache cache = ThirdEyeCacheRegistry.getInstance().getQueryCache();
    MetricDimensionScorer scorer = new MetricDimensionScorer(cache);
    pipelines.add(new EventMetricPipeline(eventProvider));

    // MetricDimension pipeline
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    pipelines.add(new MetricDimensionPipeline(metricDAO, datasetDAO, scorer));

    // MetricDataset pipeline
    pipelines.add(new MetricDatsetPipeline(metricDAO));

    Aggregator aggregator = new LinearAggregator();

    Framework framework = new Framework(pipelines, aggregator);

    // search context
    long windowSize = Long.parseLong(cmd.getOptionValue(CLI_WINDOW_SIZE, "1")) * DAY_IN_MS;
    long baselineOffset = Long.parseLong(cmd.getOptionValue(CLI_BASELINE_OFFSET, "7")) * DAY_IN_MS;

    // search loop
    System.out.println("Enter search context entities' URNs (separated by comma \",\"):");
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    String line = null;
    while((line = br.readLine()) != null) {
      String[] urns = line.split(",");

      long now = System.currentTimeMillis();
      long windowEnd = now;
      long windowStart = now - windowSize;
      long baselineEnd = windowEnd - baselineOffset;
      long baselineStart = windowStart - baselineOffset;

      Set<Entity> entities = new HashSet<>();
      for(String urn : urns) {
        entities.add(new Entity(urn));
      }

      SearchContext context = new SearchContext(entities, windowStart, windowEnd, baselineStart, baselineEnd);

      System.out.println("*** Search context:");
      for(Entity e : context.getEntities()) {
        System.out.println(e.getUrn());
      }
      System.out.println("timestampStart: " + context.getTimestampStart());
      System.out.println("timestampEnd:   " + context.getTimestampEnd());
      System.out.println("baselineStart:  " + context.getBaselineStart());
      System.out.println("baselineEnd:    " + context.getBaselineEnd());

      FrameworkResult result = null;
      try {
        result = framework.run(context);
      } catch (Exception e) {
        System.out.println("*** Exception while running framework:");
        e.printStackTrace();
        continue;
      }

      System.out.println("*** Linear results:");
      for(Entity e : result.getEntities()) {
        System.out.println(e.getUrn());
      }

      System.out.println("*** Grouped results:");
      Map<URNUtils.EntityType, Collection<Entity>> grouped = FrameworkResultUtils.topKPerType(result.getEntities(), 3);
      for(Map.Entry<URNUtils.EntityType, Collection<Entity>> entry : grouped.entrySet()) {
        System.out.println(entry.getKey().getPrefix());
        for(Entity e : entry.getValue()) {
          System.out.println(e.getUrn());
        }
      }

      System.out.println("Enter search context entities' URNs (separated by comma \",\"):");
    }

    System.out.println("done.");
  }
}
