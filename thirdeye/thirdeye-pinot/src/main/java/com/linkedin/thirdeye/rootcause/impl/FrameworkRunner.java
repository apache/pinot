package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
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
import java.io.IOException;
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


/**
 * Console interface for performing root cause search using a sample pipeline configuration.
 * The user can specify the TimeRange and Baseline entities, as well as arbitrary URNs to
 * populate the search context with. The console interface allows one-off or interactive REPL execution modes.
 * <br/><b>Example:</b> {@code java -cp target/thirdeye-pinot-1.0-SNAPSHOT.jar com.linkedin.thirdeye.rootcause.impl.FrameworkRunner
 * --config-dir local-configs/ --window-size 28 --baseline-offset 28 --entities thirdeye:metric:pageViews,thirdeye:metric:logins}
 *
 */
public class FrameworkRunner {
  private static final String CLI_CONFIG_DIR = "config-dir";
  private static final String CLI_WINDOW_SIZE = "window-size";
  private static final String CLI_BASELINE_OFFSET = "baseline-offset";
  private static final String CLI_ENTITIES = "entities";

  private static final long DAY_IN_MS = 24 * 3600 * 1000;

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    Option optConfig = new Option("c", CLI_CONFIG_DIR, true, "ThirdEye configuration file");
    optConfig.setRequired(true);
    options.addOption(optConfig);

    options.addOption(null, CLI_WINDOW_SIZE, true, "window size for search window (in days)");
    options.addOption(null, CLI_BASELINE_OFFSET, true, "baseline offset (in days)");
    options.addOption(null, CLI_ENTITIES, true, "search context entities (not specifying this will activate interactive REPL mode)");

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
    pipelines.add(new MetricDatsetPipeline(metricDAO, datasetDAO));

    Aggregator aggregator = new LinearAggregator();

    Framework framework = new Framework(pipelines, aggregator);

    // time range and baseline
    long windowSize = Long.parseLong(cmd.getOptionValue(CLI_WINDOW_SIZE, "1")) * DAY_IN_MS;
    long baselineOffset = Long.parseLong(cmd.getOptionValue(CLI_BASELINE_OFFSET, "7")) * DAY_IN_MS;

    long now = System.currentTimeMillis();
    long windowEnd = now;
    long windowStart = now - windowSize;
    long baselineEnd = windowEnd - baselineOffset;
    long baselineStart = windowStart - baselineOffset;

    TimeRangeEntity timeRange = TimeRangeEntity.fromRange(windowStart, windowEnd);
    BaselineEntity baseline = BaselineEntity.fromRange(1.0, baselineStart, baselineEnd);

    if(cmd.hasOption(CLI_ENTITIES)) {
      // one-off execution
      String[] urns = cmd.getOptionValue(CLI_ENTITIES).split(",");

      Set<Entity> entities = new HashSet<>();
      for (String urn : urns) {
        entities.add(new Entity(urn, 1.0));
      }
      entities.add(timeRange);
      entities.add(baseline);

      runFramework(framework, entities);

    } else {
      // interactive REPL
      readExecutePrintLoop(framework, timeRange, baseline);
    }

    System.out.println("done.");

    // Pinot connection workaround
    System.exit(0);
  }

  private static void readExecutePrintLoop(Framework framework, TimeRangeEntity timeRange, BaselineEntity baseline)
      throws IOException {
    // search loop
    System.out.println("Enter search context entities' URNs (separated by comma \",\"):");
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    String line = null;
    while((line = br.readLine()) != null) {
      String[] urns = line.split(",");

      Set<Entity> entities = new HashSet<>();
      for(String urn : urns) {
        entities.add(new Entity(urn, 1.0));
      }
      entities.add(timeRange);
      entities.add(baseline);

      runFramework(framework, entities);

      System.out.println("Enter search context entities' URNs (separated by comma \",\"):");
    }
  }

  private static void runFramework(Framework framework, Set<Entity> entities) {
    SearchContext context = new SearchContext(entities);

    System.out.println("*** Search context:");
    for(Entity e : context.getEntities()) {
      System.out.println(formatEntity(e));
    }

    FrameworkResult result = null;
    try {
      result = framework.run(context);
    } catch (Exception e) {
      System.out.println("*** Exception while running framework:");
      e.printStackTrace();
      return;
    }

    System.out.println("*** Linear results:");
    for(Entity e : result.getAggregatedResults()) {
      System.out.println(formatEntity(e));
    }

    System.out.println("*** Grouped results:");
    Map<EntityUtils.EntityType, Collection<Entity>>
        grouped = FrameworkResultUtils.topKPerType(result.getAggregatedResults(), 3);
    for(Map.Entry<EntityUtils.EntityType, Collection<Entity>> entry : grouped.entrySet()) {
      System.out.println(entry.getKey().getPrefix());
      for(Entity e : entry.getValue()) {
        System.out.println(formatEntity(e));
      }
    }
  }

  static String formatEntity(Entity e) {
    return String.format("%.3f [%s] %s", e.getScore(), e.getClass().getSimpleName(), e.getUrn());
  }
}
