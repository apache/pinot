package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.events.EventDataProvider;
import com.linkedin.thirdeye.anomaly.events.EventDataProviderConfiguration;
import com.linkedin.thirdeye.anomaly.events.EventDataProviderLoader;
import com.linkedin.thirdeye.anomaly.events.HolidayEventProvider;
import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.anomaly.events.HistoricalAnomalyEventProvider;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.rootcause.Aggregator;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.RCAFramework;
import com.linkedin.thirdeye.rootcause.RCAFrameworkResult;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.SearchContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;


/**
 * Console interface for performing root cause search using a sample pipeline configuration.
 * The user can specify the TimeRange and Baseline entities, as well as arbitrary URNs to
 * populate the search context with. The console interface allows one-off or interactive REPL execution modes.
 *
 * <br/><b>Example:</b> {@code java -cp target/thirdeye-pinot-1.0-SNAPSHOT.jar com.linkedin.thirdeye.rootcause.impl.RCAFrameworkRunner
 * --config-dir local-configs/ --window-size 28 --baseline-offset 28 --entities thirdeye:metric:pageViews,thirdeye:metric:logins}
 *
 */
public class RCAFrameworkRunner {

  private static final String CLI_CONFIG_DIR = "config-dir";
  private static final String CLI_WINDOW_SIZE = "window-size";
  private static final String CLI_BASELINE_OFFSET = "baseline-offset";
  private static final String CLI_ENTITIES = "entities";
  private static final String CLI_DIMENSION_CUTOFF = "dimension-cutoff";

  private static final long DAY_IN_MS = 24 * 3600 * 1000;

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    Option optConfig = new Option("c", CLI_CONFIG_DIR, true, "ThirdEye configuration file");
    optConfig.setRequired(true);
    options.addOption(optConfig);

    options.addOption(null, CLI_WINDOW_SIZE, true, "window size for search window (in days)");
    options.addOption(null, CLI_BASELINE_OFFSET, true, "baseline offset (in days)");
    options.addOption(null, CLI_ENTITIES, true, "search context metric entities (not specifying this will activate interactive REPL mode)");
    options.addOption(null, CLI_DIMENSION_CUTOFF, true,
        String.format("cutoff number for top dimensions from contribution analysis (default = %d)", DimensionPipeline.DEFAULT_CUTOFF));

    Parser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(RCAFrameworkRunner.class.getSimpleName(), options);
      System.exit(1);
    }

    // runtime logger config
    ((Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.WARN);
    ((Logger)LoggerFactory.getLogger("com.linkedin.thirdeye.rootcause")).setLevel(Level.INFO);

    // config
    File config = new File(cmd.getOptionValue(CLI_CONFIG_DIR));

    File daoConfig = new File(config.getAbsolutePath() + "/persistence.yml");
    DaoProviderUtil.init(daoConfig);

    ThirdEyeConfiguration thirdEyeConfig = new ThirdEyeAnomalyConfiguration();
    thirdEyeConfig.setRootDir(config.getAbsolutePath());
    ThirdEyeCacheRegistry.initializeCaches(thirdEyeConfig);


    List<Pipeline> pipelines = new ArrayList<>();

    EventDataProviderManager eventProvider = EventDataProviderManager.getInstance();
    eventProvider.registerEventDataProvider(EventType.HOLIDAY.toString(), new HolidayEventProvider());
    eventProvider.registerEventDataProvider(EventType.HISTORICAL_ANOMALY.toString(), new HistoricalAnomalyEventProvider());
    // External event providers
    File rcaConfig = new File(config.getAbsolutePath() + "/rca.yml");
    if (rcaConfig.exists()) {
      EventDataProviderLoader.registerEventDataProvidersFromConfig(rcaConfig, eventProvider);
    }

    // Holiday pipeline
    pipelines.add(new HolidayEventsPipeline(eventProvider));

    // EventMetric pipeline
    QueryCache cache = ThirdEyeCacheRegistry.getInstance().getQueryCache();
    DimensionScorer scorer = new DimensionScorer(cache);
    pipelines.add(new EventMetricPipeline(eventProvider));

    // Dimension pipeline
    int cutoff = Integer.parseInt(cmd.getOptionValue(CLI_DIMENSION_CUTOFF, String.valueOf(DimensionPipeline.DEFAULT_CUTOFF)));
    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    pipelines.add(new DimensionPipeline(metricDAO, datasetDAO, scorer, cutoff));

    // MetricDataset pipeline
    pipelines.add(new MetricDatasetPipeline(metricDAO, datasetDAO));

    // External pipelines
    if (rcaConfig.exists()) {
      pipelines.addAll(PipelineLoader.getPipelinesFromConfig(rcaConfig));
    }

    Aggregator aggregator = new LinearAggregator();

    RCAFramework framework = new RCAFramework(pipelines, aggregator);

    Set<Entity> entities = new HashSet<>();

    // time range and baseline
    if(cmd.hasOption(CLI_WINDOW_SIZE)) {
      long windowSize = Long.parseLong(cmd.getOptionValue(CLI_WINDOW_SIZE, "1")) * DAY_IN_MS;
      long baselineOffset = Long.parseLong(cmd.getOptionValue(CLI_BASELINE_OFFSET, "7")) * DAY_IN_MS;

      long now = System.currentTimeMillis();
      long windowEnd = now;
      long windowStart = now - windowSize;
      long baselineEnd = windowEnd - baselineOffset;
      long baselineStart = windowStart - baselineOffset;

      entities.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_CURRENT, windowStart, windowEnd));
      entities.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_BASELINE, baselineStart, baselineEnd));
    }

    if (cmd.hasOption(CLI_ENTITIES)) {
      entities.addAll(parseURNSequence(cmd.getOptionValue(CLI_ENTITIES), 1.0));
      runFramework(framework, entities);
    } else {
      readExecutePrintLoop(framework, entities);
    }

    System.out.println("done.");

    // Pinot connection workaround
    System.exit(0);
  }

  private static void readExecutePrintLoop(RCAFramework framework, Collection<Entity> baseEntities)
      throws IOException {
    // search loop
    System.out.println("Enter search context metric entities' URNs (separated by comma \",\"):");
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    String line;
    while((line = br.readLine()) != null) {
      Set<Entity> entities = new HashSet<>();
      entities.addAll(baseEntities);
      entities.addAll(parseURNSequence(line, 1.0));

      runFramework(framework, entities);

      System.out.println("Enter search context metric entities' URNs (separated by comma \",\"):");
    }
  }

  private static void runFramework(RCAFramework framework, Set<Entity> entities) {
    SearchContext context = new SearchContext(entities);

    System.out.println("*** Search context:");
    for(Entity e : context.getEntities()) {
      System.out.println(formatEntity(e));
    }

    RCAFrameworkResult result = null;
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
    Map<String, Collection<Entity>>
        grouped = topKPerType(result.getAggregatedResults(), 3);
    for(Map.Entry<String, Collection<Entity>> entry : grouped.entrySet()) {
      System.out.println(entry.getKey());
      for(Entity e : entry.getValue()) {
        System.out.println(formatEntity(e));
      }
    }
  }

  private static Collection<Entity> parseURNSequence(String urns, double score) {
    Set<Entity> entities = new HashSet<>();
    String[] parts = urns.split(",");
    for(String part : parts) {
      entities.add(parseURN(part, score));
    }
    return entities;
  }

  private static Entity parseURN(String urn, double score) {
    String prefix = EntityType.extractPrefix(urn);
    if(DimensionEntity.TYPE.getPrefix().equals(prefix)) {
      return DimensionEntity.fromURN(urn, score);
    } else if(MetricEntity.TYPE.getPrefix().equals(prefix)) {
      return MetricEntity.fromURN(urn, score);
    } else if(TimeRangeEntity.TYPE.getPrefix().equals(prefix)){
      return TimeRangeEntity.fromURN(urn, score);
    }
    throw new IllegalArgumentException(String.format("Could not parse URN '%s'", urn));
  }

  /**
   * Returns the top K (first K) results per entity type from a collection of entities.
   *
   * @param entities aggregated entities
   * @param k maximum number of entities per entity type
   * @return mapping of entity types to list of entities
   */
  private static Map<String, Collection<Entity>> topKPerType(Collection<Entity> entities, int k) {
    Map<String, Collection<Entity>> map = new HashMap<>();
    for(Entity e : entities) {
      String prefix = EntityType.extractPrefix(e);

      if(!map.containsKey(prefix))
        map.put(prefix, new ArrayList<Entity>());

      Collection<Entity> current = map.get(prefix);
      if(current.size() < k)
        current.add(e);
    }

    return map;
  }

  static String formatEntity(Entity e) {
    return String.format("%.3f [%s] %s", e.getScore(), e.getClass().getSimpleName(), e.getUrn());
  }




}
