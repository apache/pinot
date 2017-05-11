package com.linkedin.thirdeye.rootcause.impl;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.linkedin.thirdeye.anomaly.ThirdEyeAnomalyConfiguration;
import com.linkedin.thirdeye.anomaly.events.EventDataProviderLoader;
import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.anomaly.events.HistoricalAnomalyEventProvider;
import com.linkedin.thirdeye.anomaly.events.HolidayEventProvider;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.common.ThirdEyeConfiguration;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EntityToEntityMappingManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.RCAFramework;
import com.linkedin.thirdeye.rootcause.RCAFrameworkExecutionResult;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.LoggerFactory;


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
  private static final String CLI_PIPELINE = "pipeline";
  private static final String CLI_TIME_START = "time-start";
  private static final String CLI_TIME_END = "time-end";
  private static final String CLI_INTERACTIVE = "interactive";

  private static final String P_INPUT = RCAFramework.INPUT;
  private static final String P_EVENT_HOLIDAY = "eventHoliday";
  private static final String P_EVENT_ANOMALY = "eventAnomaly";
  private static final String P_EVENT_TOPK = "eventTopK";
  private static final String P_METRIC_DATASET_RAW = "metricDatasetRaw";
  private static final String P_METRIC_METRIC_RAW = "metricMetricRaw";
  private static final String P_METRIC_TOPK = "metricTopK";
  private static final String P_DIMENSION_METRIC_RAW = "dimensionMetricRaw";
  private static final String P_DIMENSION_REWRITE = "dimensionRewrite";
  private static final String P_DIMENSION_TOPK = "dimensionTopK";
  private static final String P_SERVICE_METRIC_RAW = "serviceMetricRaw";
  private static final String P_SERVICE_TOPK = "serviceTopK";
  private static final String P_OUTPUT = RCAFramework.OUTPUT;

  private static final DateTimeFormatter ISO8601 = ISODateTimeFormat.basicDateTimeNoMillis();

  private static final int TOPK_EVENT = 10;
  private static final int TOPK_METRIC = 5;
  private static final int TOPK_DIMENSION = 10;
  private static final int TOPK_SERVICE = 5;

  private static final long DAY_IN_MS = 24 * 3600 * 1000;

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    Option optConfig = new Option("c", CLI_CONFIG_DIR, true, "ThirdEye configuration file");
    optConfig.setRequired(true);
    options.addOption(optConfig);

    options.addOption(null, CLI_WINDOW_SIZE, true, "window size for search window (in days, defaults to '7')");
    options.addOption(null, CLI_BASELINE_OFFSET, true, "baseline offset (in days, from start of window)");
    options.addOption(null, CLI_ENTITIES, true, "search context metric entities");
    options.addOption(null, CLI_PIPELINE, true, "pipeline config YAML file (not specifying this will launch default pipeline)");
    options.addOption(null, CLI_TIME_START, true, "start time of the search window (ISO 8601, e.g. '20170701T150000Z')");
    options.addOption(null, CLI_TIME_END, true, "end time of the search window (ISO 8601, e.g. '20170831T030000Z', defaults to now)");
    options.addOption(null, CLI_INTERACTIVE, false, "enters interacive REPL mode (specified entities will be added automatically)");

    Parser parser = new BasicParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(RCAFrameworkRunner.class.getSimpleName(), options);
      System.exit(1);
    }

    if(cmd.hasOption(CLI_WINDOW_SIZE) && cmd.hasOption(CLI_TIME_START)) {
      System.out.println(String.format("--%s and --%s mutually exclusive", CLI_WINDOW_SIZE, CLI_TIME_START));
      System.exit(1);
    }

    // runtime logger config
    ((Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.WARN);
    ((Logger)LoggerFactory.getLogger("com.linkedin.thirdeye.rootcause")).setLevel(Level.DEBUG);

    // config
    File config = new File(cmd.getOptionValue(CLI_CONFIG_DIR));

    File daoConfig = new File(config.getAbsolutePath() + "/persistence.yml");
    DaoProviderUtil.init(daoConfig);

    ThirdEyeConfiguration thirdEyeConfig = new ThirdEyeAnomalyConfiguration();
    thirdEyeConfig.setRootDir(config.getAbsolutePath());
    ThirdEyeCacheRegistry.initializeCaches(thirdEyeConfig);

    EventDataProviderManager eventProvider = EventDataProviderManager.getInstance();
    eventProvider.registerEventDataProvider(EventType.HOLIDAY.toString(), new HolidayEventProvider());
    eventProvider.registerEventDataProvider(EventType.HISTORICAL_ANOMALY.toString(), new HistoricalAnomalyEventProvider());

    // ************************************************************************
    // Framework setup
    // ************************************************************************
    RCAFramework framework;
    if(cmd.hasOption(CLI_PIPELINE)) {
      File rcaConfig = new File(cmd.getOptionValue(CLI_PIPELINE));
      EventDataProviderLoader.registerEventDataProvidersFromConfig(rcaConfig, eventProvider);
      List<Pipeline> pipelines = PipelineLoader.getPipelinesFromConfig(rcaConfig);

      // Executor
      ExecutorService executor = Executors.newSingleThreadExecutor();

      framework = new RCAFramework(pipelines, executor);

    } else {
      framework = makeStaticFramework(eventProvider);
    }

    // ************************************************************************
    // Entities
    // ************************************************************************
    Set<Entity> entities = new HashSet<>();

    // time range and baseline
    long now = System.currentTimeMillis();
    long windowEnd = now;
    if(cmd.hasOption(CLI_TIME_END))
      windowEnd = ISO8601.parseDateTime(cmd.getOptionValue(CLI_TIME_END)).getMillis();

    long windowSize = 7 * DAY_IN_MS;
    if(cmd.hasOption(CLI_TIME_START))
      windowSize = windowEnd - ISO8601.parseDateTime(cmd.getOptionValue(CLI_TIME_START)).getMillis();
    else if(cmd.hasOption(CLI_WINDOW_SIZE))
      windowSize = Long.parseLong(cmd.getOptionValue(CLI_WINDOW_SIZE)) * DAY_IN_MS;

    long baselineOffset = 0;
    if(cmd.hasOption(CLI_BASELINE_OFFSET))
      baselineOffset = Long.parseLong(cmd.getOptionValue(CLI_BASELINE_OFFSET)) * DAY_IN_MS;

    long windowStart = windowEnd - windowSize;

    long baselineEnd = windowStart - baselineOffset;
    long baselineStart = baselineEnd - windowSize;

    System.out.println(String.format("Using current time range '%d' (%s) to '%d' (%s)", windowStart, ISO8601.print(windowStart), windowEnd, ISO8601.print(windowEnd)));
    System.out.println(String.format("Using baseline time range '%d' (%s) to '%d' (%s)", baselineStart, ISO8601.print(baselineStart), baselineEnd, ISO8601.print(baselineEnd)));

    entities.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_CURRENT, windowStart, windowEnd));
    entities.add(TimeRangeEntity.fromRange(1.0, TimeRangeEntity.TYPE_BASELINE, baselineStart, baselineEnd));

    if(cmd.hasOption(CLI_ENTITIES)) {
      entities.addAll(parseURNSequence(cmd.getOptionValue(CLI_ENTITIES), 1.0));
    }

    // ************************************************************************
    // Framework execution
    // ************************************************************************
    if (cmd.hasOption(CLI_INTERACTIVE)) {
      try {
        readExecutePrintLoop(framework, entities);
      } catch (InterruptedIOException ignore) {
        // left blank, exit
      }

    } else {
      runFramework(framework, entities);
    }

    System.out.println("done.");

    // Pinot connection workaround
    System.exit(0);
  }

  private static RCAFramework makeStaticFramework(EventDataProviderManager eventProvider) {
    Set<Pipeline> pipelines = new HashSet<>();

    MetricConfigManager metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    DatasetConfigManager datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    EntityToEntityMappingManager entityDAO = DAORegistry.getInstance().getEntityToEntityMappingDAO();

    // Metrics
    pipelines.add(new MetricDatasetPipeline(P_METRIC_DATASET_RAW, asSet(P_INPUT), metricDAO, datasetDAO));
    pipelines.add(new EntityMappingPipeline(P_METRIC_METRIC_RAW, asSet(P_INPUT), entityDAO, "METRIC_TO_METRIC", false, false));
    pipelines.add(new TopKPipeline(P_METRIC_TOPK, asSet(P_INPUT, P_METRIC_DATASET_RAW, P_METRIC_METRIC_RAW), MetricEntity.class, TOPK_METRIC));

    // Dimensions (from metrics)
    QueryCache cache = ThirdEyeCacheRegistry.getInstance().getQueryCache();
    ExecutorService executorScorer = Executors.newFixedThreadPool(3);
    pipelines.add(new DimensionAnalysisPipeline(P_DIMENSION_METRIC_RAW, asSet(P_INPUT, P_METRIC_TOPK), metricDAO, datasetDAO, cache, executorScorer));
    pipelines.add(new EntityMappingPipeline(P_DIMENSION_REWRITE, asSet(P_DIMENSION_METRIC_RAW), entityDAO, "DIMENSION_TO_DIMENSION", true, true));
    pipelines.add(new TopKPipeline(P_DIMENSION_TOPK, asSet(P_INPUT, P_DIMENSION_REWRITE), DimensionEntity.class, TOPK_DIMENSION));

    // Systems
    pipelines.add(new EntityMappingPipeline(P_SERVICE_METRIC_RAW, asSet(P_METRIC_TOPK), entityDAO, "METRIC_TO_SERVICE", false, false));
    pipelines.add(new TopKPipeline(P_SERVICE_TOPK, asSet(P_INPUT, P_SERVICE_METRIC_RAW), ServiceEntity.class, TOPK_SERVICE));

    // Events (from metrics and dimensions)
    pipelines.add(new AnomalyEventsPipeline(P_EVENT_ANOMALY, asSet(P_INPUT, P_METRIC_TOPK), eventProvider, metricDAO));
    pipelines.add(new HolidayEventsPipeline(P_EVENT_HOLIDAY, asSet(P_INPUT, P_DIMENSION_TOPK), eventProvider));
    pipelines.add(new TopKPipeline(P_EVENT_TOPK, asSet(P_INPUT, P_EVENT_ANOMALY, P_EVENT_HOLIDAY), EventEntity.class, TOPK_EVENT));

    // Aggregation
    pipelines.add(new LinearAggregationPipeline(P_OUTPUT, asSet(P_EVENT_TOPK, P_METRIC_TOPK, P_DIMENSION_TOPK, P_SERVICE_TOPK), -1));

    // Executor
    ExecutorService executor = Executors.newSingleThreadExecutor();

    // Framework
    return new RCAFramework(pipelines, executor);
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
    System.out.println("*** Search context:");
    for(Entity e : entities) {
      System.out.println(formatEntity(e));
    }

    RCAFrameworkExecutionResult result = null;
    try {
      result = framework.run(entities);
    } catch (Exception e) {
      System.out.println("*** Exception while running framework:");
      e.printStackTrace();
      return;
    }

    System.out.println("*** Linear results:");
    List<Entity> results = new ArrayList<>(result.getResults());
    Collections.sort(results, Entity.HIGHEST_SCORE_FIRST);

    for(Entity e : results) {
      System.out.println(formatEntity(e));
    }

    System.out.println("*** Grouped results:");
    Map<String, Collection<Entity>> grouped = topKPerType(results, 3);
    for(Map.Entry<String, Collection<Entity>> entry : grouped.entrySet()) {
      System.out.println(entry.getKey());
      for(Entity e : entry.getValue()) {
        System.out.println(formatEntity(e));
      }
    }
  }

  private static Collection<Entity> parseURNSequence(String urns, double score) {
    if(urns.isEmpty())
      return Collections.emptySet();
    Set<Entity> entities = new HashSet<>();
    String[] parts = urns.split(",");
    for(String part : parts) {
      entities.add(EntityUtils.parseURN(part, score));
    }
    return entities;
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
      String prefix = extractPrefix(e);

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

  static Set<String> asSet(String... s) {
    return new HashSet<>(Arrays.asList(s));
  }

  static String extractPrefix(Entity e) {
    String[] parts = e.getUrn().split(":", 3);
    return parts[0] + ":" + parts[1] + ":";
  }
}
