package com.linkedin.thirdeye.tools;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryHashMapImpl;
import com.linkedin.thirdeye.impl.storage.DataUpdateManager;
import com.linkedin.thirdeye.realtime.ThirdEyeKafkaConfig;
import com.linkedin.thirdeye.realtime.ThirdEyeKafkaConsumer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class StandAloneKafkaConsumer {
  public static void main(String[] args) throws Exception {
    Options opts = new Options();
    opts.addOption("h", "help", false, "Prints a help message");
    opts.addOption("r", "randomGroupId", false, "Randomly generate a group ID for kafka");

    CommandLine cli = new GnuParser().parse(opts, args);

    if (cli.hasOption("help") || cli.getArgs().length != 3) {
      new HelpFormatter().printHelp("usage: [opts] config.yml kafka.yml rootDir", opts);
      return;
    }

    // Args
    StarTreeConfig config = StarTreeConfig.decode(new FileInputStream(cli.getArgs()[0]));
    ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
    objectMapper.registerModule(new JodaModule());
    final ThirdEyeKafkaConfig kafkaConfig = objectMapper.readValue(new File(cli.getArgs()[1]), ThirdEyeKafkaConfig.class);
    File rootDir = new File(cli.getArgs()[2]);

    if (cli.hasOption("randomGroupId")) {
      kafkaConfig.setGroupId(StandAloneKafkaConsumer.class.getSimpleName() + "_" + UUID.randomUUID());
    }

    // Components
    final ExecutorService consumerExecutors = Executors.newSingleThreadExecutor();
    final ScheduledExecutorService taskScheduler = Executors.newSingleThreadScheduledExecutor();
    DataUpdateManager dataUpdateManager = new DataUpdateManager(rootDir, false);
    MetricRegistry metricRegistry = new MetricRegistry();

    // Tree
    StarTreeConfig inMemoryConfig = new StarTreeConfig(config.getCollection(),
        StarTreeRecordStoreFactoryHashMapImpl.class.getCanonicalName(),
        new Properties(),
        config.getAnomalyDetectionFunctionClass(),
        config.getAnomalyDetectionFunctionConfig(),
        config.getAnomalyHandlerClass(),
        config.getAnomalyHandlerConfig(),
        config.getAnomalyDetectionMode(),
        config.getDimensions(),
        config.getMetrics(),
        config.getTime(),
        config.getJoinSpec(),
        config.getRollup(),
        config.getTopKRollup(),
        config.getSplit(),
        false);
    final StarTree mutableTree = new StarTreeImpl(inMemoryConfig);
    mutableTree.open();

    // Report stats to console
    ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();

    // Consumer
    final ThirdEyeKafkaConsumer kafkaConsumer = new ThirdEyeKafkaConsumer(
        mutableTree,
        kafkaConfig,
        consumerExecutors,
        taskScheduler,
        dataUpdateManager,
        metricRegistry);

    // Shutdown
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          kafkaConsumer.stop();
          taskScheduler.shutdown();
          consumerExecutors.shutdown();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    });

    // Start
    kafkaConsumer.start();
    reporter.start(10, TimeUnit.SECONDS);
  }
}
