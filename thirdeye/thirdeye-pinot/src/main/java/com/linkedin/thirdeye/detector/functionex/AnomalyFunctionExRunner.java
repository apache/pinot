package com.linkedin.thirdeye.detector.functionex;

import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.bao.jdbc.DatasetConfigManagerImpl;
import com.linkedin.thirdeye.datalayer.bao.jdbc.EventManagerImpl;
import com.linkedin.thirdeye.datalayer.util.DaoProviderUtil;
import com.linkedin.thirdeye.detector.functionex.impl.ThirdEyeEventDataSource;
import com.linkedin.thirdeye.detector.functionex.impl.ThirdEyeMetricDataSource;
import com.linkedin.thirdeye.detector.functionex.impl.ThirdEyeMockDataSource;
import com.linkedin.thirdeye.detector.functionex.impl.ThirdEyePinotConnection;
import com.linkedin.thirdeye.detector.functionex.impl.ThirdEyePinotDataSource;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.BasicParser;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Harness for manual testing of AnomalyFunctionEx implementations
 * without standing up all of ThirdEye
 */
public class AnomalyFunctionExRunner {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyFunctionExRunner.class);

  private static final String ID_CLASSNAME = "classname";
  private static final String ID_MONITOR_FROM = "monitor-from";
  private static final String ID_MONITOR_TO = "monitor-to";
  private static final String ID_CONFIG = "config";
  private static final String ID_PINOT = "enable-pinot";
  private static final String ID_MOCK = "enable-mock";
  private static final String ID_THIRDEYE = "enable-thirdeye";
  private static final String ID_EVENT = "enable-event";
  private static final String ID_METRIC = "enable-metric";

  public static void main(String[] args) throws Exception {
    Options options = makeParserOptions();
    CommandLineParser parser = new BasicParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp(AnomalyFunctionExRunner.class.getSimpleName(), options);
      System.exit(1);
      return;
    }

    AnomalyFunctionExFactory factory = new AnomalyFunctionExFactory();

    String classname = cmd.getOptionValue(ID_CLASSNAME);
    LOG.info("Using anomaly function '{}'", classname);

    long monitoringEnd = Long.parseLong(cmd.getOptionValue(ID_MONITOR_TO, String.valueOf(DateTime.now(DateTimeZone.UTC).getMillis() / 1000)));
    long monitoringStart = Long.parseLong(cmd.getOptionValue(ID_MONITOR_FROM, String.valueOf(monitoringEnd - 3600)));
    LOG.info("Setting monitoring window from '{}' to '{}'", monitoringStart, monitoringEnd);

    Map<String, String> config = new HashMap<>();
    if(cmd.hasOption(ID_CONFIG)) {
      String[] params = cmd.getOptionValue(ID_CONFIG).split("(?<!\\\\),");
      for(String setting : params) {
        String[] items = setting.split("=", 2);
        config.put(items[0], items[1].replace("\\,", ","));
      }
    }
    LOG.info("Using configuration '{}'", config);

    if(cmd.hasOption(ID_THIRDEYE)) {
      LOG.info("Enabling ThirdEye internal database with config '{}'", cmd.getOptionValue(ID_THIRDEYE));
      File configFile = new File(cmd.getOptionValue(ID_THIRDEYE));
      DaoProviderUtil.init(configFile);
    }

    ThirdEyePinotDataSource pinotDataSource = null;
    if(cmd.hasOption(ID_PINOT)) {
      LOG.info("Enabling 'pinot' datasource with config '{}'", cmd.getOptionValue(ID_PINOT));
      File configFile = new File(cmd.getOptionValue(ID_PINOT));
      PinotThirdEyeClientConfig clientConfig = PinotThirdEyeClientConfig.fromFile(configFile);
      ThirdEyePinotConnection conn = new ThirdEyePinotConnection(clientConfig, 1);
      pinotDataSource = new ThirdEyePinotDataSource(conn);
      factory.addDataSource("pinot", pinotDataSource);
    }

    if(cmd.hasOption(ID_MOCK)) {
      LOG.info("Enabling 'mock' datasource");
      factory.addDataSource("mock", new ThirdEyeMockDataSource());
    }

    if(cmd.hasOption(ID_EVENT)) {
      if(!cmd.hasOption(ID_THIRDEYE)) {
        LOG.error("--{} requires --{}", ID_EVENT, ID_THIRDEYE);
        System.exit(1);
      }

      LOG.info("Enabling 'event' datasource");
      EventManager manager = DaoProviderUtil.getInstance(EventManagerImpl.class);
      factory.addDataSource("event", new ThirdEyeEventDataSource(manager));
    }

    if(cmd.hasOption(ID_METRIC)) {
      if(!cmd.hasOption(ID_THIRDEYE)) {
        LOG.error("--{} requires --{}", ID_METRIC, ID_THIRDEYE);
        System.exit(1);
      }

      if(!cmd.hasOption(ID_PINOT)) {
        LOG.error("--{} requires --{}", ID_METRIC, ID_PINOT);
        System.exit(1);
      }

      LOG.info("Enabling 'metric' datasource");
      DatasetConfigManager manager = DaoProviderUtil.getInstance(DatasetConfigManagerImpl.class);
      factory.addDataSource("metric", new ThirdEyeMetricDataSource(pinotDataSource, manager));
    }

    AnomalyFunctionExContext context = new AnomalyFunctionExContext();
    context.setClassName(classname);
    context.setMonitoringWindowStart(monitoringStart);
    context.setMonitoringWindowEnd(monitoringEnd);
    context.setConfig(config);

    LOG.info("Instantiating ...");
    AnomalyFunctionEx function = null;
    try {
      function = factory.fromContext(context);
    } catch (Exception e) {
      LOG.error("Error instantiating anomaly function", e);
      System.exit(1);
    }

    LOG.info("Applying ...");
    AnomalyFunctionExResult result = null;
    try {
      result = function.apply();
    } catch (Exception e) {
      LOG.error("Error applying anomaly function", e);
      System.exit(1);
    }

    LOG.info("Got function result with {} anomalies", result.getAnomalies().size());
    for(AnomalyFunctionExResult.Anomaly a : result.getAnomalies()) {
      String data = "(no data)";
      if(a.getData().getIndex().size() > 0)
        data = a.getData().toString();
      LOG.info("Anomaly at '{}-{}': '{}' {}", a.getStart(), a.getEnd(), a.getMessage(), data);
    }

    LOG.info("Done.");

    if(cmd.hasOption(ID_PINOT)) {
      LOG.info("Forcing termination (Pinot connection workaround)");
      System.exit(0);
    }
  }

  private static Options makeParserOptions() {
    Options options = new Options();

    Option classname = new Option("n", ID_CLASSNAME, true,
        "Fully qualified classname of anomaly function implementation.");
    classname.setRequired(true);
    options.addOption(classname);

    Option config = new Option("c", ID_CONFIG, true,
        "Configuration parameters as comma separated list (example: 'key1=value1,key2=value\\\\,2,...')");
    options.addOption(config);

    Option monitoringFrom = new Option("f", ID_MONITOR_FROM, true,
        "Monitoring window start timestamp in seconds. (Default: monitoring end timestamp - 1 hour)");
    options.addOption(monitoringFrom);

    Option monitoringTo = new Option("t", ID_MONITOR_TO, true,
        "Monitoring window end timestamp in seconds. (Default: now)");
    options.addOption(monitoringTo);

    Option pinot = new Option("P", ID_PINOT, true,
        "Enables 'pinot' data source. Requires path to pinot client config YAML file.");
    options.addOption(pinot);

    Option mock = new Option("O", ID_MOCK, false,
        "Enables 'mock' data source.");
    options.addOption(mock);

    Option thirdeye = new Option("T", ID_THIRDEYE, true,
        "Enables access to the ThirdEye internal database. Requires path to thirdeye persistence config YAML file.");
    options.addOption(thirdeye);

    Option event = new Option("E", ID_EVENT, false,
        "Enables 'event' data source. (Requires: " + ID_THIRDEYE + ")");
    options.addOption(event);

    Option metric = new Option("M", ID_METRIC, false,
        "Enables 'metric' data source. (Requires: " + ID_THIRDEYE + " " + ID_PINOT + ")");
    options.addOption(metric);

    return options;
  }
}
