package com.linkedin.thirdeye.detector.functionex;

import com.linkedin.thirdeye.client.pinot.PinotThirdEyeClientConfig;
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
  private static final String ID_PINOT = "pinot";
  private static final String ID_MOCK = "mock";

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
    LOG.info("Setting monitoring window '{}-{}'", monitoringStart, monitoringEnd);

    Map<String, String> config = new HashMap<>();
    if(cmd.hasOption(ID_CONFIG)) {
      String[] params = cmd.getOptionValue(ID_CONFIG).split(",");
      for(String setting : params) {
        String[] items = setting.split("=");
        config.put(items[0], items[1]);
      }
    }
    LOG.info("Using configuration '{}'", config);

    if(cmd.hasOption(ID_PINOT)) {
      LOG.info("Enabling '{}' datasource with config '{}'", ID_PINOT, cmd.getOptionValue(ID_PINOT));
      File configFile = new File(cmd.getOptionValue(ID_PINOT));
      PinotThirdEyeClientConfig clientConfig = PinotThirdEyeClientConfig.fromFile(configFile);
      ThirdEyePinotConnection conn = new ThirdEyePinotConnection(clientConfig);
      factory.addDataSource(ID_PINOT, new ThirdEyePinotDataSource(conn));
    }

    if(cmd.hasOption(ID_MOCK)) {
      LOG.info("Enabling '{}' datasource", ID_MOCK, cmd.getOptionValue(ID_MOCK));
      factory.addDataSource(ID_MOCK, new ThirdEyeMockDataSource());
    }

    AnomalyFunctionExContext context = new AnomalyFunctionExContext();
    context.setClassName(classname);
    context.setMonitoringWindowStart(monitoringStart);
    context.setMonitoringWindowEnd(monitoringEnd);
    context.setConfig(config);

    LOG.info("Instantiating ...");
    AnomalyFunctionEx function = factory.fromContext(context);

    LOG.info("Applying ...");
    AnomalyFunctionExResult result = function.apply();

    LOG.info("Got result with {} anomalies", result.getAnomalies().size());
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
        "Configuration parameters as comma separated list (example: key1=value1,key2=value2,...)");
    options.addOption(config);

    Option monitoringFrom = new Option("f", ID_MONITOR_FROM, true,
        "Monitoring window start timestamp in seconds. (Default: monitoring end timestamp - 1 hour)");
    options.addOption(monitoringFrom);

    Option monitoringTo = new Option("t", ID_MONITOR_TO, true,
        "Monitoring window end timestamp in seconds. (Default: now)");
    options.addOption(monitoringTo);

    Option pinot = new Option("P", ID_PINOT, true,
        String.format("Enables '%s' data source. Requires path to pinot client config YAML file.", ID_PINOT));
    options.addOption(pinot);

    Option mock = new Option("M", ID_MOCK, false,
        String.format("Enables '%s' data source.", ID_MOCK));
    options.addOption(mock);

    return options;
  }
}
