package com.linkedin.thirdeye.client.diffsummary;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.dashboard.resources.SummaryResource;
import com.linkedin.thirdeye.dashboard.views.diffsummary.SummaryResponse;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiDimensionalSummary {
  private static final Logger LOG = LoggerFactory.getLogger(PinotThirdEyeSummaryClient.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static Options buildOptions() {
    Options options = new Options();

    Option dataset = Option.builder("dataset").desc("dataset name").hasArg().argName("NAME").required().build();
    options.addOption(dataset);

    Option metricName = Option.builder("metric").desc("metric name").hasArg().argName("NAME").required().build();
    options.addOption(metricName);

    Option dimensions =
        Option.builder("dim").longOpt("dimensions").desc("dimension names that are separated by comma").hasArg()
            .argName("LIST").build();
    options.addOption(dimensions);

    Option filters =
        Option.builder("filters").desc("filter to apply on the data cube").hasArg().argName("JSON").build();
    options.addOption(filters);

    Option currentStart =
        Option.builder("cstart").longOpt("currentStart").desc("current start time inclusive").hasArg().argName("MILLIS")
            .required().build();
    options.addOption(currentStart);

    Option currentEnd =
        Option.builder("cend").longOpt("currentEnd").desc("current end time exclusive").hasArg().argName("MILLIS")
            .required().build();
    options.addOption(currentEnd);

    Option baselineStart =
        Option.builder("bstart").longOpt("baselineStart").desc("baseline start time inclusive").hasArg()
            .argName("MILLIS").required().build();
    options.addOption(baselineStart);

    Option baselineEnd =
        Option.builder("bend").longOpt("baselineEnd").desc("baseline end time exclusive").hasArg().argName("MILLIS")
            .required().build();
    options.addOption(baselineEnd);

    Option size =
        Option.builder("size").longOpt("summarySize").desc("size of summary").hasArg().argName("NUMBER").build();
    options.addOption(size);

    Option topDimension =
        Option.builder("top").longOpt("topDimensions").desc("number of top dimensions").hasArg().argName("NUMBER")
            .build();
    options.addOption(topDimension);

    Option hierarchies =
        Option.builder("h").longOpt("hierarchies").desc("dimension hierarchies").hasArg().argName("JSON").build();
    options.addOption(hierarchies);

    Option oneSideError = Option.builder("oneSideError").desc("enable one side error summary").build();
    options.addOption(oneSideError);

    Option manualOrder = Option.builder("manualOrder").desc("use manual dimension order").build();
    options.addOption(manualOrder);

    Option dateTimeZone =
        Option.builder("timeZone").desc("time zone id in Joda library").hasArg().argName("ID").build();
    options.addOption(dateTimeZone);

    return options;
  }

  public static void main(String[] args) throws Exception {
    Options options = buildOptions();
    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(MultiDimensionalSummary.class.getSimpleName(), options);
    } else {
      CommandLineParser parser = new DefaultParser();
      CommandLine commandLine = parser.parse(options, args);
      List<String> argList = commandLine.getArgList();
      Preconditions.checkArgument(argList.size() > 0, "Please provide config directory as parameter");

      // Get parameters from command line arguments
      String dataset = commandLine.getOptionValue("dataset");
      String metricName = commandLine.getOptionValue("metric");
      String dimensions = commandLine.getOptionValue("dimensions", "");
      String filterJson = commandLine.getOptionValue("filters", "{}");
      long currentStart = Long.parseLong(commandLine.getOptionValue("currentStart"));
      long currentEnd = Long.parseLong(commandLine.getOptionValue("currentEnd"));
      long baselineStart = Long.parseLong(commandLine.getOptionValue("baselineStart"));
      long baselineEnd = Long.parseLong(commandLine.getOptionValue("baselineEnd"));

      int summarySize = Integer.parseInt(commandLine.getOptionValue("size", "10"));
      int topDimensions = Integer.parseInt(commandLine.getOptionValue("topDimensions", "3"));
      String hierarchiesJson = commandLine.getOptionValue("hierarchies", "[]");
      boolean oneSideError = commandLine.hasOption("oneSideError");
      boolean manualOrder = commandLine.hasOption("manual");
      String dataTimeZoneId = commandLine.getOptionValue("timeZone", DateTimeZone.UTC.getID());

      // Initialize ThirdEye's environment
      ThirdEyeUtils.initLightWeightThirdEyeEnvironment(argList.get(0));

      // Trigger summary algorithm
      SummaryResource summaryResource = new SummaryResource();
      String summaryResultJsonString;
      if (manualOrder) {
        summaryResultJsonString = summaryResource
            .buildSummaryManualDimensionOrder(dataset, metricName, currentStart, currentEnd, baselineStart, baselineEnd,
                dimensions, filterJson, summarySize, oneSideError, dataTimeZoneId);
      } else {
        summaryResultJsonString = summaryResource
            .buildSummary(dataset, metricName, currentStart, currentEnd, baselineStart, baselineEnd, dimensions,
                filterJson, summarySize, topDimensions, hierarchiesJson, oneSideError, dataTimeZoneId);
      }

      // Log summary result
      SummaryResponse summaryResponse = objectMapper.readValue(summaryResultJsonString, SummaryResponse.class);
      LOG.info(summaryResponse.toString());
    }

    // Force closing the connections to data sources.
    System.exit(0);
  }
}
