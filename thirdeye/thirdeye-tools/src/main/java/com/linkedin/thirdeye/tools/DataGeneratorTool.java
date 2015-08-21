package com.linkedin.thirdeye.tools;

import java.io.File;
import java.io.FileInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import com.linkedin.thirdeye.api.StarTreeConfig;

public class DataGeneratorTool {

  private static String USAGE = "usage: [opts] starTreeConfigFile schemaFile outputDirectory";
  public static DataGeneratorConfig config ;
  DataGeneratorWriter writer;

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("minTime", true, "Min time to load");
    options.addOption("maxTime", true, "Max time to load");
    options.addOption("numRecords", true, "Number of records to generate");
    options.addOption("numFiles", true, "Number of avro files to generate");
    options.addOption("cardinality", true, "CSV of cardinality for each dimension");
    options.addOption("help", false, "Prints this help message");
    options.addOption("debug", false, "Debug logging");

    CommandLine commandLine = new GnuParser().parse(options, args);
    if (commandLine.getArgs().length != 3 || commandLine.hasOption("help")) {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp(USAGE, options);
      System.exit(1);
    }

    config = new DataGeneratorConfig();

    config.minTime = commandLine.getOptionValue("minTime", null);
    config.maxTime = commandLine.getOptionValue("maxTime", null);
    config.numRecords = Integer.valueOf(commandLine.getOptionValue("numRecords", "0"));
    config.numFiles = Integer.valueOf(commandLine.getOptionValue("numFiles", "0"));
    config.cardinality = commandLine.getOptionValue("cardinality", null);
    config.starTreeConfig = StarTreeConfig.decode(new FileInputStream(commandLine.getArgs()[0]));
    config.schemaFile = new File(commandLine.getArgs()[1]);
    config.outputDataDirectory = new File(commandLine.getArgs()[2]);

    DataGeneratorTool dataGenTool = new DataGeneratorTool();
    dataGenTool.generateData(config);
  }

  public void generateData(DataGeneratorConfig config) throws Exception {
    config.setOptions();
    writer = new DataGeneratorWriter(config);
    writer.init();
    writer.generate();
  }

}