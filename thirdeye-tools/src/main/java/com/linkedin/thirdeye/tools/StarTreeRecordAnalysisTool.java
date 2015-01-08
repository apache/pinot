package com.linkedin.thirdeye.tools;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.NumberUtils;
import com.linkedin.thirdeye.impl.StarTreeRecordStreamAvroFileImpl;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StarTreeRecordAnalysisTool implements Runnable
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final TypeReference TYPE_REFERENCE = new TypeReference<Map<String, String>>(){};

  private final PrintWriter printWriter;
  private final StarTreeConfig config;
  private final Map<String, String> filter;
  private final Long time;
  private final List<Iterable<StarTreeRecord>> streams;
  private final boolean debug;

  public StarTreeRecordAnalysisTool(PrintWriter printWriter,
                                    StarTreeConfig config,
                                    Map<String, String> filter,
                                    Long time,
                                    List<Iterable<StarTreeRecord>> streams,
                                    boolean debug)
  {
    this.printWriter = printWriter;
    this.config = config;
    this.filter = filter;
    this.time = time;
    this.streams = streams;
    this.debug = debug;
  }

  @Override
  public void run()
  {
    Map<String, Number> aggregates = new HashMap<String, Number>();
    for (String metricName : config.getMetricNames())
    {
      aggregates.put(metricName, 0);
    }

    for (Iterable<StarTreeRecord> stream : streams)
    {
      for (StarTreeRecord record : stream)
      {
        if (matches(record))
        {
          updateAggregates(record, aggregates);

          if (debug)
          {
            printWriter.println(record);
          }
        }
      }
    }

    for (String metricName : config.getMetricNames())
    {
      printWriter.print(metricName);
      printWriter.print("=");
      printWriter.print(aggregates.get(metricName));
      printWriter.println();
    }
  }

  private boolean matches(StarTreeRecord record)
  {
    for (Map.Entry<String, String> entry : filter.entrySet())
    {
      String value = record.getDimensionValues().get(entry.getKey());

      if (!entry.getValue().equals(StarTreeConstants.STAR)
              && !entry.getValue().equals(value))
      {
        return false;
      }

      if (time != null && !time.equals(record.getTime()))
      {
        return false;
      }
    }
    return true;
  }

  private void updateAggregates(StarTreeRecord record, Map<String, Number> aggregates)
  {
    for(int i=0;i<config.getMetricNames().size();i++)
    {
      String metricName = config.getMetricNames().get(i);
      Number oldValue = aggregates.get(metricName);
      Number newValue = NumberUtils.sum(oldValue, record.getMetricValues().get(metricName), config.getMetricTypes().get(i));
      aggregates.put(metricName, newValue);
    }
  }

  public static void main(String[] args) throws Exception
  {
    Options options = new Options();
    options.addOption("filter", true, "JSON map of dimension name -> value");
    options.addOption("time", true, "Time value");
    options.addOption("debug", false, "Print out each matching star tree record");

    CommandLine commandLine = new GnuParser().parse(options, args);

    if (commandLine.getArgs().length < 2)
    {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp("usage: [opts] configFile inputFile(s) ...", options);
      return;
    }

    StarTreeConfig config = StarTreeConfig.decode(new FileInputStream(commandLine.getArgs()[0]));

    Map<String, String> filter
            = OBJECT_MAPPER.readValue(commandLine.getOptionValue("filter", "{}"), TYPE_REFERENCE);

    Long time = null;
    if (commandLine.hasOption("time"))
    {
      time = Long.valueOf(commandLine.getOptionValue("time"));
    }

    List<Iterable<StarTreeRecord>> streams = new ArrayList<Iterable<StarTreeRecord>>();
    for (int i = 1; i < commandLine.getArgs().length; i++)
    {
      streams.add(new StarTreeRecordStreamAvroFileImpl(
              new File(commandLine.getArgs()[i]),
              config.getDimensionNames(),
              config.getMetricNames(),
              config.getMetricTypes(),
              config.getTime().getColumnName()));
    }

    boolean debug = commandLine.hasOption("debug");

    PrintWriter printWriter = new PrintWriter(System.out);
    new StarTreeRecordAnalysisTool(printWriter, config, filter, time, streams, debug).run();
    printWriter.flush();
  }
}
