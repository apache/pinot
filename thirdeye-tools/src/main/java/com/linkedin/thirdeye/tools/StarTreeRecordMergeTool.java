package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.api.StarTreeConfig;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StarTreeRecordMergeTool implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeRecordMergeTool.class);

  public enum FileType
  {
    TSV,
    AVRO
  }

  private final StarTreeConfig config;
  private final FileType fileType;
  private final File outputFile;
  private final List<File> inputFiles;

  public StarTreeRecordMergeTool(StarTreeConfig config,
                                 FileType fileType,
                                 File outputFile,
                                 List<File> inputFiles)
  {
    this.config = config;
    this.fileType = fileType;
    this.outputFile = outputFile;
    this.inputFiles = inputFiles;
  }

  @Override
  public void run()
  {
    try
    {
      switch (fileType)
      {
        case AVRO:
          runAvro();
          break;
        default:
          throw new IllegalArgumentException("FileType " + fileType + " not supported");
      }
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  private void runAvro() throws Exception
  {
    Set<Long> timeBuckets = new HashSet<Long>();
    Map<Map<String, String>, Map<String, Integer>> aggregates = new HashMap<Map<String, String>, Map<String, Integer>>();

    Schema schema = null;
    int numRead = 0;

    for (File inputFile : inputFiles)
    {
      LOG.info("Processing {}", inputFile);

      FileReader<GenericRecord> fileReader
              = DataFileReader.openReader(inputFile, new GenericDatumReader<GenericRecord>());

      // Check schema
      if (schema == null)
      {
        schema = fileReader.getSchema();

        // Check dimensions
        for (String dimensionName : config.getDimensionNames())
        {
          Schema.Field field = schema.getField(dimensionName);
          if (field == null)
          {
            throw new IllegalStateException("Schema missing field " + dimensionName + ": " + schema);
          }
        }

        // Check metrics
        for (String metricName : config.getMetricNames())
        {
          Schema.Field field = schema.getField(metricName);
          if (field == null)
          {
            throw new IllegalStateException("Schema missing field " + metricName + ": " + schema);
          }
        }

        // Check time
        Schema.Field field = schema.getField(config.getTime().getColumnName());
        if (field == null)
        {
          throw new IllegalStateException("Schema missing field " + config.getTime().getColumnName() + ": " + schema);
        }

        LOG.info("Using schema {}", schema);
      }
      else if (!schema.equals(fileReader.getSchema()))
      {
        throw new IllegalStateException("Cannot merge records with different schemas: "
                                                + schema + ";" + fileReader.getSchema());
      }

      // Compute aggregates
      GenericRecord record = null;
      while (fileReader.hasNext())
      {
        record = fileReader.next(record);

        // Key == dimension values
        Map<String, String> dimensionValues = new HashMap<String, String>(config.getDimensionNames().size());
        for (String dimensionName : config.getDimensionNames())
        {
          Object dimensionValue = record.get(dimensionName);
          if (dimensionValue == null)
          {
            throw new IllegalStateException("Got null value for " + dimensionName + ": " + record);
          }
          dimensionValues.put(dimensionName, dimensionValue.toString());
        }

        // Value == metric values
        Map<String, Integer> metricValues = aggregates.get(dimensionValues);
        if (metricValues == null)
        {
          metricValues = new HashMap<String, Integer>();
          for (String metricName : config.getMetricNames())
          {
            metricValues.put(metricName, 0);
          }
          aggregates.put(dimensionValues, metricValues);
        }

        // Update metric values
        for (String metricName : config.getMetricNames())
        {
          Object metricValue = record.get(metricName);
          if (metricValue == null)
          {
            metricValue = 0;
          }
          Integer newValue = metricValues.get(metricName) + ((Number) metricValue).intValue();
          metricValues.put(metricName, newValue);
        }

        Object timeValue = record.get(config.getTime().getColumnName());
        if (timeValue != null)
        {
          timeBuckets.add(((Number) timeValue).longValue());
        }

        if (++numRead % 5000 == 0)
        {
          LOG.info("Read {} records ({} dimension combinations) ({} time buckets)",
                   numRead, aggregates.size(), timeBuckets.size());
        }
      }
    }

    if (schema == null)
    {
      throw new IllegalStateException("Did not obtain schema from input file");
    }

    // Open output file
    LOG.info("Writing aggregates to {}", outputFile);
    DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));
    fileWriter.create(schema, outputFile);

    // Convert aggregates to new Avro file
    int numWritten = 0;
    GenericRecord record = new GenericData.Record(schema);
    for (Map.Entry<Map<String, String>, Map<String, Integer>> entry : aggregates.entrySet())
    {
      // Fill in dimensions
      for (Map.Entry<String, String> dimension : entry.getKey().entrySet())
      {
        Schema.Field field = schema.getField(dimension.getKey());

        switch (getType(field.schema()))
        {
          case INT:
            record.put(dimension.getKey(), Integer.valueOf(dimension.getValue()));
            break;
          case LONG:
            record.put(dimension.getKey(), Long.valueOf(dimension.getValue()));
            break;
          case FLOAT:
            record.put(dimension.getKey(), Float.valueOf(dimension.getValue()));
            break;
          case DOUBLE:
            record.put(dimension.getKey(), Double.valueOf(dimension.getValue()));
            break;
          case BOOLEAN:
            record.put(dimension.getKey(), Boolean.valueOf(dimension.getValue()));
            break;
          case STRING:
            record.put(dimension.getKey(), dimension.getValue());
            break;
          default:
            throw new IllegalStateException("Unsupported dimension type " + field.schema());
        }
      }

      // Fill in metrics
      for (Map.Entry<String, Integer> metric : entry.getValue().entrySet())
      {
        Schema.Field field = schema.getField(metric.getKey());

        switch (getType(field.schema()))
        {
          case INT:
            record.put(metric.getKey(), metric.getValue());
            break;
          case LONG:
            record.put(metric.getKey(), metric.getValue().longValue());
            break;
          case FLOAT:
            record.put(metric.getKey(), metric.getValue().floatValue());
            break;
          case DOUBLE:
            record.put(metric.getKey(), metric.getValue().doubleValue());
            break;
          default:
            throw new IllegalStateException("Invalid metric schema type: " + field.schema().getType());
        }
      }

      // All other values fill in 0 (should be time column options)
      for (Schema.Field field : schema.getFields())
      {
        if (!entry.getKey().containsKey(field.name())  // neither dimension,
                && !entry.getValue().containsKey(field.name())) // nor metric
        {
          switch (getType(field.schema()))
          {
            case INT:
              record.put(field.name(), 0);
              break;
            case LONG:
              record.put(field.name(), 0L);
              break;
            case FLOAT:
            case DOUBLE:
              record.put(field.name(), 0.0);
              break;
            default:
              throw new IllegalStateException("Invalid time schema type: " + field.schema().getType());
          }
        }
      }

      // Write to file
      fileWriter.append(record);

      if (++numWritten % 5000 == 0)
      {
        LOG.info("Wrote {} records to output file", numWritten);
      }
    }

    // Close file writer
    fileWriter.close();

    // Find min/max time bucket
    Long minTimeBucket = null;
    Long maxTimeBucket = null;
    for (Long timeBucket : timeBuckets)
    {
      if (minTimeBucket == null || minTimeBucket > timeBucket)
      {
        minTimeBucket = timeBucket;
      }

      if (maxTimeBucket == null || maxTimeBucket < timeBucket)
      {
        maxTimeBucket = timeBucket;
      }
    }

    LOG.info("Read {} records; wrote {} records", numRead, numWritten);
    LOG.info("Processed {} time buckets (min={}, max={})", timeBuckets.size(), minTimeBucket, maxTimeBucket);
  }

  private static Schema.Type getType(Schema schema)
  {
    Schema.Type type = null;
    if (Schema.Type.UNION.equals(schema.getType()))
    {
      List<Schema> schemas = schema.getTypes();
      for (Schema s : schemas)
      {
        if (!Schema.Type.NULL.equals(s.getType()))
        {
          type = s.getType();
        }
      }
    }
    else
    {
      type = schema.getType();
    }

    if (type == null)
    {
      throw new IllegalStateException("Could not unambiguously determine type of schema " + schema);
    }

    return type;
  }

  public static void main(String[] args) throws Exception
  {
    Options options = new Options();
    options.addOption("fileType", true, "Input/output file type (default: avro)");

    CommandLine commandLine = new GnuParser().parse(options, args);

    if (commandLine.getArgs().length < 3)
    {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp("usage: [opts] configFile outputFile inputFile(s) ...", options);
      return;
    }

    StarTreeConfig config = StarTreeConfig.decode(new FileInputStream(commandLine.getArgs()[0]));

    FileType fileType = FileType.valueOf(commandLine.getOptionValue("fileType", "AVRO").toUpperCase());

    File outputFile = new File(commandLine.getArgs()[1]);

    List<File> inputFiles = new ArrayList<File>();
    for (int i = 2; i < commandLine.getArgs().length; i++)
    {
      inputFiles.add(new File(commandLine.getArgs()[i]));
    }

    new StarTreeRecordMergeTool(config, fileType, outputFile, inputFiles).run();
  }
}
