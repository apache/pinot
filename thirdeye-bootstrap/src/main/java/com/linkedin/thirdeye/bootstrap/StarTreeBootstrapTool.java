package com.linkedin.thirdeye.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreCircularBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryCircularBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStreamAvroFileImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStreamTextStreamImpl;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Takes as input raw star tree record streams, and constructs a tree + fixed leaf buffers.
 */
public class StarTreeBootstrapTool implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeBootstrapTool.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String TREE_FILE = "tree.bin";
  private static final String CONFIG_FILE = "config.json";
  private static final String DATA_DIR = "data";

  private final boolean keepMetricValues;
  private final boolean keepBuffers;
  private final StarTreeConfig starTreeConfig;
  private final Collection<Iterable<StarTreeRecord>> recordStreams;
  private final File outputDir;
  private final File dataDir;
  private final ExecutorService executorService;
  private final int numTimeBuckets;

  public StarTreeBootstrapTool(int numTimeBuckets,
                               boolean keepMetricValues,
                               boolean keepBuffers,
                               StarTreeConfig starTreeConfig,
                               Collection<Iterable<StarTreeRecord>> recordStreams,
                               File outputDir)
  {
    this.numTimeBuckets = numTimeBuckets;
    this.keepMetricValues = keepMetricValues;
    this.keepBuffers = keepBuffers;
    this.starTreeConfig = starTreeConfig;
    this.recordStreams = recordStreams;
    this.outputDir = outputDir;
    this.dataDir = new File(outputDir, DATA_DIR);
    this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  @Override
  public void run()
  {
    try
    {
      if (!outputDir.exists() && outputDir.mkdir())
      {
        LOG.info("Created {}", outputDir);
      }

      StarTreeManager starTreeManager = new StarTreeManagerImpl(executorService);

      // Register config
      starTreeManager.registerConfig(starTreeConfig.getCollection(), starTreeConfig);

      // Build tree
      int streamId = 0;
      for (Iterable<StarTreeRecord> recordStream : recordStreams)
      {
        LOG.info("Processing stream {} of {}", ++streamId, recordStreams.size());
        starTreeManager.load(starTreeConfig.getCollection(), recordStream);
      }

      // Serialize tree structure
      StarTree starTree = starTreeManager.getStarTree(starTreeConfig.getCollection());

      // Write file
      File starTreeFile = new File(outputDir, TREE_FILE);
      LOG.info("Writing {}", starTreeFile);
      ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(starTreeFile));
      os.writeObject(starTree.getRoot());
      os.flush();
      os.close();

      // Convert buffers from variable to fixed, adding "other" buckets as appropriate, and store
      if (keepBuffers)
      {
        if (!dataDir.exists() && dataDir.mkdir())
        {
          LOG.info("Created {}", dataDir);
        }
        writeFixedBuffers(starTree.getRoot());
      }

      // Create record store config
      Properties recordStoreFactoryConfig = new Properties();
      recordStoreFactoryConfig.setProperty("numTimeBuckets", Integer.toString(numTimeBuckets));

      // Create star tree config
      Map<String, Object> configJson = new HashMap<String, Object>();
      configJson.put("collection", starTreeConfig.getCollection());
      configJson.put("dimensionNames", starTreeConfig.getDimensionNames());
      configJson.put("metricNames", starTreeConfig.getMetricNames());
      configJson.put("timeColumnName", starTreeConfig.getTimeColumnName());
      configJson.put("thresholdFunctionClass", starTreeConfig.getThresholdFunction().getClass().getCanonicalName());
      configJson.put("thresholdFunctionConfig", starTreeConfig.getThresholdFunction().getConfig());
      configJson.put("recordStoreFactoryClass", StarTreeRecordStoreFactoryCircularBufferImpl.class.getCanonicalName());
      configJson.put("recordStoreFactoryConfig", recordStoreFactoryConfig);

      // Write config
      File configFile = new File(outputDir, CONFIG_FILE);
      LOG.info("Writing {}", configFile);
      OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(configFile, configJson);
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
    finally
    {
      executorService.shutdown();
    }
  }

  private void writeFixedBuffers(StarTreeNode node) throws IOException
  {
    if (node.isLeaf())
    {
      int bufferSize = node.getRecordStore().size() * // number of records in the store
              (starTreeConfig.getDimensionNames().size() * Integer.SIZE / 8 // the dimension part
                      + (starTreeConfig.getMetricNames().size() + 1) * numTimeBuckets * Long.SIZE / 8); // metric + time


      // Create forward index
      Map<String, Map<String, Integer>> forwardIndex = new HashMap<String, Map<String, Integer>>();
      int nextValueId = StarTreeConstants.FIRST_VALUE;
      for (StarTreeRecord record : node.getRecordStore())
      {
        for (String dimensionName : starTreeConfig.getDimensionNames())
        {
          // Init value map for dimension
          Map<String, Integer> forward = forwardIndex.get(dimensionName);
          if (forward == null)
          {
            forward = new HashMap<String, Integer>();
            forward.put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);
            forward.put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
            forwardIndex.put(dimensionName, forward);
          }

          // Register id if we haven't seen this value before
          String dimensionValue = record.getDimensionValues().get(dimensionName);
          Integer valueId = forward.get(dimensionValue);
          if (valueId == null)
          {
            forward.put(dimensionValue, nextValueId++);
          }
        }
      }

      // Write index
      File file = new File(dataDir, node.getId().toString() + StarTreeRecordStoreFactoryCircularBufferImpl.INDEX_SUFFIX);
      OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file, forwardIndex);
      LOG.info("Wrote {} ({} MB)", file, file.length() / (1024.0 * 1024));

      // Fill buffer in fixed format
      if (bufferSize > 0)
      {
        file = new File(dataDir, node.getId() + StarTreeRecordStoreFactoryCircularBufferImpl.BUFFER_SUFFIX);
        OutputStream outputStream = new FileOutputStream(file);
        StarTreeRecordStoreCircularBufferImpl.fillBuffer(
                outputStream,
                starTreeConfig.getDimensionNames(),
                starTreeConfig.getMetricNames(),
                forwardIndex,
                node.getRecordStore(),
                numTimeBuckets,
                keepMetricValues);
        outputStream.flush();
        outputStream.close();
        LOG.info("Wrote {} ({} MB)", file, file.length() / (1024.0 * 1024));
      }
      else
      {
        file = new File(dataDir, node.getId().toString() + StarTreeRecordStoreFactoryCircularBufferImpl.BUFFER_SUFFIX);
        if (file.createNewFile())
        {
          LOG.info("Created empty file {} ({} MB)", file, file.length() / (1024.0 * 1024));
        }
      }
    }
    else
    {
      for (StarTreeNode child : node.getChildren())
      {
        writeFixedBuffers(child);
      }
      writeFixedBuffers(node.getOtherNode());
      writeFixedBuffers(node.getStarNode());
    }
  }

  public static void main(String[] args) throws Exception
  {
    // Options
    Options options = new Options();
    options.addOption("fileType", true, "File type (avro|tsv)");
    options.addOption("keepMetricValues", false, "Keep metric values in buffers (default: false)");
    options.addOption("keepBuffers", false, "Generate buffers (default: false)");
    options.addOption("numTimeBuckets", true, "Number of time buckets (this times time granularity is retention period)");

    // Parse
    CommandLine commandLine = new GnuParser().parse(options, args);
    if (commandLine.getArgs().length < 3)
    {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp("usage: [opts] configFile outputDir inputFile(s) ...", options);
      return;
    }

    // Args
    String configJson = commandLine.getArgs()[0];
    String outputDir = commandLine.getArgs()[1];
    String[] inputFiles = Arrays.copyOfRange(commandLine.getArgs(), 2, commandLine.getArgs().length);

    // Options
    String fileType = commandLine.getOptionValue("fileType", "tsv");
    Boolean keepMetricValues = commandLine.hasOption("keepMetricValues");
    Boolean keepBuffers = commandLine.hasOption("keepBuffers");
    Integer numTimeBuckets = Integer.valueOf(commandLine.getOptionValue("numTimeBuckets", "" + (7 * 24))); // assuming 1wk @ 1hr granularity

    // Config
    StarTreeConfig config = StarTreeConfig.fromJson(OBJECT_MAPPER.readTree(new File(configJson)));

    // Construct record streams
    List<Iterable<StarTreeRecord>> recordStreams = new ArrayList<Iterable<StarTreeRecord>>();
    if ("avro".equals(fileType))
    {
      for (String inputFile : inputFiles)
      {
        recordStreams.add(new StarTreeRecordStreamAvroFileImpl(
                new File(inputFile),
                config.getDimensionNames(),
                config.getMetricNames(),
                config.getTimeColumnName()));
      }
    }
    else if ("tsv".equals(fileType))
    {
      for (String inputFile : inputFiles)
      {
        recordStreams.add(new StarTreeRecordStreamTextStreamImpl(
                new FileInputStream(inputFile),
                config.getDimensionNames(),
                config.getMetricNames(),
                "\t"));
      }
    }
    else
    {
      throw new IllegalArgumentException("Invalid file type " + fileType);
    }

    // Run bootstrap job
    new StarTreeBootstrapTool(numTimeBuckets,
                              keepMetricValues,
                              keepBuffers,
                              config,
                              recordStreams,
                              new File(outputDir)).run();
  }
}
