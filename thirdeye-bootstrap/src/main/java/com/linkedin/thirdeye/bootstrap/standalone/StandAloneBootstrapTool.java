package com.linkedin.thirdeye.bootstrap.standalone;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStreamAvroFileImpl;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Takes as input raw star tree record streams, and constructs a tree + fixed leaf buffers.
 */
public class StandAloneBootstrapTool implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(StandAloneBootstrapTool.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final String STARTREE_FILE = "startree.bin";

  private final String collection;
  private final StarTreeConfig starTreeConfig;
  private final Collection<Iterable<StarTreeRecord>> recordStreams;
  private final File outputDir;
  private final ExecutorService executorService;

  public StandAloneBootstrapTool(String collection,
                                 StarTreeConfig starTreeConfig,
                                 Collection<Iterable<StarTreeRecord>> recordStreams,
                                 File outputDir)
  {
    this.collection = collection;
    this.starTreeConfig = starTreeConfig;
    this.recordStreams = recordStreams;
    this.outputDir = outputDir;
    this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
  }

  @Override
  public void run()
  {
    try
    {
      StarTreeManager starTreeManager = new StarTreeManagerImpl(executorService);

      // Register config
      starTreeManager.registerConfig(collection, starTreeConfig);

      // Build tree
      int streamId = 0;
      for (Iterable<StarTreeRecord> recordStream : recordStreams)
      {
        LOG.info("Processing stream {} of {}", ++streamId, recordStreams.size());
        starTreeManager.load(collection, recordStream);
      }

      // Serialize tree structure
      StarTree starTree = starTreeManager.getStarTree(collection);

      // Write file
      File starTreeFile = new File(outputDir, STARTREE_FILE);
      LOG.info("Writing {}", starTreeFile);
      ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(starTreeFile));
      os.writeObject(starTree.getRoot());
      os.flush();
      os.close();
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length < 4)
    {
      throw new IllegalArgumentException("usage: config.json fileType outputDir inputFile ...");
    }

    // Parse args
    String configJson = args[0];
    String fileType = args[1];
    String outputDir = args[2];
    String[] inputFiles = Arrays.copyOfRange(args, 3, args.length);

    // Parse config
    JsonNode config = OBJECT_MAPPER.readTree(new File(configJson));

    // Get dimension names
    List<String> dimensionNames = new ArrayList<String>();
    for (JsonNode dimensionName : config.get("dimensionNames"))
    {
      dimensionNames.add(dimensionName.asText());
    }

    // Get metric names
    List<String> metricNames = new ArrayList<String>();
    for (JsonNode metricName : config.get("metricNames"))
    {
      metricNames.add(metricName.asText());
    }

    // Get time column name
    String timeColumnName = config.get("timeColumnName").asText();

    // Build config
    StarTreeConfig.Builder starTreeConfig = new StarTreeConfig.Builder();
    starTreeConfig.setDimensionNames(dimensionNames)
                  .setMetricNames(metricNames)
                  .setTimeColumnName(timeColumnName);
    if (config.has("thresholdFunctionClass"))
    {
      starTreeConfig.setThresholdFunctionClass(config.get("thresholdFunctionClass").asText());
      if (config.has("thresholdFunctionConfig"))
      {
        Properties props = new Properties();
        Iterator<Map.Entry<String, JsonNode>> itr = config.get("thresholdFunctionConfig").getFields();
        while (itr.hasNext())
        {
          Map.Entry<String, JsonNode> next = itr.next();
          props.put(next.getKey(), next.getValue().asText());
        }
        starTreeConfig.setThresholdFunctionConfig(props);
      }
    }
    if (config.has("maxRecordStoreEntries"))
    {
      starTreeConfig.setMaxRecordStoreEntries(config.get("maxRecordStoreEntries").asInt());
    }

    // Construct record streams
    List<Iterable<StarTreeRecord>> recordStreams = new ArrayList<Iterable<StarTreeRecord>>();
    if ("avro".equals(fileType))
    {
      for (String inputFile : inputFiles)
      {
        recordStreams.add(new StarTreeRecordStreamAvroFileImpl(
                new File(inputFile),
                dimensionNames,
                metricNames,
                timeColumnName));
      }
    }
    else
    {
      throw new IllegalArgumentException("Invalid file type " + fileType);
    }

    // Run bootstrap job
    new StandAloneBootstrapTool(config.get("collection").asText(),
                                starTreeConfig.build(),
                                recordStreams,
                                new File(outputDir)).run();
  }
}
