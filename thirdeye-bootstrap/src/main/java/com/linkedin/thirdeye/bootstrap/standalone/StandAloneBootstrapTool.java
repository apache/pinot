package com.linkedin.thirdeye.bootstrap.standalone;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFixedCircularBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStreamAvroFileImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStreamTextStreamImpl;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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

      // Convert buffers from variable to fixed, adding "other" buckets as appropriate, and store
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }

  private static void writeFixedBuffers(StarTreeConfig config, StarTreeNode root, File outputDir) throws IOException
  {
    if (root.isLeaf())
    {
      List<String> fixedDimensions = new ArrayList<String>(root.getAncestorDimensionNames());
      fixedDimensions.add(root.getDimensionName());

      ByteBuffer fixedBuffer = fixRecordStore(config, root.getRecordStore(), fixedDimensions);
      fixedBuffer.rewind();

      FileChannel fileChannel = new FileOutputStream(new File(outputDir, root.getId().toString())).getChannel();
      fileChannel.write(fixedBuffer);
      fileChannel.close();
    }
    else
    {
      for (StarTreeNode child : root.getChildren())
      {
        writeFixedBuffers(config, child, outputDir);
      }
      writeFixedBuffers(config, root.getOtherNode(), outputDir);
      writeFixedBuffers(config, root.getStarNode(), outputDir);
    }
  }

  private static ByteBuffer fixRecordStore(final StarTreeConfig config,
                                           final StarTreeRecordStore recordStore,
                                           final List<String> fixedDimensions)
  {
    // Get loose dimensions
    List<String> looseDimensions = new ArrayList<String>(config.getDimensionNames());
    looseDimensions.removeAll(fixedDimensions);

    // For recursive method
    List<Boolean> chosenLooseDimensions = new ArrayList<Boolean>(looseDimensions.size());
    Map<String, String> dimensionValues = new HashMap<String, String>();
    for (String looseDimension : looseDimensions)
    {
      chosenLooseDimensions.add(false);
    }

    // Get records from store, and add "other" combination w/ metrics == 0 for non-fixed dimensions
    List<StarTreeRecord> records = new ArrayList<StarTreeRecord>();
    Set<Long> timeBuckets = new HashSet<Long>();
    for (StarTreeRecord record : recordStore)
    {
      timeBuckets.add(record.getTime());
      records.add(record);
      dimensionValues.putAll(record.getDimensionValues());
      addOtherPlaceholders(config, dimensionValues, record.getTime(), looseDimensions, chosenLooseDimensions, records);
      dimensionValues.clear();
    }

    // Create forward index
    int currentId = StarTreeConstants.FIRST_VALUE;
    final Map<String, Map<String, Integer>> forwardIndex = new HashMap<String, Map<String, Integer>>();
    for (StarTreeRecord record : records)
    {
      for (String dimensionName : config.getDimensionNames())
      {
        Map<String, Integer> valueIds = forwardIndex.get(dimensionName);
        if (valueIds == null)
        {
          valueIds = new HashMap<String, Integer>();
          forwardIndex.put(dimensionName, valueIds);
        }

        String dimensionValue = record.getDimensionValues().get(dimensionName);
        Integer valueId = valueIds.get(dimensionValue);
        if (valueId == null)
        {
          valueId = currentId++;
          valueIds.put(dimensionValue, valueId);
        }
      }
    }

    // Sort records by time then dimensions w.r.t. forwardIndex
    Collections.sort(records, new Comparator<StarTreeRecord>()
    {
      @Override
      public int compare(StarTreeRecord o1, StarTreeRecord o2)
      {
        if (!o1.getTime().equals(o2.getTime()))
        {
          return (int) (o1.getTime() - o2.getTime());
        }

        for (String dimensionName : config.getDimensionNames())
        {
          // Get IDs from forward index
          String v1 = o1.getDimensionValues().get(dimensionName);
          String v2 = o2.getDimensionValues().get(dimensionName);
          int i1 = forwardIndex.get(dimensionName).get(v1);
          int i2 = forwardIndex.get(dimensionName).get(v2);

          if (i1 != i2)
          {
            return i1 - i2;
          }
        }

        return 0;
      }
    });

    // Write to a buffer
    int entrySize = StarTreeRecordStoreFixedCircularBufferImpl.getEntrySize(config.getDimensionNames(), config.getMetricNames());
    int bufferSize = records.size() * entrySize;
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    for (StarTreeRecord record : records)
    {
      StarTreeRecordStoreFixedCircularBufferImpl.writeRecord(
              buffer,
              record,
              config.getDimensionNames(),
              config.getMetricNames(),
              forwardIndex,
              timeBuckets.size());
    }

    return buffer;
  }

  private static void addOtherPlaceholders(StarTreeConfig config,
                                           Map<String, String> dimensionValues,
                                           Long time,
                                           List<String> looseDimensions,
                                           List<Boolean> chosenLooseDimensions,
                                           List<StarTreeRecord> collector)
  {
    boolean allChosen = true;
    for (Boolean oneChosen : chosenLooseDimensions)
    {
      allChosen &= oneChosen;
    }

    if (allChosen)
    {
      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
      builder.setDimensionValues(dimensionValues);
      for (String metricName : config.getMetricNames())
      {
        builder.setMetricValue(metricName, 0L);
      }
      builder.setTime(time);
      collector.add(builder.build());
    }
    else
    {
      for (int i = 0; i < looseDimensions.size(); i++)
      {
        if (!chosenLooseDimensions.get(i))
        {
          chosenLooseDimensions.set(i, true);

          String dimensionName = looseDimensions.get(i);
          String dimensionValue = dimensionValues.get(looseDimensions.get(i));

          dimensionValues.put(dimensionName, StarTreeConstants.OTHER);

          addOtherPlaceholders(config, dimensionValues, time, looseDimensions, chosenLooseDimensions, collector);

          dimensionValues.put(dimensionName, dimensionValue);
          chosenLooseDimensions.set(i, false);
        }
      }
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
    else if ("tsv".equals(fileType))
    {
      for (String inputFile : inputFiles)
      {
        recordStreams.add(new StarTreeRecordStreamTextStreamImpl(
                new FileInputStream(inputFile),
                dimensionNames,
                metricNames,
                "\t"));
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
