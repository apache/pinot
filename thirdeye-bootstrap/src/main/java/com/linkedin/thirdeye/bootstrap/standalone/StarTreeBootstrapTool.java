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
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryFixedCircularBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFixedCircularBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStreamAvroFileImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStreamTextStreamImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;
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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
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

  private final String collection;
  private final long startTime;
  private final long endTime;
  private final StarTreeConfig starTreeConfig;
  private final Collection<Iterable<StarTreeRecord>> recordStreams;
  private final File outputDir;
  private final ExecutorService executorService;

  public StarTreeBootstrapTool(String collection,
                               long startTime,
                               long endTime,
                               StarTreeConfig starTreeConfig,
                               Collection<Iterable<StarTreeRecord>> recordStreams,
                               File outputDir)
  {
    this.collection = collection;
    this.startTime = startTime;
    this.endTime = endTime;
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
      File starTreeFile = new File(outputDir, TREE_FILE);
      LOG.info("Writing {}", starTreeFile);
      ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(starTreeFile));
      os.writeObject(starTree.getRoot());
      os.flush();
      os.close();

      // Convert buffers from variable to fixed, adding "other" buckets as appropriate, and store
      File bufferDir = new File(outputDir, DATA_DIR);
      if (!bufferDir.exists() && bufferDir.mkdir())
      {
        LOG.info("Created {}", bufferDir);
      }
      writeFixedBuffers(starTreeConfig, startTime, endTime, starTree.getRoot(), bufferDir);

      // Create new config
      Properties recordStoreFactoryConfig = new Properties();
      recordStoreFactoryConfig.setProperty("rootDir", bufferDir.getAbsolutePath()); // TODO: Something easily usable remotely
      Map<String, Object> configJson = new HashMap<String, Object>();
      configJson.put("dimensionNames", starTreeConfig.getDimensionNames());
      configJson.put("metricNames", starTreeConfig.getMetricNames());
      configJson.put("timeColumnName", starTreeConfig.getTimeColumnName());
      configJson.put("thresholdFunctionClass", starTreeConfig.getThresholdFunction().getClass().getCanonicalName());
      configJson.put("thresholdFunctionConfig", starTreeConfig.getThresholdFunction().getConfig());
      configJson.put("recordStoreFactoryClass", StarTreeRecordStoreFactoryFixedCircularBufferImpl.class.getCanonicalName());
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

  private static void writeFixedBuffers(StarTreeConfig config,
                                        long startTime,
                                        long endTime,
                                        StarTreeNode root,
                                        File outputDir) throws IOException
  {
    if (root.isLeaf())
    {
      writeFixedRecordStore(config, startTime, endTime, root.getRecordStore(), outputDir, root.getId());
    }
    else
    {
      for (StarTreeNode child : root.getChildren())
      {
        writeFixedBuffers(config, startTime, endTime, child, outputDir);
      }
      writeFixedBuffers(config, startTime, endTime, root.getOtherNode(), outputDir);
      writeFixedBuffers(config, startTime, endTime, root.getStarNode(), outputDir);
    }
  }

  private static void writeFixedRecordStore(final StarTreeConfig config,
                                            final long startTime,
                                            final long endTime,
                                            final StarTreeRecordStore recordStore,
                                            final File rootDir,
                                            final UUID nodeId) throws IOException
  {
    List<StarTreeRecord> records = new ArrayList<StarTreeRecord>();
    Map<Map<String, String>, Set<Long>> dimensionCombinations = new HashMap<Map<String, String>, Set<Long>>();

    // Get records from store
    Map<String, List<StarTreeRecord>> groupedRecords = new HashMap<String, List<StarTreeRecord>>();
    for (StarTreeRecord record : recordStore)
    {
      // Record time / dimension combination
      Set<Long> times = dimensionCombinations.get(record.getDimensionValues());
      if (times == null)
      {
        times = new HashSet<Long>();
        dimensionCombinations.put(record.getDimensionValues(), times);
      }
      times.add(record.getTime());

      List<StarTreeRecord> group = groupedRecords.get(record.getKey());
      if (group == null)
      {
        group = new ArrayList<StarTreeRecord>();
        groupedRecords.put(record.getKey(), group);
      }
      group.add(record);
    }

    // Write catch-all "other" bucket for each time bucket
    for (long timeBucket = startTime; timeBucket <= endTime; timeBucket++)
    {
      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
      for (String dimensionName : config.getDimensionNames())
      {
        builder.setDimensionValue(dimensionName, StarTreeConstants.OTHER);
      }
      for (String metricName : config.getMetricNames())
      {
        builder.setMetricValue(metricName, 0L);
      }
      builder.setTime(timeBucket);

      // Only add if it's not already there
      StarTreeRecord other = builder.build();
      if (!records.contains(other))
      {
        records.add(other);
      }
    }

    // Add a placeholder record with zeroed out metric values for each bucket we didn't see
    for (Map.Entry<Map<String, String>, Set<Long>> entry : dimensionCombinations.entrySet())
    {
      Map<String, String> dimensionValues = entry.getKey();
      Set<Long> times = entry.getValue();

      for (long i = startTime; i <= endTime; i++)
      {
        if (!times.contains(i))
        {
          StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
          builder.setDimensionValues(dimensionValues);
          for (String metricName : config.getMetricNames())
          {
            builder.setMetricValue(metricName, 0L);
          }
          builder.setTime(i); // okay to be bucket since this is a placeholder
          records.add(builder.build());
        }
      }
    }

    // Aggregate these records
    for (List<StarTreeRecord> group : groupedRecords.values())
    {
      records.add(StarTreeUtils.merge(group));
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

        // Always add "*" and "?" as well
        valueIds.put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);
        valueIds.put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
      }
    }

    // Sort records by time then dimensions w.r.t. forwardIndex
    final int numBuckets = (int) (endTime - startTime + 1); // inclusive
    Collections.sort(records, new Comparator<StarTreeRecord>()
    {
      @Override
      public int compare(StarTreeRecord o1, StarTreeRecord o2)
      {
        int b1 = (int) (o1.getTime() % numBuckets);
        int b2 = (int) (o2.getTime() % numBuckets);

        if (b1 != b2)
        {
          return b1 - b2;
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

        if (!o1.getTime().equals(o2.getTime()))
        {
          return (int) (o1.getTime() - o2.getTime());
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
              numBuckets);
    }
    buffer.flip();

    // Write record store
    File file = new File(rootDir, nodeId.toString() + StarTreeRecordStoreFactoryFixedCircularBufferImpl.BUFFER_SUFFIX);
    FileChannel fileChannel = new FileOutputStream(file).getChannel();
    fileChannel.write(buffer);
    fileChannel.force(true);
    fileChannel.close();
    LOG.info("Wrote {}", file);

    // Write index
    file = new File(rootDir, nodeId.toString() + StarTreeRecordStoreFactoryFixedCircularBufferImpl.INDEX_SUFFIX);
    OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValue(file, forwardIndex);
    LOG.info("Wrote {}", file);
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length < 7)
    {
      throw new IllegalArgumentException("usage: collection startTime endTime config.json fileType outputDir inputFile ...");
    }

    // Parse args
    String collection = args[0];
    String startTime = args[1];
    String endTime = args[2];
    String configJson = args[3];
    String fileType = args[4];
    String outputDir = args[5];
    String[] inputFiles = Arrays.copyOfRange(args, 6, args.length);

    // Parse config
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
    new StarTreeBootstrapTool(collection,
                              Long.valueOf(startTime),
                              Long.valueOf(endTime),
                              config,
                              recordStreams,
                              new File(outputDir)).run();
  }
}
