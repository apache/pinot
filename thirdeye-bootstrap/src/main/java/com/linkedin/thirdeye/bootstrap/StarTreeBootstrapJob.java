package com.linkedin.thirdeye.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreeQueryImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreCircularBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class StarTreeBootstrapJob extends Configured
{
  public static final String PROP_STARTREE_CONFIG = "startree.config";
  public static final String PROP_STARTREE_ROOT = "startree.root";
  public static final String PROP_INPUT_PATHS = "input.paths";
  public static final String PROP_OUTPUT_PATH = "output.path";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String name;
  private final Properties props;

  public StarTreeBootstrapJob(String name, Properties props)
  {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class StarTreeBootstrapMapper extends Mapper<Object, Text, Text, Text>
  {
    private final Text nodeId = new Text();

    private StarTree starTree;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      FileSystem fileSystem = FileSystem.get(context.getConfiguration());
      Path rootPath = new Path(context.getConfiguration().get(PROP_STARTREE_ROOT));
      Path configPath = new Path(context.getConfiguration().get(PROP_STARTREE_CONFIG));

      try
      {
        StarTreeConfig config = StarTreeConfig.fromJson(OBJECT_MAPPER.readTree(fileSystem.open(configPath)));
        ObjectInputStream objectInputStream = new ObjectInputStream(fileSystem.open(rootPath));
        StarTreeNode root = (StarTreeNode) objectInputStream.readObject();
        starTree = new StarTreeImpl(config, root);
      }
      catch (Exception e)
      {
        throw new IOException(e);
      }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      int idx = 0;
      String[] tokens = value.toString().split("\t");

      // TSV -> query
      StarTreeQueryImpl.Builder query = new StarTreeQueryImpl.Builder();
      for (String dimensionName : starTree.getConfig().getDimensionNames())
      {
        query.setDimensionValue(dimensionName, tokens[idx++]);
      }
      query.setTimeBuckets(new HashSet<Long>(Arrays.asList(Long.valueOf(tokens[idx]))));

      // Get node IDs
      Collection<StarTreeNode> nodes = starTree.findAll(query.build());

      // Output ID -> entry mapping
      for (StarTreeNode node : nodes)
      {
        nodeId.set(node.getId().toString());
        context.write(nodeId, value);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
      // Write an all-other node w/ empty metric values for each
      StringBuilder sb = new StringBuilder();
      for (String dimensionName : starTree.getConfig().getDimensionNames())
      {
        sb.append(StarTreeConstants.OTHER).append("\t");
      }
      for (String metricName : starTree.getConfig().getMetricNames())
      {
        sb.append(0).append("\t");
      }
      sb.append(0);

      Text value = new Text(sb.toString());

      writeOtherRecord(context, starTree.getRoot(), value);
    }

    private void writeOtherRecord(Context context, StarTreeNode node, Text value) throws IOException, InterruptedException
    {
      if (node.isLeaf())
      {
        nodeId.set(node.getId().toString());
        context.write(nodeId, value);
      }
      else
      {
        for (StarTreeNode child : node.getChildren())
        {
          writeOtherRecord(context, child, value);
        }
        writeOtherRecord(context, node.getOtherNode(), value);
        writeOtherRecord(context, node.getStarNode(), value);
      }
    }
  }

  public static class StarTreeBootstrapReducer extends Reducer<Text, Text, NullWritable, NullWritable>
  {
    private static final Logger LOG = LoggerFactory.getLogger(StarTreeBootstrapReducer.class);

    private StarTreeConfig config;
    private int numTimeBuckets;
    private Path outputPath;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      try
      {
        Path configPath = new Path(context.getConfiguration().get(PROP_STARTREE_CONFIG));
        config = StarTreeConfig.fromJson(
                OBJECT_MAPPER.readTree(FileSystem.get(context.getConfiguration()).open(configPath)));
        numTimeBuckets = Integer.valueOf(config.getRecordStoreFactory().getConfig().getProperty("numTimeBuckets"));
      }
      catch (Exception e)
      {
        throw new IOException(e);
      }

      outputPath = new Path(context.getConfiguration().get(PROP_OUTPUT_PATH));
    }

    @Override
    public void reduce(Text nodeId, Iterable<Text> tsvRecords, Context context) throws IOException, InterruptedException
    {
      Map<String, Map<Long, StarTreeRecord>> records = new HashMap<String, Map<Long, StarTreeRecord>>();

      LOG.info("Merging records for " + nodeId.toString());

      // Aggregate records
      for (Text tsvRecord : tsvRecords)
      {
        StarTreeRecord record = createRecord(tsvRecord);

        // Initialize buckets
        Map<Long, StarTreeRecord> timeBuckets = records.get(record.getKey(false));
        if (timeBuckets == null)
        {
          timeBuckets = new HashMap<Long, StarTreeRecord>();
          records.put(record.getKey(false), timeBuckets);
        }

        // Get bucket
        long bucket = record.getTime() % numTimeBuckets;

        // Merge or overwrite existing record
        StarTreeRecord aggRecord = timeBuckets.get(bucket);
        if (aggRecord == null || aggRecord.getTime() < record.getTime())
        {
          timeBuckets.put(bucket, record);
        }
        else if (aggRecord.getTime().equals(record.getTime()))
        {
          timeBuckets.put(bucket, StarTreeUtils.merge(Arrays.asList(record, aggRecord)));
        }
      }

      // Get all merged records
      List<StarTreeRecord> mergedRecords = new ArrayList<StarTreeRecord>();
      for (Map<Long, StarTreeRecord> timeBucket : records.values())
      {
        for (Map.Entry<Long, StarTreeRecord> entry : timeBucket.entrySet())
        {
          mergedRecords.add(entry.getValue());
        }
      }
      records = null; // for gc

      // Create new forward index
      int nextValueId = StarTreeConstants.FIRST_VALUE;
      Map<String, Map<String, Integer>> forwardIndex = new HashMap<String, Map<String, Integer>>();
      for (StarTreeRecord record : mergedRecords)
      {
        for (String dimensionName : config.getDimensionNames())
        {
          Map<String, Integer> forward = forwardIndex.get(dimensionName);
          if (forward == null)
          {
            forward = new HashMap<String, Integer>();
            forwardIndex.put(dimensionName, forward);
          }
          String dimensionValue = record.getDimensionValues().get(dimensionName);
          Integer valueId = forward.get(dimensionValue);
          if (valueId == null)
          {
            forward.put(dimensionValue, nextValueId++);
          }
        }
      }

      // Get buffer
      int bufferSize = mergedRecords.size() * // number of records in the store
              (config.getDimensionNames().size() * Integer.SIZE / 8 // the dimension part
                      + (config.getMetricNames().size() + 1) * numTimeBuckets * Long.SIZE / 8); // metric + time
      ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);

      // Load records into buffer
      buffer.clear();
      StarTreeRecordStoreCircularBufferImpl.fillBuffer(
              buffer,
              config.getDimensionNames(),
              config.getMetricNames(),
              forwardIndex,
              mergedRecords,
              numTimeBuckets,
              true);

      // Write that buffer to file (n.b. known heap buffer so use backing array)
      buffer.flip();
      Path bufferPath = new Path(outputPath, nodeId.toString() + StarTreeRecordStoreFactoryCircularBufferHdfsImpl.BUFFER_SUFFIX);
      OutputStream outputStream = FileSystem.get(context.getConfiguration()).create(bufferPath, true);
      WritableByteChannel channel = Channels.newChannel(outputStream);
      channel.write(buffer);
      outputStream.flush();
      outputStream.close();

      // Write forward index to file
      Path indexPath = new Path(outputPath, nodeId.toString() + StarTreeRecordStoreFactoryCircularBufferHdfsImpl.INDEX_SUFFIX);
      outputStream = FileSystem.get(context.getConfiguration()).create(indexPath, true);
      OBJECT_MAPPER.writeValue(outputStream, forwardIndex);
      outputStream.flush();
      outputStream.close();
    }

    private StarTreeRecord createRecord(Text tsvLine)
    {
      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();

      int idx = 0;
      String[] tokens = tsvLine.toString().split("\t");

      for (String dimensionName : config.getDimensionNames())
      {
        builder.setDimensionValue(dimensionName, tokens[idx++]);
      }

      for (String metricName : config.getMetricNames())
      {
        builder.setMetricValue(metricName, Long.valueOf(tokens[idx++]));
      }

      builder.setTime(Long.valueOf(tokens[idx]));

      return builder.build();
    }
  }

  public void run() throws Exception
  {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeBootstrapJob.class);

    // Map config
    job.setMapperClass(StarTreeBootstrapMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // Reduce config
    job.setReducerClass(StarTreeBootstrapReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    // Star-tree config
    job.getConfiguration().set(PROP_STARTREE_CONFIG, getAndCheck(PROP_STARTREE_CONFIG));
    job.getConfiguration().set(PROP_STARTREE_ROOT, getAndCheck(PROP_STARTREE_ROOT));
    job.getConfiguration().set(PROP_OUTPUT_PATH, getAndCheck(PROP_OUTPUT_PATH));

    for (String inputPath : getAndCheck(PROP_INPUT_PATHS).split(","))
    {
      FileInputFormat.addInputPath(job, new Path(inputPath));
    }

    FileOutputFormat.setOutputPath(job, new Path(getAndCheck(PROP_OUTPUT_PATH)));

    job.waitForCompletion(true);
  }

  private String getAndCheck(String propName)
  {
    String propValue = props.getProperty(propName);
    if (propValue == null)
    {
      throw new IllegalArgumentException(propName + " required property");
    }
    return propValue;
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length != 1)
    {
      throw new IllegalArgumentException("usage: config.properties");
    }

    Properties props = new Properties();
    props.load(new FileInputStream(args[0]));

    StarTreeBootstrapJob bootstrapJob = new StarTreeBootstrapJob("star_tree_bootstrap_job", props);
    bootstrapJob.run();
  }
}
