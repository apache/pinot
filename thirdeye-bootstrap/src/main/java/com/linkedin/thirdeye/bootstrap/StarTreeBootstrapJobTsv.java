package com.linkedin.thirdeye.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreCircularBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryCircularBufferImpl;
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

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class StarTreeBootstrapJobTsv extends Configured
{
  public static final String PROP_STARTREE_CONFIG = "startree.config";
  public static final String PROP_STARTREE_ROOT = "startree.root";
  public static final String PROP_INPUT_PATHS = "input.paths";
  public static final String PROP_OUTPUT_PATH = "output.path";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String name;
  private final Properties props;

  public StarTreeBootstrapJobTsv(String name, Properties props)
  {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class StarTreeBootstrapMapper extends Mapper<Object, Text, Text, Text>
  {
    private static final Logger LOG = LoggerFactory.getLogger(StarTreeBootstrapMapper.class);

    private final Text nodeId = new Text();
    private final Text recordTsv = new Text();

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
      Map<UUID, StarTreeRecord> records = new HashMap<UUID, StarTreeRecord>();

      collectRecords(starTree.getRoot(), toRecord(starTree.getConfig(), value), records);

      for (Map.Entry<UUID, StarTreeRecord> entry : records.entrySet())
      {
        nodeId.set(entry.getKey().toString());
        recordTsv.set(fromRecord(starTree.getConfig(), entry.getValue()));
        context.write(nodeId, recordTsv);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
      StarTreeRecordImpl.Builder otherRecord = new StarTreeRecordImpl.Builder();

      for (String dimensionName : starTree.getConfig().getDimensionNames())
      {
        otherRecord.setDimensionValue(dimensionName, StarTreeConstants.OTHER);
      }

      for (String metricName : starTree.getConfig().getMetricNames())
      {
        otherRecord.setMetricValue(metricName, 0);
      }

      otherRecord.setTime(0L);

      Text value = new Text(fromRecord(starTree.getConfig(), otherRecord.build()));

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

    private void collectRecords(StarTreeNode node, StarTreeRecord record, Map<UUID, StarTreeRecord> collector)
    {
      if (node.isLeaf())
      {
        collector.put(node.getId(), record);
      }
      else
      {
        StarTreeNode target = node.getChild(record.getDimensionValues().get(node.getChildDimensionName()));
        if (target == null)
        {
          target = node.getOtherNode();
        }
        collectRecords(target, record, collector);
        collectRecords(node.getStarNode(), record.relax(target.getDimensionName()), collector);
      }
    }
  }

  public static class StarTreeBootstrapCombiner extends Reducer<Text, Text, Text, Text>
  {
    private static final Logger LOG = LoggerFactory.getLogger(StarTreeBootstrapCombiner.class);

    private final Text outputTsvRecord = new Text();

    private StarTreeConfig config;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      FileSystem fileSystem = FileSystem.get(context.getConfiguration());
      Path configPath = new Path(context.getConfiguration().get(PROP_STARTREE_CONFIG));

      try
      {
        config = StarTreeConfig.fromJson(OBJECT_MAPPER.readTree(fileSystem.open(configPath)));
      }
      catch (Exception e)
      {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(Text nodeId, Iterable<Text> tsvRecords, Context context) throws IOException, InterruptedException
    {
      Map<String, StarTreeRecord> aggregates = new HashMap<String, StarTreeRecord>();

      for (Text tsvRecord : tsvRecords)
      {
        StarTreeRecord record = toRecord(config, tsvRecord);

        StarTreeRecord aggregate = aggregates.get(record.getKey(true));
        if (aggregate == null || aggregate.getTime() < record.getTime())
        {
          aggregates.put(record.getKey(true), record);
        }
        else if (aggregate.getTime().equals(record.getTime()))
        {
          aggregates.put(record.getKey(true), StarTreeUtils.merge(Arrays.asList(record, aggregate)));
        }
      }

      for (StarTreeRecord record : aggregates.values())
      {
        outputTsvRecord.set(fromRecord(config, record));
        context.write(nodeId, outputTsvRecord);
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

      // Create new forward index
      int nextValueId = StarTreeConstants.FIRST_VALUE;
      Map<String, Map<String, Integer>> forwardIndex = new HashMap<String, Map<String, Integer>>();
      for (StarTreeRecord record : mergedRecords)
      {
        for (String dimensionName : config.getDimensionNames())
        {
          // Initialize per-dimension index (needs * and ?)
          Map<String, Integer> forward = forwardIndex.get(dimensionName);
          if (forward == null)
          {
            forward = new HashMap<String, Integer>();
            forward.put(StarTreeConstants.STAR, StarTreeConstants.STAR_VALUE);
            forward.put(StarTreeConstants.OTHER, StarTreeConstants.OTHER_VALUE);
            forwardIndex.put(dimensionName, forward);
          }

          // Allocate new value if necessary
          String dimensionValue = record.getDimensionValues().get(dimensionName);
          Integer valueId = forward.get(dimensionValue);
          if (valueId == null)
          {
            forward.put(dimensionValue, nextValueId++);
          }
        }
      }

      // Load records into buffer
      Path bufferPath = new Path(outputPath, nodeId + StarTreeRecordStoreFactoryCircularBufferImpl.BUFFER_SUFFIX);
      OutputStream outputStream = new BufferedOutputStream(FileSystem.get(context.getConfiguration()).create(bufferPath, true));
      StarTreeRecordStoreCircularBufferImpl.fillBuffer(
              outputStream,
              config.getDimensionNames(),
              config.getMetricNames(),
              forwardIndex,
              mergedRecords,
              numTimeBuckets,
              true);
      outputStream.flush();
      outputStream.close();

      // Write forward index to file
      Path indexPath = new Path(outputPath, nodeId.toString() + StarTreeRecordStoreFactoryCircularBufferImpl.INDEX_SUFFIX);
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
        builder.setMetricValue(metricName, Integer.valueOf(tokens[idx++]));
      }

      builder.setTime(Long.valueOf(tokens[idx]));

      return builder.build();
    }
  }

  public void run() throws Exception
  {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeBootstrapJobTsv.class);

    // Map config
    job.setMapperClass(StarTreeBootstrapMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    // Combiner
    job.setCombinerClass(StarTreeBootstrapCombiner.class);

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

  private static StarTreeRecord toRecord(StarTreeConfig config, Text value)
  {
    StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();

    int idx = 0;
    String[] tokens = value.toString().split("\t");

    for (String dimensionName : config.getDimensionNames())
    {
      builder.setDimensionValue(dimensionName, tokens[idx++]);
    }

    for (String metricName : config.getMetricNames())
    {
      builder.setMetricValue(metricName, Integer.valueOf(tokens[idx++]));
    }

    builder.setTime(Long.valueOf(tokens[idx]));

    return builder.build();
  }

  private static String fromRecord(StarTreeConfig config, StarTreeRecord record)
  {
    StringBuilder sb = new StringBuilder();

    for (String dimensionName : config.getDimensionNames())
    {
      sb.append(record.getDimensionValues().get(dimensionName)).append("\t");
    }

    for (String metricName : config.getMetricNames())
    {
      sb.append(record.getMetricValues().get(metricName)).append("\t");
    }

    sb.append(record.getTime());

    return sb.toString();
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length != 1)
    {
      throw new IllegalArgumentException("usage: config.properties");
    }

    Properties props = new Properties();
    props.load(new FileInputStream(args[0]));

    StarTreeBootstrapJobTsv bootstrapJob = new StarTreeBootstrapJobTsv("star_tree_bootstrap_job", props);
    bootstrapJob.run();
  }
}
