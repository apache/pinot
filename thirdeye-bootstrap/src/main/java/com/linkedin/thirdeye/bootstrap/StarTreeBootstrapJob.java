package com.linkedin.thirdeye.bootstrap;

import static com.linkedin.thirdeye.bootstrap.StarTreeJobConstants.*;

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
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class StarTreeBootstrapJob extends Configured
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeBootstrapJob.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String name;
  private final Properties props;

  public StarTreeBootstrapJob(String name, Properties props)
  {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class StarTreeBootstrapMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, AvroValue<GenericRecord>>
  {
    private final Text nodeId = new Text();
    private final AvroValue<GenericRecord> outputRecord = new AvroValue<GenericRecord>();

    private StarTree starTree;
    private Schema schema;
    private Long minTime;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      FileSystem fileSystem = FileSystem.get(context.getConfiguration());
      Path rootPath = new Path(context.getConfiguration().get(PROP_STARTREE_ROOT));
      Path configPath = new Path(context.getConfiguration().get(PROP_STARTREE_CONFIG));
      File outputFile = new File(context.getConfiguration().get(PROP_OUTPUT_PATH));

      try
      {
        StarTreeConfig config = StarTreeConfig.fromJson(OBJECT_MAPPER.readTree(fileSystem.open(configPath)), outputFile);
        ObjectInputStream objectInputStream = new ObjectInputStream(fileSystem.open(rootPath));
        StarTreeNode root = (StarTreeNode) objectInputStream.readObject();
        starTree = new StarTreeImpl(config, root);
        // n.b. do not open this tree, but just use structure for routing
      }
      catch (Exception e)
      {
        throw new IOException(e);
      }
    }

    @Override
    public void map(AvroKey<GenericRecord> record, NullWritable value, Context context) throws IOException, InterruptedException
    {
      // Note schema
      if (schema == null)
      {
        schema = record.datum().getSchema();
      }

      // Collect specific / star records from tree that match
      Map<UUID, StarTreeRecord> collector = new HashMap<UUID, StarTreeRecord>();
      StarTreeRecord starTreeRecord = StarTreeJobUtils.toStarTreeRecord(starTree.getConfig(), record.datum());
      StarTreeJobUtils.collectRecords(starTree.getRoot(), starTreeRecord, collector);

      // Record minimum time (for other record)
      if (minTime == null || starTreeRecord.getTime() < minTime)
      {
        minTime = starTreeRecord.getTime();
      }

      // Output them
      for (Map.Entry<UUID, StarTreeRecord> entry : collector.entrySet())
      {
        nodeId.set(entry.getKey().toString());
        outputRecord.datum(StarTreeJobUtils.toGenericRecord(starTree.getConfig(), schema, entry.getValue(), outputRecord.datum()));
        context.write(nodeId, outputRecord);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
      // Build other value
      StarTreeRecordImpl.Builder otherRecord = new StarTreeRecordImpl.Builder();
      for (String dimensionName : starTree.getConfig().getDimensionNames())
      {
        otherRecord.setDimensionValue(dimensionName, StarTreeConstants.OTHER);
      }
      for (String metricName : starTree.getConfig().getMetricNames())
      {
        otherRecord.setMetricValue(metricName, 0);
      }
      otherRecord.setTime(minTime);

      // Write it for each node
      AvroValue<GenericRecord> value = new AvroValue<GenericRecord>(
              StarTreeJobUtils.toGenericRecord(starTree.getConfig(),
                                               schema,
                                               otherRecord.build(),
                                               null));
      writeOtherRecord(context, starTree.getRoot(), value);
    }

    private void writeOtherRecord(Context context,
                                  StarTreeNode node,
                                  AvroValue<GenericRecord> value) throws IOException, InterruptedException
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

  public static class StarTreeBootstrapReducer extends Reducer<Text, AvroValue<GenericRecord>, NullWritable, NullWritable>
  {
    private StarTreeConfig config;
    private int numTimeBuckets;
    private Path outputPath;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      try
      {
        File outputFile = new File(context.getConfiguration().get(PROP_OUTPUT_PATH));
        Path configPath = new Path(context.getConfiguration().get(PROP_STARTREE_CONFIG));
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        config = StarTreeConfig.fromJson(OBJECT_MAPPER.readTree(fileSystem.open(configPath)), outputFile);
        numTimeBuckets = Integer.valueOf(config.getRecordStoreFactory().getConfig().getProperty("numTimeBuckets"));

        LOG.info("Creating buffers with {} time buckets", numTimeBuckets);
      }
      catch (Exception e)
      {
        throw new IOException(e);
      }

      outputPath = new Path(context.getConfiguration().get(PROP_OUTPUT_PATH));
    }

    @Override
    public void reduce(Text nodeId, Iterable<AvroValue<GenericRecord>> avroRecords, Context context) throws IOException, InterruptedException
    {
      Map<String, Map<Long, StarTreeRecord>> records = StarTreeJobUtils.aggregateRecords(config, avroRecords, numTimeBuckets);

      LOG.info("Writing {} aggregate records to {}", records.size(), nodeId.toString());

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
  }

  public void run() throws Exception
  {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeBootstrapJob.class);

    // Avro schema
    Schema schema = new Schema.Parser().parse(FileSystem.get(getConf()).open(new Path(getAndCheck(PROP_AVRO_SCHEMA))));
    LOG.info("{}", schema);

    // Map config
    job.setMapperClass(StarTreeBootstrapMapper.class);
    AvroJob.setInputKeySchema(job, schema);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(AvroValue.class);
    AvroJob.setMapOutputValueSchema(job, schema);

    // Reduce config
    job.setReducerClass(StarTreeBootstrapReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    // Star-tree config
    job.getConfiguration().set(PROP_STARTREE_CONFIG, getAndCheck(PROP_STARTREE_CONFIG));
    job.getConfiguration().set(PROP_STARTREE_ROOT, getAndCheck(PROP_STARTREE_ROOT));
    job.getConfiguration().set(PROP_OUTPUT_PATH, getAndCheck(PROP_OUTPUT_PATH));
    job.getConfiguration().set(PROP_AVRO_SCHEMA, getAndCheck(PROP_AVRO_SCHEMA));

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

    StarTreeBootstrapJob bootstrapJob = new StarTreeBootstrapJob("star_tree_bootstrap_avro_job", props);
    bootstrapJob.run();
  }
}
