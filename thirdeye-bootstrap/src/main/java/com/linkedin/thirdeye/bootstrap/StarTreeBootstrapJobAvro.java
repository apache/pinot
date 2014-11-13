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
import com.linkedin.thirdeye.impl.StarTreeUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class StarTreeBootstrapJobAvro extends Configured
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeBootstrapJobAvro.class);

  public static final String PROP_AVRO_SCHEMA = "avro.schema";
  public static final String PROP_STARTREE_CONFIG = "startree.config";
  public static final String PROP_STARTREE_ROOT = "startree.root";
  public static final String PROP_INPUT_PATHS = "input.paths";
  public static final String PROP_OUTPUT_PATH = "output.path";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String name;
  private final Properties props;

  public StarTreeBootstrapJobAvro(String name, Properties props)
  {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class StarTreeBootstrapAvroMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, AvroValue<GenericRecord>>
  {
    private final Text nodeId = new Text();
    private final AvroValue<GenericRecord> outputRecord = new AvroValue<GenericRecord>();

    private StarTree starTree;
    private Schema schema;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      FileSystem fileSystem = FileSystem.get(context.getConfiguration());
      Path rootPath = new Path(context.getConfiguration().get(PROP_STARTREE_ROOT));
      Path configPath = new Path(context.getConfiguration().get(PROP_STARTREE_CONFIG));

      // n.b. this is to make StarTreeConfig happy - we never open the tree, so this is unused
      // TODO: We could just use StarTreeNode to travers and avoid messy StarTree stuff, but later
      File outputFile = new File(context.getConfiguration().get(PROP_OUTPUT_PATH));

      try
      {
        StarTreeConfig config = StarTreeConfig.fromJson(OBJECT_MAPPER.readTree(fileSystem.open(configPath)), outputFile);
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
    public void map(AvroKey<GenericRecord> record, NullWritable value, Context context) throws IOException, InterruptedException
    {
      // Note schema
      if (schema == null)
      {
        schema = record.datum().getSchema();
      }

      // Collect specific / star records from tree that match
      Map<UUID, StarTreeRecord> collector = new HashMap<UUID, StarTreeRecord>();
      collectRecords(starTree.getRoot(), toStarTreeRecord(starTree.getConfig(), record.datum()), collector);

      // Output them
      for (Map.Entry<UUID, StarTreeRecord> entry : collector.entrySet())
      {
        nodeId.set(entry.getKey().toString());
        outputRecord.datum(toGenericRecord(starTree.getConfig(), schema, entry.getValue(), outputRecord.datum()));
        context.write(nodeId, outputRecord);
      }
    }
  }

  public static class StarTreeBootstrapAvroReducer extends Reducer<Text, AvroValue<GenericRecord>, NullWritable, NullWritable>
  {
    private StarTreeConfig config;
    private int numTimeBuckets;
    private Path outputPath;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      try
      {
        // n.b. this is to make StarTreeConfig happy - we never open the tree, so this is unused
        // TODO: We could just use StarTreeNode to travers and avoid messy StarTree stuff, but later
        File outputFile = new File(context.getConfiguration().get(PROP_OUTPUT_PATH));

        Path configPath = new Path(context.getConfiguration().get(PROP_STARTREE_CONFIG));
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        config = StarTreeConfig.fromJson(OBJECT_MAPPER.readTree(fileSystem.open(configPath)), outputFile);
        numTimeBuckets = Integer.valueOf(config.getRecordStoreFactory().getConfig().getProperty("numTimeBuckets"));
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
      Map<String, Map<Long, StarTreeRecord>> records = aggregateRecords(config, avroRecords, numTimeBuckets);

      // Get all merged records
      List<StarTreeRecord> mergedRecords = new ArrayList<StarTreeRecord>();
      for (Map<Long, StarTreeRecord> timeBucket : records.values())
      {
        for (Map.Entry<Long, StarTreeRecord> entry : timeBucket.entrySet())
        {
          mergedRecords.add(entry.getValue());
        }
      }

      // Build other value
      StarTreeRecordImpl.Builder otherRecord = new StarTreeRecordImpl.Builder();
      for (String dimensionName : config.getDimensionNames())
      {
        otherRecord.setDimensionValue(dimensionName, StarTreeConstants.OTHER);
      }
      for (String metricName : config.getMetricNames())
      {
        otherRecord.setMetricValue(metricName, 0L);
      }
      otherRecord.setTime(0L);
      mergedRecords.add(otherRecord.build());

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
      Path bufferPath = new Path(outputPath, nodeId + StarTreeRecordStoreFactoryCircularBufferHdfsImpl.BUFFER_SUFFIX);
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
      Path indexPath = new Path(outputPath, nodeId.toString() + StarTreeRecordStoreFactoryCircularBufferHdfsImpl.INDEX_SUFFIX);
      outputStream = FileSystem.get(context.getConfiguration()).create(indexPath, true);
      OBJECT_MAPPER.writeValue(outputStream, forwardIndex);
      outputStream.flush();
      outputStream.close();
    }
  }

  /**
   * Converts a GenericRecord to a StarTreeRecord
   */
  private static StarTreeRecord toStarTreeRecord(StarTreeConfig config, GenericRecord record)
  {
    StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();

    // Dimensions
    for (String dimensionName : config.getDimensionNames())
    {
      Object dimensionValue = record.get(dimensionName);
      if (dimensionValue == null)
      {
        throw new IllegalStateException("Record has no value for dimension " + dimensionName);
      }
      builder.setDimensionValue(dimensionName, dimensionValue.toString());
    }

    // Metrics (n.b. null -> 0L)
    for (String metricName : config.getMetricNames())
    {
      Object metricValue = record.get(metricName);
      if (metricValue == null)
      {
        metricValue = 0L;
      }
      builder.setMetricValue(metricName, ((Number) metricValue).longValue());
    }

    // Time
    Object time = record.get(config.getTimeColumnName());
    if (time == null)
    {
      throw new IllegalStateException("Record does not have time column " + config.getTimeColumnName() + ": " + record);
    }
    builder.setTime(((Number) time).longValue());

    return builder.build();
  }

  /**
   * Converts a StarTreeRecord to GenericRecord
   */
  private static GenericRecord toGenericRecord(StarTreeConfig config, Schema schema, StarTreeRecord record, GenericRecord reuse)
  {
    GenericRecord genericRecord;
    if (reuse != null)
    {
      genericRecord = reuse;
    }
    else
    {
      genericRecord = new GenericData.Record(schema);
    }

    // Dimensions
    for (Map.Entry<String, String> dimension : record.getDimensionValues().entrySet())
    {
      switch (getType(schema.getField(dimension.getKey()).schema()))
      {
        case INT:
          genericRecord.put(dimension.getKey(), Integer.valueOf(dimension.getValue()));
          break;
        case LONG:
          genericRecord.put(dimension.getKey(), Long.valueOf(dimension.getValue()));
          break;
        case FLOAT:
          genericRecord.put(dimension.getKey(), Float.valueOf(dimension.getValue()));
          break;
        case DOUBLE:
          genericRecord.put(dimension.getKey(), Double.valueOf(dimension.getValue()));
          break;
        case BOOLEAN:
          genericRecord.put(dimension.getKey(), Boolean.valueOf(dimension.getValue()));
          break;
        case STRING:
          genericRecord.put(dimension.getKey(), dimension.getValue());
          break;
        default:
          throw new IllegalStateException("Unsupported dimension type " + schema.getField(dimension.getKey()));
      }
    }

    // Metrics
    for (Map.Entry<String, Long> metric : record.getMetricValues().entrySet())
    {
      switch (getType(schema.getField(metric.getKey()).schema()))
      {
        case INT:
          genericRecord.put(metric.getKey(), metric.getValue().intValue());
          break;
        case LONG:
          genericRecord.put(metric.getKey(), metric.getValue());
          break;
        case FLOAT:
          genericRecord.put(metric.getKey(), metric.getValue().floatValue());
          break;
        case DOUBLE:
          genericRecord.put(metric.getKey(), metric.getValue().doubleValue());
          break;
        default:
          throw new IllegalStateException("Invalid metric schema type: " + schema.getField(metric.getKey()));
      }
    }

    // Time
    switch (getType(schema.getField(config.getTimeColumnName()).schema()))
    {
      case INT:
        genericRecord.put(config.getTimeColumnName(), record.getTime().intValue());
        break;
      case LONG:
        genericRecord.put(config.getTimeColumnName(), record.getTime());
        break;
      default:
        throw new IllegalStateException("Invalid time schema type: " + schema.getField(config.getTimeColumnName()));
    }

    // (Assume values we didn't touch are time, and fill in w/ 0, as these will be unused)
    for (Schema.Field field : schema.getFields())
    {
      if (!record.getDimensionValues().containsKey(field.name())
              && !record.getMetricValues().containsKey(field.name()))
      {
        switch (getType(field.schema()))
        {
          case INT:
            genericRecord.put(field.name(), 0);
            break;
          case LONG:
            genericRecord.put(field.name(), 0L);
            break;
          default:
            throw new IllegalStateException("Invalid time schema type: " + field.schema().getType());
        }
      }
    }

    return genericRecord;
  }

  /**
   * Returns the type of a schema, handling ["null", {type}]-style optional fields.
   */
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

  private static Map<String, Map<Long, StarTreeRecord>> aggregateRecords(
          StarTreeConfig config,
          Iterable<AvroValue<GenericRecord>> avroRecords,
          int numTimeBuckets)
  {
    Map<String, Map<Long, StarTreeRecord>> records = new HashMap<String, Map<Long, StarTreeRecord>>();

    // Aggregate records
    for (AvroValue<GenericRecord> avroRecord : avroRecords)
    {
      StarTreeRecord record = toStarTreeRecord(config, avroRecord.datum());

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

    return records;
  }

  /**
   * Traverses tree structure and collects all combinations of record that are present (star/specific)
   */
  private static void collectRecords(StarTreeNode node, StarTreeRecord record, Map<UUID, StarTreeRecord> collector)
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

  public void run() throws Exception
  {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeBootstrapJobTsv.class);

    // Avro schema
    Schema schema = new Schema.Parser().parse(FileSystem.get(getConf()).open(new Path(getAndCheck(PROP_AVRO_SCHEMA))));
    LOG.info("{}", schema);

    // Map config
    job.setMapperClass(StarTreeBootstrapAvroMapper.class);
    AvroJob.setInputKeySchema(job, schema);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(AvroValue.class);
    AvroJob.setMapOutputValueSchema(job, schema);

    // Reduce config
    job.setReducerClass(StarTreeBootstrapAvroReducer.class);
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

    StarTreeBootstrapJobAvro bootstrapJob = new StarTreeBootstrapJobAvro("star_tree_bootstrap_avro_job", props);
    bootstrapJob.run();
  }
}
