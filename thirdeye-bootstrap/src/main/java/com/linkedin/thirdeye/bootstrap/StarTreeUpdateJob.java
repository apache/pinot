package com.linkedin.thirdeye.bootstrap;

import static com.linkedin.thirdeye.bootstrap.StarTreeJobConstants.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * MR job to generate set of Avro files for each leaf node, which can be directly loaded.
 */
public class StarTreeUpdateJob extends Configured
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeUpdateJob.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final String name;
  private final Properties props;

  public StarTreeUpdateJob(String name, Properties props)
  {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class StarTreeUpdateMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, Text, AvroValue<GenericRecord>>
  {
    private final Text nodeId = new Text();
    private final AvroValue<GenericRecord> outputRecord = new AvroValue<GenericRecord>();

    private StarTree starTree;
    private Schema schema;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      FileSystem fileSystem = FileSystem.get(context.getConfiguration());
      File outputFile = new File(context.getConfiguration().get(PROP_OUTPUT_PATH));
      Path rootPath = new Path(context.getConfiguration().get(PROP_STARTREE_ROOT));
      Path configPath = new Path(context.getConfiguration().get(PROP_STARTREE_CONFIG));

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

      // Output them
      for (Map.Entry<UUID, StarTreeRecord> entry : collector.entrySet())
      {
        nodeId.set(entry.getKey().toString());
        outputRecord.datum(StarTreeJobUtils.toGenericRecord(starTree.getConfig(), schema, entry.getValue(), outputRecord.datum()));
        context.write(nodeId, outputRecord);
      }
    }
  }

  public static class StarTreeUpdateReducer extends Reducer<Text, AvroValue<GenericRecord>, NullWritable, NullWritable>
  {
    private StarTreeConfig config;
    private Path outputPath;
    private Schema schema;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      try
      {
        File outputFile = new File(context.getConfiguration().get(PROP_OUTPUT_PATH));
        Path configPath = new Path(context.getConfiguration().get(PROP_STARTREE_CONFIG));
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        config = StarTreeConfig.fromJson(OBJECT_MAPPER.readTree(fileSystem.open(configPath)), outputFile);
        schema = new Schema.Parser().parse(FileSystem.get(context.getConfiguration()).open(new Path(context.getConfiguration().get(PROP_AVRO_SCHEMA))));
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
      // Create an Avro data file in output directory {nodeId}.avro
      Path avroFile = new Path(outputPath, nodeId + AVRO_FILE_SUFFIX);
      LOG.info("Writing {}", avroFile);

      // Write all values to that
      DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
      DataFileWriter<GenericRecord> fileWriter = new DataFileWriter<GenericRecord>(datumWriter);
      fileWriter.create(schema, FileSystem.get(context.getConfiguration()).create(avroFile));
      for (AvroValue<GenericRecord> avroRecord : avroRecords)
      {
        fileWriter.append(avroRecord.datum());
      }
      fileWriter.flush();
      fileWriter.close();
    }
  }

  public void run() throws Exception
  {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeUpdateJob.class);

    // Avro schema
    Schema schema = new Schema.Parser().parse(FileSystem.get(getConf()).open(new Path(getAndCheck(PROP_AVRO_SCHEMA))));
    LOG.info("{}", schema);

    // Map config
    job.setMapperClass(StarTreeUpdateMapper.class);
    AvroJob.setInputKeySchema(job, schema);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(AvroValue.class);
    AvroJob.setMapOutputValueSchema(job, schema);

    // Reduce config
    job.setReducerClass(StarTreeUpdateReducer.class);
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

    StarTreeUpdateJob updateJob = new StarTreeUpdateJob("star_tree_update_avro_job", props);
    updateJob.run();
  }
}
