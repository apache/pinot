package com.linkedin.thirdeye.bootstrap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StarTreeBootstrapJob extends Configured
{
  public static final String PROP_STARTREE_COLLECTION = "startree.collection";
  public static final String PROP_STARTREE_CONFIG = "startree.config";
  public static final String PROP_STARTREE_DATA = "startree.data";
  public static final String PROP_STARTREE_ROOT = "startree.root";
  public static final String PROP_INPUT_PATHS = "input.paths";
  public static final String PROP_OUTPUT_PATH = "output.path";

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final TypeReference TYPE_REFERENCE = new TypeReference<Map<String, Map<String, Integer>>>(){};

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

    private String collection;
    private ExecutorService executorService;
    private StarTreeManager starTreeManager;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      collection = context.getConfiguration().get(PROP_STARTREE_COLLECTION);

      FileSystem fileSystem = FileSystem.get(context.getConfiguration());
      executorService = Executors.newSingleThreadExecutor();
      starTreeManager = new StarTreeManagerImpl(executorService);

      Path rootPath = new Path(context.getConfiguration().get(PROP_STARTREE_ROOT));
      Path configPath = new Path(context.getConfiguration().get(PROP_STARTREE_CONFIG));

      try
      {
        starTreeManager.restore(collection,
                                fileSystem.open(rootPath),
                                fileSystem.open(configPath));
      }
      catch (Exception e)
      {
        throw new IOException(e);
      }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      StarTree starTree = starTreeManager.getStarTree(collection);

      int idx = 0;
      String[] tokens = value.toString().split("\t");

      // TSV -> query
      StarTreeQueryImpl.Builder query = new StarTreeQueryImpl.Builder();
      for (String dimensionName : starTree.getConfig().getDimensionNames())
      {
        query.setDimensionValue(dimensionName, tokens[idx++]);
      }
      query.setTimeBuckets(new HashSet<Long>(Arrays.asList(Long.valueOf(tokens[idx]))));

      // Get node ID
      StarTreeNode node = starTree.search(query.build());
      nodeId.set(node.getId().toString());

      // Output ID -> entry mapping
      context.write(nodeId, value);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
      if (executorService != null)
      {
        executorService.shutdown();
      }
    }
  }

  public static class StarTreeBootstrapReducer extends Reducer<Text, Text, NullWritable, Text>
  {
    private final Text bufferWrapper = new Text();

    private StarTreeConfig config;
    private int numTimeBuckets;
    private MultipleOutputs<NullWritable, Text> multipleOutputs;

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

      multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
    }

    @Override
    public void reduce(Text nodeId, Iterable<Text> tsvRecords, Context context) throws IOException, InterruptedException
    {
      // Group records by combination then time
      Map<Map<String, String>, Map<Long, List<StarTreeRecord>>> groupedRecords
              = new HashMap<Map<String, String>, Map<Long, List<StarTreeRecord>>>();
      for (Text tsvRecord : tsvRecords)
      {
        // Convert to star tree record
        int idx = 0;
        String[] tokens = tsvRecord.toString().split("\t");
        StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();
        for (String dimensionName : config.getDimensionNames())
        {
          builder.setDimensionValue(dimensionName, tokens[idx++]);
        }
        for (String metricName : config.getMetricNames())
        {
          builder.setMetricValue(metricName, Long.valueOf(tokens[idx++]));
        }
        builder.setTime(Long.valueOf(tokens[idx]));
        StarTreeRecord record = builder.build();

        // Add to appropriate bucket
        Map<Long, List<StarTreeRecord>> timeBucket = groupedRecords.get(record.getDimensionValues());
        if (timeBucket == null)
        {
          timeBucket = new HashMap<Long, List<StarTreeRecord>>();
          groupedRecords.put(record.getDimensionValues(), timeBucket);
        }
        List<StarTreeRecord> records = timeBucket.get(record.getTime());
        if (records == null)
        {
          records = new ArrayList<StarTreeRecord>();
          timeBucket.put(record.getTime(), records);
        }
        records.add(record);
      }

      // Merge the records for the latest time bucket
      List<StarTreeRecord> mergedRecords = new ArrayList<StarTreeRecord>();
      for (Map.Entry<Map<String, String>, Map<Long, List<StarTreeRecord>>> e1 : groupedRecords.entrySet())
      {
        Map<String, String> combination = e1.getKey();
        Map<Long, List<StarTreeRecord>> timeGroups = e1.getValue();

        Long latestTime = null;
        List<StarTreeRecord> latestRecords = null;

        for (Map.Entry<Long, List<StarTreeRecord>> e2 : e1.getValue().entrySet())
        {
          if (latestTime == null || e2.getKey() > latestTime)
          {
            latestTime = e2.getKey();
            latestRecords = e2.getValue();
          }
        }

        if (latestRecords == null)
        {
          throw new IllegalStateException("Could not find latest records for combination " + combination);
        }

        mergedRecords.add(StarTreeUtils.merge(latestRecords));
      }

      // Get forward index for node
      Path indexPath = new Path(
              context.getConfiguration().get(PROP_STARTREE_DATA),
                      nodeId.toString() +
                      StarTreeRecordStoreFactoryCircularBufferHdfsImpl.INDEX_SUFFIX);
      Map<String, Map<String, Integer>> forwardIndex
              = OBJECT_MAPPER.readValue(FileSystem.get(context.getConfiguration()).open(indexPath), TYPE_REFERENCE);

      // Create a new buffer with those records
      int bufferSize = mergedRecords.size() * // number of records in the store
              (config.getDimensionNames().size() * Integer.SIZE / 8 // the dimension part
                      + (config.getMetricNames().size() + 1) * numTimeBuckets * Long.SIZE / 8); // metric + time
      ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
      StarTreeRecordStoreCircularBufferImpl.fillBuffer(
              buffer,
              config.getDimensionNames(),
              config.getMetricNames(),
              forwardIndex,
              mergedRecords,
              numTimeBuckets,
              true);

      // Write that buffer to file
      String fileName = nodeId.toString() + StarTreeRecordStoreFactoryCircularBufferHdfsImpl.BUFFER_SUFFIX;
      bufferWrapper.set(buffer.array()); // okay, using heap buffer
      multipleOutputs.write(NullWritable.get(), bufferWrapper, fileName);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
      multipleOutputs.close();
    }
  }

  public void run() throws Exception
  {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeBootstrapJob.class);

    job.setMapperClass(StarTreeBootstrapMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(StarTreeBootstrapReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    job.getConfiguration().set(PROP_STARTREE_COLLECTION, getAndCheck(PROP_STARTREE_COLLECTION));
    job.getConfiguration().set(PROP_STARTREE_CONFIG, getAndCheck(PROP_STARTREE_CONFIG));
    job.getConfiguration().set(PROP_STARTREE_DATA, getAndCheck(PROP_STARTREE_DATA));
    job.getConfiguration().set(PROP_STARTREE_ROOT, getAndCheck(PROP_STARTREE_ROOT));

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
