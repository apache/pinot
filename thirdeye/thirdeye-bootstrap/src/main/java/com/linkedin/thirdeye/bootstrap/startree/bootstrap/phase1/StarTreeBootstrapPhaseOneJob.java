package com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1;

import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_INPUT_AVRO_SCHEMA;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_OUTPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneConstants.STAR_TREE_GENERATION_OUTPUT_PATH;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.api.*;
import com.linkedin.thirdeye.bootstrap.util.ThirdEyeAvroUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.bootstrap.startree.StarTreeJobUtils;
import com.linkedin.thirdeye.bootstrap.util.TarGzCompressionUtils;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;

/**
 * @author kgopalak <br/>
 *         INPUT: RAW DATA FILES. <br/>
 *         EACH RECORD OF THE FORMAT {DIMENSION, TIME, RECORD} <br/>
 *         MAP OUTPUT: {DIMENSION KEY, TIME, METRIC} <br/>
 *         REDUCE OUTPUT: DIMENSION KEY: SET{TIME_BUCKET, METRIC}
 */
public class StarTreeBootstrapPhaseOneJob extends Configured
{
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeBootstrapPhaseOneJob.class);

  private String name;
  private Properties props;

  public StarTreeBootstrapPhaseOneJob(String name, Properties props)
  {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class BootstrapMapper extends
      Mapper<AvroKey<GenericRecord>, NullWritable, BytesWritable, BytesWritable>
  {
    private StarTreeConfig starTreeConfig;
    private StarTreeBootstrapPhaseOneConfig config;
    private TimeUnit sourceTimeUnit;
    private TimeUnit aggregationTimeUnit;
    private int aggregationGranularitySize;
    private int inputTimeUnitSize;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    private String[] dimensionValues;
    private StarTreeNode starTreeRootNode;
    private String collectionName;
    private Map<UUID, StarTreeNode> leafNodesMap;
    private String localStagingDir = "./staging";
    int inputCount = 0;
    int outputCount = 0;
    long totalTime = 0;
    Map<UUID, Map<String, Map<String, Integer>>> forwardIndexMap;
    Map<UUID, List<int[]>> nodeIdToleafRecordsMap;
    boolean debug = false;

    private DateTimeFormatter dateTimeFormatter;

    private Map<BootstrapPhaseMapOutputKey, BootstrapPhaseMapOutputValue> outputKeyValueCache =
        new HashMap<BootstrapPhaseMapOutputKey, BootstrapPhaseMapOutputValue>();
    int totalDimensionKeys = 0;
    int totalTimeSeriesSize = 0;
    Map<UUID, StarTreeRecord> collector = new HashMap<UUID, StarTreeRecord>();

    int maxTimeSeriesToCache;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      LOGGER.info("StarTreeBootstrapPhaseOneJob.BootstrapMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem dfs = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(STAR_TREE_BOOTSTRAP_CONFIG_PATH.toString()));
      try
      {
        starTreeConfig = StarTreeConfig.decode(dfs.open(configPath));
        config = StarTreeBootstrapPhaseOneConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        sourceTimeUnit = TimeUnit.valueOf(config.getTimeUnit());
        inputTimeUnitSize = config.getInputTimeUnitSize();
        aggregationTimeUnit = TimeUnit.valueOf(config.getAggregationGranularity());
        aggregationGranularitySize = config.getAggregationGranularitySize();
        dimensionValues = new String[dimensionNames.size()];

        // how many can we store in 0.05GB, before clearing the cache
        maxTimeSeriesToCache =
            (int) (0.05 * 1000 * 1000 * 1000) / (8 + metricSchema.getRowSizeInBytes());
        LOGGER.info("MAX_TIMESERIES_IN_CACHE=" + maxTimeSeriesToCache);
        if (starTreeConfig.getTime().getFormat() != null)
        {
          // Check that input time unit / size is 1 MILLISECONDS
          if (!(inputTimeUnitSize == 1 && TimeUnit.MILLISECONDS.equals(sourceTimeUnit)))
          {
            throw new IllegalArgumentException(
                "Input time granularity must be 1 MILLISECONDS if format is provided: "
                    + "granularity is " + inputTimeUnitSize + " " + sourceTimeUnit + " and "
                    + "format is " + starTreeConfig.getTime().getFormat());
          }
          dateTimeFormatter = DateTimeFormat.forPattern(starTreeConfig.getTime().getFormat());
        }
      } catch (Exception e)
      {
        throw new IOException(e);
      }
      // copy the tree locally and load it
      String starTreeOutputPath =
          context.getConfiguration().get(STAR_TREE_GENERATION_OUTPUT_PATH.toString());
      try
      {

        collectionName = config.getCollectionName();
        Path pathToTree = new Path(starTreeOutputPath + "/" + "tree.bin");
        InputStream is = dfs.open(pathToTree);
        starTreeRootNode = StarTreePersistanceUtil.loadStarTree(is);

      } catch (Exception e)
      {
        throw new IOException(e);
      }

      // copy the dimension index

      // copy the dimension index tar gz locally and untar it
      try
      {

        collectionName = config.getCollectionName();
        String tarGZName = "dimensionStore.tar.gz";
        Path pathToDimensionIndexTarGz = new Path(starTreeOutputPath, tarGZName);
        dfs.copyToLocalFile(pathToDimensionIndexTarGz, new Path(tarGZName));
        new File(localStagingDir).mkdirs();
        TarGzCompressionUtils.unTar(new File(tarGZName), new File(localStagingDir));
        Collection<File> listFiles =
            FileUtils.listFiles(new File("."), FileFileFilter.FILE, DirectoryFileFilter.DIRECTORY);
        boolean b = false;
        for (File f : listFiles)
        {
          LOGGER.info(f.getAbsolutePath());
        }
      } catch (Exception e)
      {
        throw new IOException(e);
      }

      // load the leaf record dimensions into memory. This assumes that the tree
      // + record dimensions can fit in memory
      // TODO: in the previous star tree gen step, we will have to break up the
      // tree into sub parts.
      LinkedList<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
      StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTreeRootNode);
      LOGGER.info("Num leaf Nodes in star tree:" + leafNodes.size());
      leafNodesMap = new HashMap<UUID, StarTreeNode>();
      forwardIndexMap = new HashMap<UUID, Map<String, Map<String, Integer>>>();
      nodeIdToleafRecordsMap = new HashMap<UUID, List<int[]>>();
      for (StarTreeNode node : leafNodes)
      {
        UUID uuid = node.getId();
        Map<String, Map<String, Integer>> forwardIndex =
            StarTreePersistanceUtil.readForwardIndex(uuid.toString(), localStagingDir
                + "/dimensionStore");
        List<int[]> leafRecords =
            StarTreePersistanceUtil.readLeafRecords(localStagingDir + "/dimensionStore",
                uuid.toString(), dimensionNames.size());
        leafNodesMap.put(uuid, node);
        forwardIndexMap.put(uuid, forwardIndex);
        nodeIdToleafRecordsMap.put(uuid, leafRecords);
      }

    }

    @Override
    public void map(AvroKey<GenericRecord> record, NullWritable value, Context context)
        throws IOException, InterruptedException
    {
      long start = System.currentTimeMillis();

      StarTreeRecord parsedRecord = ThirdEyeAvroUtils.convert(starTreeConfig, record.datum());
      if (parsedRecord == null)
      {
        context.getCounter(StarTreeBootstrapPhase1Counter.INVALID_TIME_RECORDS).increment(1);
        return;
      }

      // Count metric values
      for (Long time : parsedRecord.getMetricTimeSeries().getTimeWindowSet())
      {
        for (MetricSpec metricSpec : starTreeConfig.getMetrics())
        {
          Number metricValue = parsedRecord.getMetricTimeSeries().get(time, metricSpec.getName());
          context.getCounter(config.getCollectionName(), metricSpec.getName()).increment(
              metricValue.longValue());
        }
      }

      // Collect specific / star records from tree that match
      collector.clear();
      MetricTimeSeries emptyTimeSeries = new MetricTimeSeries(metricSchema);
      StarTreeRecord starTreeRecord =
          new StarTreeRecordImpl(starTreeConfig, parsedRecord.getDimensionKey(), emptyTimeSeries);
      StarTreeJobUtils.collectRecords(starTreeConfig, starTreeRootNode, starTreeRecord, collector);
      LOGGER.debug("processing {}", parsedRecord.getDimensionKey());
      LOGGER.debug("times series {}", parsedRecord.getMetricTimeSeries());
      for (UUID uuid : collector.keySet())
      {
        if (!leafNodesMap.containsKey(uuid))
        {
          String msg =
              "Got a mapping to non existant leaf node:" + uuid + " - " + collector.get(uuid)
                  + " input :" + starTreeRecord;
          LOGGER.error(msg);
          throw new RuntimeException(msg);
        }
        List<int[]> leafRecords = nodeIdToleafRecordsMap.get(uuid);
        Map<String, Map<String, Integer>> forwardIndex = forwardIndexMap.get(uuid);
        Map<String, Map<Integer, String>> reverseForwardIndex =
            StarTreeUtils.toReverseIndex(forwardIndex);
        int[] bestMatch =
            StarTreeJobUtils.findBestMatch(parsedRecord.getDimensionKey(), dimensionNames,
                leafRecords, forwardIndex);
        String[] dimValues =
            StarTreeUtils.convertToStringValue(reverseForwardIndex, bestMatch, dimensionNames);
        DimensionKey matchedDimensionKey = new DimensionKey(dimValues);
        if (debug)
        {
          LOGGER.info("Match: {} under {}", matchedDimensionKey, leafNodesMap.get(uuid).getPath());
        }
        BootstrapPhaseMapOutputKey outputKey =
            new BootstrapPhaseMapOutputKey(uuid, matchedDimensionKey.toMD5());
        BootstrapPhaseMapOutputValue bootstrapPhaseMapOutputValue =
            outputKeyValueCache.get(outputKey);
        if (bootstrapPhaseMapOutputValue == null)
        {
          MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);
          timeSeries.aggregate(parsedRecord.getMetricTimeSeries());
          BootstrapPhaseMapOutputValue outputValue =
              new BootstrapPhaseMapOutputValue(matchedDimensionKey, timeSeries);
          outputKeyValueCache.put(outputKey, outputValue);
          totalDimensionKeys = totalDimensionKeys + 1;
          totalTimeSeriesSize =
              totalTimeSeriesSize + parsedRecord.getMetricTimeSeries().getTimeWindowSet().size();
        } else
        {
          int oldSize = bootstrapPhaseMapOutputValue.metricTimeSeries.getTimeWindowSet().size();
          bootstrapPhaseMapOutputValue.metricTimeSeries.aggregate(parsedRecord
              .getMetricTimeSeries());
          int newSize = bootstrapPhaseMapOutputValue.metricTimeSeries.getTimeWindowSet().size();
          totalTimeSeriesSize = totalTimeSeriesSize + (newSize - oldSize);
        }
      }
      // clear cache if there is enough data accumulated
      if (totalTimeSeriesSize > maxTimeSeriesToCache)
      {
        flushCache(context);
        outputKeyValueCache.clear();
        totalDimensionKeys = 0;
        totalTimeSeriesSize = 0;
      }

      long end = System.currentTimeMillis();
      inputCount = inputCount + 1;
      outputCount = outputCount + collector.size();
      totalTime = totalTime + (end - start);
      if (inputCount % 5000 == 0)
      {
        LOGGER.info("Processed {} in {}. avg time {} avg Fan out:{}", inputCount, totalTime,
            totalTime / inputCount, (outputCount / inputCount));
      }
    }

    private void flushCache(Context context) throws IOException, InterruptedException
    {
      LOGGER.info("Flushing cache");
      for (BootstrapPhaseMapOutputKey outputKey : outputKeyValueCache.keySet())
      {
        BytesWritable keyWritable = new BytesWritable(outputKey.toBytes());
        BootstrapPhaseMapOutputValue outputValue = outputKeyValueCache.get(outputKey);
        BytesWritable valWritable = new BytesWritable(outputValue.toBytes());
        context.write(keyWritable, valWritable);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
      flushCache(context);
      File f = new File(localStagingDir);
      FileUtils.deleteDirectory(f);
      new File("leaf-data.tar.gz").delete();
    }
  }

  public static class StarTreeBootstrapReducer extends
      Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable>
  {
    private StarTreeBootstrapPhaseOneConfig config;
    private List<MetricType> metricTypes;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private MetricSchema metricSchema;
    private boolean debug = false;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      Configuration configuration = context.getConfiguration();
      FileSystem dfs = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(STAR_TREE_BOOTSTRAP_CONFIG_PATH.toString()));
      try
      {
        StarTreeConfig starTreeConfig = StarTreeConfig.decode(dfs.open(configPath));
        config = StarTreeBootstrapPhaseOneConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
      } catch (Exception e)
      {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(BytesWritable bootstrapMapOutputKeyWritable,
        Iterable<BytesWritable> bootstrapMapOutputValueWritableIterable, Context context)
        throws IOException, InterruptedException
    {

      MetricTimeSeries aggregateSeries = new MetricTimeSeries(metricSchema);
      DimensionKey key = null;
      for (BytesWritable writable : bootstrapMapOutputValueWritableIterable)
      {
        BootstrapPhaseMapOutputValue mapOutputValue =
            BootstrapPhaseMapOutputValue.fromBytes(writable.copyBytes(), metricSchema);
        if (key == null)
        {
          key = mapOutputValue.dimensionKey;
        }
        aggregateSeries.aggregate(mapOutputValue.metricTimeSeries);
      }
      BootstrapPhaseMapOutputValue mapOutputValue =
          new BootstrapPhaseMapOutputValue(key, aggregateSeries);
      BytesWritable valWritable = new BytesWritable(mapOutputValue.toBytes());
      if (debug)
      {
        LOGGER.info("Processed {}", key);
        LOGGER.info("time series: {}", aggregateSeries);
      }
      context.write(bootstrapMapOutputKeyWritable, valWritable);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {

    }
  }

  public static class NodeIdBasedPartitioner extends Partitioner<BytesWritable, BytesWritable>
  {

    @Override
    public int getPartition(BytesWritable keyWritable, BytesWritable value, int numReducers)
    {
      BootstrapPhaseMapOutputKey key;
      try
      {
        key = BootstrapPhaseMapOutputKey.fromBytes(keyWritable.copyBytes());
        return (key.nodeId.hashCode() & Integer.MAX_VALUE) % numReducers;
      } catch (IOException e)
      {
        LOGGER.error("Error computing partition Id", e);
      }
      return 0;
    }

  }

  public Job run() throws Exception
  {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeBootstrapPhaseOneJob.class);

    // Avro schema
    FileSystem fs = FileSystem.get(getConf());
    Schema schema =
        new Schema.Parser().parse(fs.open(new Path(
            getAndCheck(STAR_TREE_BOOTSTRAP_INPUT_AVRO_SCHEMA.toString()))));
    LOGGER.info("{}", schema);

    // Map config
    job.setMapperClass(BootstrapMapper.class);
    AvroJob.setInputKeySchema(job, schema);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    job.setPartitionerClass(NodeIdBasedPartitioner.class);
    // Reduce config
    // job.setCombinerClass(StarTreeBootstrapReducer.class);
    job.setReducerClass(StarTreeBootstrapReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    String numReducers = props.getProperty("num.reducers");
    if (numReducers != null)
    {
      job.setNumReduceTasks(Integer.parseInt(numReducers));
    } else
    {
      job.setNumReduceTasks(10);
    }
    LOGGER.info("Setting number of reducers : " + job.getNumReduceTasks());

    // star tree bootstrap phase phase config
    Configuration configuration = job.getConfiguration();
    String inputPathDir = getAndSetConfiguration(configuration, STAR_TREE_BOOTSTRAP_INPUT_PATH);
    getAndSetConfiguration(configuration, STAR_TREE_GENERATION_OUTPUT_PATH);
    getAndSetConfiguration(configuration, STAR_TREE_BOOTSTRAP_CONFIG_PATH);
    getAndSetConfiguration(configuration, STAR_TREE_BOOTSTRAP_OUTPUT_PATH);
    getAndSetConfiguration(configuration, STAR_TREE_BOOTSTRAP_INPUT_AVRO_SCHEMA);
    LOGGER.info("Input path dir: " + inputPathDir);
    FileInputFormat.setInputDirRecursive(job, true);
    for (String inputPath : inputPathDir.split(","))
    {
      Path input = new Path(inputPath);
      FileStatus[] listFiles = fs.listStatus(input);
      boolean isNested = false;
      for (FileStatus fileStatus : listFiles)
      {
        if (fileStatus.isDirectory())
        {
          isNested = true;
          LOGGER.info("Adding input:" + fileStatus.getPath());
          FileInputFormat.addInputPath(job, fileStatus.getPath());
        }
      }
      if (!isNested)
      {
        LOGGER.info("Adding input:" + inputPath);
        FileInputFormat.addInputPath(job, input);
      }
    }

    FileOutputFormat.setOutputPath(job,
        new Path(getAndCheck(STAR_TREE_BOOTSTRAP_OUTPUT_PATH.toString())));

    job.waitForCompletion(true);

    Counters counters = job.getCounters();
    for (Enum e : StarTreeBootstrapPhase1Counter.values())
    {
      Counter counter = counters.findCounter(e);
      LOGGER.info(counter.getDisplayName() + " : " + counter.getValue());
    }

    Path configPath = new Path(configuration.get(STAR_TREE_BOOTSTRAP_CONFIG_PATH.toString()));
    StarTreeConfig starTreeConfig = StarTreeConfig.decode(fs.open(configPath));
    StarTreeBootstrapPhaseOneConfig config =
        StarTreeBootstrapPhaseOneConfig.fromStarTreeConfig(starTreeConfig);
    for (String metricName : config.getMetricNames())
    {
      Counter counter = counters.findCounter(config.getCollectionName(), metricName);
      LOGGER.info(counter.getDisplayName() + " : " + counter.getValue());
    }
    return job;

  }

  private String getAndSetConfiguration(Configuration configuration,
      StarTreeBootstrapPhaseOneConstants constant)
  {
    String value = getAndCheck(constant.toString());
    configuration.set(constant.toString(), value);
    return value;
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

    StarTreeBootstrapPhaseOneJob job =
        new StarTreeBootstrapPhaseOneJob("star_tree_bootstrap_job", props);
    job.run();
  }

  public static enum StarTreeBootstrapPhase1Counter
  {
    INVALID_TIME_RECORDS
  }
}
