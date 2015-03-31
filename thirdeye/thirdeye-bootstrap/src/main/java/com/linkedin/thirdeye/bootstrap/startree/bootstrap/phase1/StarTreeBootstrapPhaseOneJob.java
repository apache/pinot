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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.bootstrap.startree.StarTreeJobUtils;
import com.linkedin.thirdeye.bootstrap.util.TarGzCompressionUtils;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;

/**
 *
 * @author kgopalak <br/>
 *
 *         INPUT: RAW DATA FILES. <br/>
 *         EACH RECORD OF THE FORMAT {DIMENSION, TIME, RECORD} <br/>
 *         MAP OUTPUT: {DIMENSION KEY, TIME, METRIC} <br/>
 *         REDUCE OUTPUT: DIMENSION KEY: SET{TIME_BUCKET, METRIC}
 */
public class StarTreeBootstrapPhaseOneJob extends Configured {
  private static final Logger LOG = LoggerFactory
      .getLogger(StarTreeBootstrapPhaseOneJob.class);

  private String name;
  private Properties props;

  public StarTreeBootstrapPhaseOneJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class BootstrapMapper
      extends
      Mapper<AvroKey<GenericRecord>, NullWritable, BytesWritable, BytesWritable> {
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
    private String localInputDataDir = "./leaf-data-input";
    int inputCount = 0;
    int outputCount = 0;
    long totalTime = 0;
    Map<UUID, Map<String, Map<String, Integer>>> forwardIndexMap;
    Map<UUID, List<int[]>> nodeIdToleafRecordsMap;
    boolean debug = false;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOG.info("StarTreeBootstrapPhaseOneJob.BootstrapMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem dfs = FileSystem.get(configuration);
      Path configPath = new Path(
          configuration.get(STAR_TREE_BOOTSTRAP_CONFIG_PATH.toString()));
      try {
        starTreeConfig = StarTreeConfig.decode(dfs.open(configPath));
        config = StarTreeBootstrapPhaseOneConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        sourceTimeUnit = TimeUnit.valueOf(config.getTimeUnit());
        inputTimeUnitSize = config.getInputTimeUnitSize();
        aggregationTimeUnit = TimeUnit.valueOf(config
            .getAggregationGranularity());
        aggregationGranularitySize = config.getAggregationGranularitySize();
        dimensionValues = new String[dimensionNames.size()];
      } catch (Exception e) {
        throw new IOException(e);
      }
      // copy the tree locally and load it
      String starTreeOutputPath = context.getConfiguration().get(
          STAR_TREE_GENERATION_OUTPUT_PATH.toString());
      try {

        collectionName = config.getCollectionName();
        Path pathToTree = new Path(starTreeOutputPath + "/" + "star-tree-"
            + collectionName, collectionName + "-tree.bin");
        InputStream is = dfs.open(pathToTree);
        starTreeRootNode = StarTreePersistanceUtil.loadStarTree(is);

      } catch (Exception e) {
        throw new IOException(e);
      }

      // copy the leaf data

      // copy the leaf data tar gz locally and untar it
      try {

        collectionName = config.getCollectionName();
        String tarGZName = "leaf-data.tar.gz";
        Path pathToLeafData = new Path(starTreeOutputPath + "/" + "star-tree-"
            + collectionName, tarGZName);
        dfs.copyToLocalFile(pathToLeafData, new Path(tarGZName));
        new File(localInputDataDir).mkdirs();
        TarGzCompressionUtils.unTar(new File(tarGZName), new File(
            localInputDataDir));
        Collection<File> listFiles = FileUtils.listFiles(new File("."),
            FileFileFilter.FILE, DirectoryFileFilter.DIRECTORY);
        boolean b = true;
        for (File f : listFiles) {
          LOG.info(f.getAbsolutePath());
          if (b && f.getName().endsWith("idx")) {
            LOG.info(FileUtils.readFileToString(f));
            // b = false;
          }
        }
      } catch (Exception e) {
        throw new IOException(e);
      }

      // load the leaf record dimensions into memory. This assumes that the tree
      // + record dimensions can fit in memory
      // TODO: in the previous star tree gen step, we will have to break up the
      // tree into sub parts.
      LinkedList<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
      StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTreeRootNode);
      LOG.info("Num leaf Nodes in star tree:" + leafNodes.size());
      leafNodesMap = new HashMap<UUID, StarTreeNode>();
      forwardIndexMap = new HashMap<UUID, Map<String, Map<String, Integer>>>();
      nodeIdToleafRecordsMap = new HashMap<UUID, List<int[]>>();
      for (StarTreeNode node : leafNodes) {
        UUID uuid = node.getId();
        Map<String, Map<String, Integer>> forwardIndex = StarTreePersistanceUtil
            .readForwardIndex(uuid.toString(), localInputDataDir + "/data");
        List<int[]> leafRecords = StarTreePersistanceUtil
            .readLeafRecords(localInputDataDir + "/data", uuid.toString(),
                dimensionNames.size());
        leafNodesMap.put(uuid, node);
        forwardIndexMap.put(uuid, forwardIndex);
        nodeIdToleafRecordsMap.put(uuid, leafRecords);
      }

    }

    @Override
    public void map(AvroKey<GenericRecord> record, NullWritable value,
        Context context) throws IOException, InterruptedException {

      Object timeObj = record.datum().get(config.getTimeColumnName());
      if (timeObj == null){
        LOG.error("Dropping record because " + config.getTimeColumnName() + " is set to null");
        context.getCounter(StarTreeBootstrapPhase1Counter.NULL_TIME_RECORDS).increment(1);
        return;
      }

      // check if the time column has appropriate values and drop the record with zero timestamp.
      try{
        Long time = Long.parseLong(timeObj.toString());
        if(time == 0){
          LOG.error("Dropping record because " + config.getTimeColumnName() + " is set to 0");
          context.getCounter(StarTreeBootstrapPhase1Counter.ZERO_TIME_RECORDS).increment(1);
          return;
        }
        if(time < 0)
          throw new IllegalStateException("Value for dimension " + starTreeConfig.getTime().getColumnName() + " in " + record.datum() + " cannot be parsed");
      }catch(NumberFormatException ex){
        throw new IllegalStateException("Value for dimension "+ starTreeConfig.getTime().getColumnName() + " in " + record.datum() + " cannot be parsed");
      }

      long start = System.currentTimeMillis();
      Map<String, String> dimensionValuesMap = new HashMap<String, String>();
      for (int i = 0; i < dimensionNames.size(); i++) {
        String dimensionName = dimensionNames.get(i);
        String dimensionValue = "";
        Object val = record.datum().get(dimensionName);
        if (val != null) {
          dimensionValue = val.toString();
        }
        dimensionValues[i] = dimensionValue;
        dimensionValuesMap.put(dimensionName, dimensionValue);
      }
      DimensionKey key = new DimensionKey(dimensionValues);
      Long sourceTimeWindow = Long.parseLong(record.datum().get(config.getTimeColumnName()).toString());
      sourceTimeWindow = sourceTimeWindow * inputTimeUnitSize;
      long timeWindow = aggregationTimeUnit.convert(sourceTimeWindow, sourceTimeUnit);
      timeWindow = timeWindow / aggregationGranularitySize;
      MetricTimeSeries series = new MetricTimeSeries(metricSchema);
      for (int i = 0; i < metricNames.size(); i++) {
        String metricName = metricNames.get(i);
        Object object = record.datum().get(metricName);
        String metricValueStr = "0";
        if (object != null) {
          metricValueStr = object.toString();
        }
        try {
          Number metricValue = metricTypes.get(i).toNumber(metricValueStr);
          series.increment(timeWindow, metricName, metricValue);
        } catch (NumberFormatException e) {
          throw new NumberFormatException("Exception trying to convert "
              + metricValueStr + " to " + metricTypes.get(i)
              + " for metricName:" + metricName);
        }
      }
      // Collect specific / star records from tree that match
      Map<UUID, StarTreeRecord> collector = new HashMap<UUID, StarTreeRecord>();
      MetricTimeSeries emptyTimeSeries = new MetricTimeSeries(metricSchema);
      StarTreeRecord starTreeRecord = new StarTreeRecordImpl(starTreeConfig, key, emptyTimeSeries);
      StarTreeJobUtils.collectRecords(starTreeConfig, starTreeRootNode, starTreeRecord,
          collector);
      if (debug) {
        LOG.info("processing {}", key);
        LOG.info("times series {}", series);

      }
      for (UUID uuid : collector.keySet()) {
        if (!leafNodesMap.containsKey(uuid)) {
          String msg = "Got a mapping to non existant leaf node:" + uuid
              + " - " + collector.get(uuid) + " input :" + starTreeRecord;
          LOG.error(msg);
          throw new RuntimeException(msg);
        }
        List<int[]> leafRecords = nodeIdToleafRecordsMap.get(uuid);
        Map<String, Map<String, Integer>> forwardIndex = forwardIndexMap
            .get(uuid);
        Map<String, Map<Integer, String>> reverseForwardIndex = StarTreeUtils
            .toReverseIndex(forwardIndex);
        int[] bestMatch = StarTreeJobUtils.findBestMatch(key, dimensionNames,
            leafRecords, forwardIndex);
        String[] dimValues = StarTreeUtils.convertToStringValue(
            reverseForwardIndex, bestMatch, dimensionNames);
        DimensionKey matchedDimensionKey = new DimensionKey(dimValues);
        if (debug) {
          LOG.info("Match: {} under {}", matchedDimensionKey,
              leafNodesMap.get(uuid).getPath());
        }
        //
        BootstrapPhaseMapOutputKey outputKey = new BootstrapPhaseMapOutputKey(
            uuid, matchedDimensionKey.toMD5());
        BootstrapPhaseMapOutputValue outputValue = new BootstrapPhaseMapOutputValue(
            matchedDimensionKey, series);
        BytesWritable keyWritable = new BytesWritable(outputKey.toBytes());
        BytesWritable valWritable = new BytesWritable(outputValue.toBytes());
        context.write(keyWritable, valWritable);
      }
      long end = System.currentTimeMillis();
      inputCount = inputCount + 1;
      outputCount = outputCount + collector.size();
      totalTime = totalTime + (end - start);
      if (inputCount % 5000 == 0) {
        LOG.info("Processed {} in {}. avg time {} avg Fan out:{}", inputCount,
            totalTime, totalTime / inputCount, (outputCount / inputCount));
      }
    }

    @Override
    public void cleanup(Context context) throws IOException,
        InterruptedException {

      File f = new File(localInputDataDir);
      FileUtils.deleteDirectory(f);
      new File("leaf-data.tar.gz").delete();
    }
  }

  public static class StarTreeBootstrapReducer extends
      Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private StarTreeBootstrapPhaseOneConfig config;
    private List<MetricType> metricTypes;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private MetricSchema metricSchema;
    private boolean debug = false;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration configuration = context.getConfiguration();
      FileSystem dfs = FileSystem.get(configuration);
      Path configPath = new Path(
          configuration.get(STAR_TREE_BOOTSTRAP_CONFIG_PATH.toString()));
      try {
        StarTreeConfig starTreeConfig = StarTreeConfig.decode(dfs.open(configPath));
        config = StarTreeBootstrapPhaseOneConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(BytesWritable bootstrapMapOutputKeyWritable,
        Iterable<BytesWritable> bootstrapMapOutputValueWritableIterable,
        Context context) throws IOException, InterruptedException {

      MetricTimeSeries aggregateSeries = new MetricTimeSeries(metricSchema);
      DimensionKey key = null;
      for (BytesWritable writable : bootstrapMapOutputValueWritableIterable) {
        BootstrapPhaseMapOutputValue mapOutputValue = BootstrapPhaseMapOutputValue
            .fromBytes(writable.copyBytes(), metricSchema);
        if (key == null) {
          key = mapOutputValue.dimensionKey;
        }
        aggregateSeries.aggregate(mapOutputValue.metricTimeSeries);
      }
      BootstrapPhaseMapOutputValue mapOutputValue = new BootstrapPhaseMapOutputValue(
          key, aggregateSeries);
      BytesWritable valWritable = new BytesWritable(mapOutputValue.toBytes());
      if (debug) {
        LOG.info("Processed {}", key);
        LOG.info("time series: {}", aggregateSeries);
      }
      context.write(bootstrapMapOutputKeyWritable, valWritable);
    }

    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {

    }
  }

  public static class NodeIdBasedPartitioner extends
      Partitioner<BytesWritable, BytesWritable> {

    @Override
    public int getPartition(BytesWritable keyWritable, BytesWritable value,
        int numReducers) {
      BootstrapPhaseMapOutputKey key;
      try {
        key = BootstrapPhaseMapOutputKey.fromBytes(keyWritable.copyBytes());
        return (key.nodeId.hashCode() & Integer.MAX_VALUE) % numReducers;
      } catch (IOException e) {
        LOG.error("Error computing partition Id", e);
      }
      return 0;
    }

  }

  public void run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeBootstrapPhaseOneJob.class);

    // Avro schema
    FileSystem fs = FileSystem.get(getConf());
    Schema schema = new Schema.Parser().parse(fs.open(new Path(
        getAndCheck(STAR_TREE_BOOTSTRAP_INPUT_AVRO_SCHEMA.toString()))));
    LOG.info("{}", schema);

    // Map config
    job.setMapperClass(BootstrapMapper.class);
    AvroJob.setInputKeySchema(job, schema);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    job.setPartitionerClass(NodeIdBasedPartitioner.class);
    // Reduce config
    job.setCombinerClass(StarTreeBootstrapReducer.class);
    job.setReducerClass(StarTreeBootstrapReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setNumReduceTasks(10);
    // aggregation phase config
    Configuration configuration = job.getConfiguration();
    String inputPathDir = getAndSetConfiguration(configuration,
        STAR_TREE_BOOTSTRAP_INPUT_PATH);
    getAndSetConfiguration(configuration, STAR_TREE_GENERATION_OUTPUT_PATH);
    getAndSetConfiguration(configuration, STAR_TREE_BOOTSTRAP_CONFIG_PATH);
    getAndSetConfiguration(configuration, STAR_TREE_BOOTSTRAP_OUTPUT_PATH);
    getAndSetConfiguration(configuration, STAR_TREE_BOOTSTRAP_INPUT_AVRO_SCHEMA);
    LOG.info("Input path dir: " + inputPathDir);

    for (String inputPath : inputPathDir.split(",")) {
      Path input = new Path(inputPath);
      FileStatus[] listFiles = fs.listStatus(input);
      boolean isNested = false;
      for (FileStatus fileStatus : listFiles) {
        if (fileStatus.isDirectory()) {
          isNested = true;
          LOG.info("Adding input:" + fileStatus.getPath());
          FileInputFormat.addInputPath(job, fileStatus.getPath());
        }
      }
      if (!isNested) {
        LOG.info("Adding input:" + inputPath);
        FileInputFormat.addInputPath(job, input);
      }

    }

    FileOutputFormat.setOutputPath(job, new Path(
        getAndCheck(STAR_TREE_BOOTSTRAP_OUTPUT_PATH.toString())));

    job.waitForCompletion(true);

    Counters counters = job.getCounters();
    for(Enum e : StarTreeBootstrapPhase1Counter.values()){
      Counter counter = counters.findCounter(e);
      LOG.info(counter.getDisplayName() + " : " + counter.getValue());
    }
  }

  private String getAndSetConfiguration(Configuration configuration,
      StarTreeBootstrapPhaseOneConstants constant) {
    String value = getAndCheck(constant.toString());
    configuration.set(constant.toString(), value);
    return value;
  }

  private String getAndCheck(String propName) {
    String propValue = props.getProperty(propName);
    if (propValue == null) {
      throw new IllegalArgumentException(propName + " required property");
    }
    return propValue;
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("usage: config.properties");
    }

    Properties props = new Properties();
    props.load(new FileInputStream(args[0]));

    StarTreeBootstrapPhaseOneJob job = new StarTreeBootstrapPhaseOneJob(
        "star_tree_bootstrap_job", props);
    job.run();
  }

  public static enum StarTreeBootstrapPhase1Counter{
    ZERO_TIME_RECORDS,
    NULL_TIME_RECORDS
  }
}
