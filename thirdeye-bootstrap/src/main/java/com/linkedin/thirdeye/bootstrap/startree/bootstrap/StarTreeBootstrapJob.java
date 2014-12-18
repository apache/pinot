package com.linkedin.thirdeye.bootstrap.startree.bootstrap;

import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.StarTreeBootstrapConstants.STAR_TREE_BOOTSTRAP_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.StarTreeBootstrapConstants.STAR_TREE_BOOTSTRAP_INPUT_AVRO_SCHEMA;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.StarTreeBootstrapConstants.STAR_TREE_BOOTSTRAP_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.StarTreeBootstrapConstants.STAR_TREE_BOOTSTRAP_OUTPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.StarTreeBootstrapConstants.STAR_TREE_GENERATION_OUTPUT_PATH;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.bootstrap.DimensionKey;
import com.linkedin.thirdeye.bootstrap.MetricSchema;
import com.linkedin.thirdeye.bootstrap.MetricTimeSeries;
import com.linkedin.thirdeye.bootstrap.MetricType;
import com.linkedin.thirdeye.bootstrap.startree.StarTreeJobUtils;
import com.linkedin.thirdeye.bootstrap.util.TarGzCompressionUtils;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreCircularBufferImpl;
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
public class StarTreeBootstrapJob extends Configured {
  private static final Logger LOG = LoggerFactory
      .getLogger(StarTreeBootstrapJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;
  private Properties props;

  enum Constants {

  }

  public StarTreeBootstrapJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class AggregationMapper
      extends
      Mapper<AvroKey<GenericRecord>, NullWritable, BytesWritable, BytesWritable> {
    private StarTreeBootstrapConfig config;
    private TimeUnit sourceTimeUnit;
    private TimeUnit aggregationTimeUnit;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    private String[] dimensionValues;
    private StarTreeNode starTreeRootNode;
    private String collectionName;
    private Map<UUID, StarTreeNode> leafNodesMap;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOG.info("AggregatePhaseJob.AggregationMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(
          configuration.get(STAR_TREE_BOOTSTRAP_CONFIG_PATH.toString()));
      try {
        config = OBJECT_MAPPER.readValue(fileSystem.open(configPath),
            StarTreeBootstrapConfig.class);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = Lists.newArrayList();
        for (String type : config.getMetricTypes()) {
          metricTypes.add(MetricType.valueOf(type));
        }
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        sourceTimeUnit = TimeUnit.valueOf(config.getTimeUnit());
        aggregationTimeUnit = TimeUnit.valueOf(config
            .getAggregationGranularity());
        dimensionValues = new String[dimensionNames.size()];
      } catch (Exception e) {
        throw new IOException(e);
      }

      String starTreeOutputPath = context.getConfiguration().get(
          STAR_TREE_GENERATION_OUTPUT_PATH.toString());
      try {

        collectionName = config.getCollectionName();
        Path pathToTree = new Path(starTreeOutputPath + "/" + "star-tree-"
            + collectionName, collectionName + "-tree.bin");
        ObjectInputStream objectInputStream = new ObjectInputStream(
            fileSystem.open(pathToTree));
        starTreeRootNode = (StarTreeNode) objectInputStream.readObject();
        LinkedList<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
        StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTreeRootNode);
        LOG.info("Num leaf Nodes in star tree:" + leafNodes.size());
        leafNodesMap = new HashMap<UUID, StarTreeNode>();
        for (StarTreeNode node : leafNodes) {
          leafNodesMap.put(node.getId(), node);
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void map(AvroKey<GenericRecord> record, NullWritable value,
        Context context) throws IOException, InterruptedException {
      Map<String, String> dimensionValuesMap = new HashMap<String, String>();
      for (int i = 0; i < dimensionNames.size(); i++) {
        String dimensionName = dimensionNames.get(i);
        String dimensionValue = "";
        Object val = record.datum().get(dimensionName);
        if (val != null) {
          dimensionValue = val.toString();
        }
        dimensionValues[i] = dimensionValue;
        dimensionValuesMap.put(dimensionValues[i], dimensionValue);
      }

      DimensionKey key = new DimensionKey(dimensionValues);
      String sourceTimeWindow = record.datum().get(config.getTimeColumnName())
          .toString();

      long timeWindow = aggregationTimeUnit.convert(
          Long.parseLong(sourceTimeWindow), sourceTimeUnit);
      timeWindow = -1;
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
      Map<String, Integer> metricValues = Collections.emptyMap();
      Long time = 0L;
      StarTreeRecord starTreeRecord = new StarTreeRecordImpl(
          dimensionValuesMap, metricValues, time);
      StarTreeJobUtils.collectRecords(starTreeRootNode, starTreeRecord,
          collector);

      byte[] serializedKey = key.toBytes();

      byte[] serializedMetrics = series.toBytes();

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      dos.writeInt(serializedKey.length);
      dos.write(serializedKey);
      dos.writeInt(serializedMetrics.length);
      dos.write(serializedMetrics);
      BytesWritable valWritable = new BytesWritable(baos.toByteArray());

      for (UUID uuid : collector.keySet()) {
        if (!leafNodesMap.containsKey(uuid)) {
          String msg = "Got a mapping to non existant leaf node:" + uuid
              + " - " + collector.get(uuid) + " input :" + starTreeRecord;
          LOG.error(msg);
          throw new RuntimeException(msg);
        }
        byte[] uuidBytes = uuid.toString().getBytes();
        LOG.info(new String(uuidBytes) + Arrays.toString(uuidBytes));
        context.write(new BytesWritable(uuidBytes), valWritable);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException,
        InterruptedException {
      
      if(true){
        return;
      }
      LinkedList<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
      MetricTimeSeries emptySeries = new MetricTimeSeries(metricSchema);
      DimensionKey key;
      String[] dimValues = new String[dimensionNames.size()];
      StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTreeRootNode);
      //write empty record for each leaf, we need to do this because some incoming records may not be mapped to any leaf node.
      //there are multiple optimizations possible here
      //1. keep track of how many leaf nodes were already mapped in the map method and 
      //do the following segment only for the ones that were not mapped.
      //2.we can move this logic to reducer. its more efficient 
      for (StarTreeNode node : leafNodes) {
        UUID uuid = node.getId();
        byte[] uuidBytes = uuid.toString().getBytes();
        for (int i = 0; i < dimensionNames.size(); i++) {
          String dimName = dimensionNames.get(i);
          if (node.getAncestorDimensionNames().contains(dimName)) {
            dimValues[i] = node.getAncestorDimensionValues().get(dimName);
          } else if (node.getDimensionName().equals(dimName)) {
            dimValues[i] = node.getDimensionValue();
          }else{
            dimValues[i]= StarTreeConstants.OTHER;
          }
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        key = new DimensionKey(dimValues);
        byte[] serializedKey = key.toBytes();
        byte[] serializedMetrics = emptySeries.toBytes();
        dos.writeInt(serializedKey.length);
        dos.write(serializedKey);
        dos.writeInt(serializedMetrics.length);
        dos.write(serializedMetrics);
        BytesWritable valWritable = new BytesWritable(baos.toByteArray());
        context.write(new BytesWritable(uuidBytes), valWritable);
      }
    }
  }

  public static class StarTreeBootstrapReducer extends
      Reducer<BytesWritable, BytesWritable, NullWritable, NullWritable> {
    private StarTreeBootstrapConfig config;
    private List<MetricType> metricTypes;
    private List<String> metricNames;

    private MetricSchema metricSchema;
    private List<String> dimensionNames;

    String localInputDataDir = "leaf-data-input";
    private int numTimeBuckets;
    private boolean keepMetricValues = true;
    private String hdfsOutputDir;
    private String localOutputDataDir = "./leaf-data-output";
    private String collectionName;
    List<StarTreeRecord> records = new ArrayList<StarTreeRecord>();
    
    //instance level variable but restricted scope in the reduce method. Allows re-use and avoids GC overhead
    Map<String, String> dimensionValuesMap = new HashMap<String, String>();
    Map<String, Integer> metricValuesMap = new HashMap<String, Integer>();
    Map<DimensionKey, MetricTimeSeries> rollUpAggregateMap = new HashMap<DimensionKey, MetricTimeSeries>();
    Map<DimensionKey, MetricTimeSeries> rawAggregateMap = new HashMap<DimensionKey, MetricTimeSeries>();
    Map<String, Map<Integer, String>> reverseForwardIndex = new HashMap<String, Map<Integer, String>>();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration configuration = context.getConfiguration();
      FileSystem dfs = FileSystem.get(configuration);
      Path configPath = new Path(
          configuration.get(STAR_TREE_BOOTSTRAP_CONFIG_PATH.toString()));
      hdfsOutputDir = configuration.get(STAR_TREE_BOOTSTRAP_OUTPUT_PATH
          .toString());
      try {
        config = OBJECT_MAPPER.readValue(dfs.open(configPath),
            StarTreeBootstrapConfig.class);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = Lists.newArrayList();
        for (String type : config.getMetricTypes()) {
          metricTypes.add(MetricType.valueOf(type));
        }
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        numTimeBuckets = config.getNumTimeBuckets();
      } catch (Exception e) {
        throw new IOException(e);
      }

      // copy the leaf data tar gz locally and untar it
      String starTreeOutputPath = context.getConfiguration().get(
          STAR_TREE_GENERATION_OUTPUT_PATH.toString());
      try {

        collectionName = config.getCollectionName();
        String tarGZName = "leaf-data.tar.gz";
        Path pathToLeafData = new Path(starTreeOutputPath + "/" + "star-tree-"
            + collectionName, tarGZName);
        dfs.copyToLocalFile(pathToLeafData, new Path(tarGZName));
        new File(localInputDataDir).mkdirs();
        TarGzCompressionUtils
            .unTar(new File(tarGZName), new File(localInputDataDir));
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
        
        new File(localOutputDataDir +"/data/").mkdirs();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(BytesWritable nodeIdWritable,
        Iterable<BytesWritable> dimensionTimeSeriesIterable, Context context)
        throws IOException, InterruptedException {

      // read the leaf dimensions for this nodeId, read both forward index
      // (dictionary) and dimension records.
      byte[] uuidBytes = nodeIdWritable.copyBytes();
      String uuidString = new String(uuidBytes);
      LOG.info("Processing:" + uuidString + Arrays.toString(uuidBytes));
      String nodeId = uuidString; // UUID.fromString(uuidString);
      Map<String, Map<String, Integer>> forwardIndex;
      
      //clear all temporary data structures;
      reverseForwardIndex.clear();
      rawAggregateMap.clear();
      rollUpAggregateMap.clear();
      records.clear();
      dimensionValuesMap.clear();
      metricValuesMap.clear();
      
      List<int[]> leafRecords;
      forwardIndex = StarTreePersistanceUtil.readForwardIndex(nodeId,
          localInputDataDir + "/data");
      leafRecords = StarTreePersistanceUtil.readLeafRecords(localInputDataDir
          + "/data", nodeId, dimensionNames.size());
      // generate reverserForwardIndex
      for (String dimensionName : forwardIndex.keySet()) {
        Map<String, Integer> map = forwardIndex.get(dimensionName);
        reverseForwardIndex.put(dimensionName, new HashMap<Integer, String>());
        for (Entry<String, Integer> entry : map.entrySet()) {
          reverseForwardIndex.get(dimensionName).put(entry.getValue(),
              entry.getKey());
        }
      }
      // aggregate if the dimensionKeys are the same
      for (BytesWritable writable : dimensionTimeSeriesIterable) {
        byte[] bytes = writable.copyBytes();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(
            bytes));
        int length = dis.readInt();
        byte[] dimensionKeyBytes = new byte[length];
        dis.readFully(dimensionKeyBytes);
        DimensionKey key = DimensionKey.fromBytes(dimensionKeyBytes);
        length = dis.readInt();
        byte[] timeSeriesBytes = new byte[length];
        dis.readFully(timeSeriesBytes);
        MetricTimeSeries series = MetricTimeSeries.fromBytes(timeSeriesBytes,
            metricSchema);
        if (!rawAggregateMap.containsKey(key)) {
          rawAggregateMap.put(key, new MetricTimeSeries(metricSchema));
        }
        rawAggregateMap.get(key).aggregate(series);
      }

      // for each dimension Key, find the best roll up and aggregate on the roll
      // up
      for (DimensionKey key : rawAggregateMap.keySet()) {
        MetricTimeSeries series = rawAggregateMap.get(key);
        // find the best match possible amongst the leaf record
        int[] matchedValues = StarTreeJobUtils.findBestMatch(key,
            dimensionNames, leafRecords, forwardIndex);
        DimensionKey bestMatchDimensionKey = new DimensionKey(
            convertToStringValue(reverseForwardIndex, matchedValues));
        if (!rollUpAggregateMap.containsKey(bestMatchDimensionKey)) {
          rollUpAggregateMap.put(bestMatchDimensionKey, new MetricTimeSeries(
              metricSchema));
        }
        rollUpAggregateMap.get(bestMatchDimensionKey).aggregate(series);
      }

      // generate the circular buffer for this node.

      for (int[] leafRecord : leafRecords) {
        // convert int value to string
        String[] dimValues = convertToStringValue(reverseForwardIndex,
            leafRecord);
        // construction the dimensionValues
        dimensionValuesMap.clear();
        for (int i = 0; i < dimensionNames.size(); i++) {
          dimensionValuesMap.put(dimensionNames.get(i), dimValues[i]);
        }
        // get the aggregated timeseries for this dimensionKey
        DimensionKey dimensionKey = new DimensionKey(dimValues);
        MetricTimeSeries metricTimeSeries = rollUpAggregateMap
            .get(dimensionKey);
        if (metricTimeSeries != null) {
          // add a star tree record for each timewindow
          for (long timeWindow : metricTimeSeries.getTimeWindowSet()) {
            metricValuesMap.clear();
            for (String metricName : metricNames) {
              Number number = metricTimeSeries.get(timeWindow, metricName);
              // TODO: support all data types in star tree
              metricValuesMap.put(metricName, number.intValue());
            }
            StarTreeRecord record = new StarTreeRecordImpl(dimensionValuesMap,
                metricValuesMap, timeWindow);
            records.add(record);
          }
        } else {
          // add an empty record for time 0
          metricValuesMap.clear();
          for (String metricName : metricNames) {
            // TODO: support all data types in star tree
            metricValuesMap.put(metricName, 0);
          }
          StarTreeRecord record = new StarTreeRecordImpl(dimensionValuesMap,
              metricValuesMap, 0l);
          records.add(record);
        }
      }
      OutputStream outputStream = new FileOutputStream(localOutputDataDir +"/data/" + nodeId.toString() + StarTreeConstants.BUFFER_FILE_SUFFIX);
      StarTreeRecordStoreCircularBufferImpl.fillBuffer(outputStream,
          dimensionNames, metricNames, forwardIndex, records, numTimeBuckets,
          keepMetricValues);
      outputStream.flush();
      outputStream.close();
      StarTreePersistanceUtil.saveLeafNodeForwardIndex(localOutputDataDir +"/data/", forwardIndex, nodeId);
    }
    
    /**
     * converts the raw integer id to string representation using the reverse forward Index
     * @param reverseForwardIndex
     * @param leafRecord
     * @return
     */
    private String[] convertToStringValue(
        Map<String, Map<Integer, String>> reverseForwardIndex, int[] leafRecord) {
      String[] ret = new String[leafRecord.length];
      for (int i = 0; i < leafRecord.length; i++) {
        ret[i] = reverseForwardIndex.get(dimensionNames.get(i)).get(
            leafRecord[i]);
      }
      return ret;
    }

    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {

      FileSystem dfs = FileSystem.get(context.getConfiguration());

      String leafDataTarGz = context.getTaskAttemptID().getTaskID()
          + "-leaf-data.tar.gz";
      LOG.info("Generating " + leafDataTarGz + " from " + localOutputDataDir);
      // generate the tar file
      TarGzCompressionUtils.createTarGzOfDirectory(localOutputDataDir,
          leafDataTarGz);
      Path src = FileSystem.getLocal(new Configuration()).makeQualified(
          new Path(leafDataTarGz));
      Path dst = dfs.makeQualified(new Path(hdfsOutputDir, leafDataTarGz));
      LOG.info("Copying " + src + " to " + dst);
      dfs.copyFromLocalFile(src, dst);

    }
  }

  public void run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeBootstrapJob.class);

    // Avro schema
    Schema schema = new Schema.Parser()
        .parse(FileSystem
            .get(getConf())
            .open(
                new Path(
                    getAndCheck(StarTreeBootstrapConstants.STAR_TREE_BOOTSTRAP_INPUT_AVRO_SCHEMA
                        .toString()))));
    LOG.info("{}", schema);

    // Map config
    job.setMapperClass(AggregationMapper.class);
    AvroJob.setInputKeySchema(job, schema);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    job.setReducerClass(StarTreeBootstrapReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    // job.setOutputFormatClass(NullWritable.class);

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
      LOG.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }

    FileOutputFormat.setOutputPath(job, new Path(
        getAndCheck(STAR_TREE_BOOTSTRAP_OUTPUT_PATH.toString())));

    job.waitForCompletion(true);
  }

  private String getAndSetConfiguration(Configuration configuration,
      StarTreeBootstrapConstants constant) {
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

    StarTreeBootstrapJob job = new StarTreeBootstrapJob(
        "star_tree_bootstrap_job", props);
    job.run();
  }

}
