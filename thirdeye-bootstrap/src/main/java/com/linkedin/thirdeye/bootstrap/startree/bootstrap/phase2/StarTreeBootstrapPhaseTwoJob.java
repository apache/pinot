package com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2;

import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoConstants.STAR_TREE_GENERATION_OUTPUT_PATH;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import com.linkedin.thirdeye.api.StarTreeConfig;
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.BootstrapPhaseMapOutputKey;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.BootstrapPhaseMapOutputValue;
import com.linkedin.thirdeye.bootstrap.util.CircularBufferUtil;
import com.linkedin.thirdeye.bootstrap.util.TarGzCompressionUtils;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeUtils;

public class StarTreeBootstrapPhaseTwoJob extends Configured {
  private static final Logger LOG = LoggerFactory
      .getLogger(StarTreeBootstrapPhaseTwoJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;
  private Properties props;

  public StarTreeBootstrapPhaseTwoJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class BootstrapPhaseTwoMapper extends
      Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {

    private StarTreeBootstrapPhaseTwoConfig config;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    private String[] dimensionValues;
    private StarTreeNode starTreeRootNode;
    private String collectionName;
    private Map<UUID, StarTreeNode> leafNodesMap;
    private String localInputDataDir = "./leaf-data-input";

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOG.info("AggregatePhaseJob.AggregationMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem dfs = FileSystem.get(configuration);
      Path configPath = new Path(
          configuration.get(STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH.toString()));
      try {
        StarTreeConfig starTreeConfig = StarTreeConfig.decode(dfs.open(configPath));
        config = StarTreeBootstrapPhaseTwoConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = Lists.newArrayList();
        for (String type : config.getMetricTypes()) {
          metricTypes.add(MetricType.valueOf(type));
        }
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
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
    public void map(BytesWritable keyWritable, BytesWritable valueWritable,
        Context context) throws IOException, InterruptedException {
      // write the key:nodeId value:leaf record, metrics
      BootstrapPhaseMapOutputKey key;
      key = BootstrapPhaseMapOutputKey.fromBytes(keyWritable.copyBytes());
      BootstrapPhaseMapOutputValue value;
      value = BootstrapPhaseMapOutputValue.fromBytes(valueWritable.copyBytes(),
          metricSchema);
      UUID nodeId = key.getNodeId();
      leafNodesMap.remove(nodeId);
      context.write(new BytesWritable(nodeId.toString().getBytes()),
          valueWritable);
    }

    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {
      String[] dimValues = new String[dimensionNames.size()];
      MetricTimeSeries emptyTimeSeries = new MetricTimeSeries(metricSchema);
      // write empty records for the leaf nodes that did not see any record
      for (Entry<UUID, StarTreeNode> entry : leafNodesMap.entrySet()) {
        UUID uuid = entry.getKey();
        StarTreeNode node = entry.getValue();
        byte[] uuidBytes = uuid.toString().getBytes();
        for (int i = 0; i < dimensionNames.size(); i++) {
          String dimName = dimensionNames.get(i);
          if (node.getAncestorDimensionNames().contains(dimName)) {
            dimValues[i] = node.getAncestorDimensionValues().get(dimName);
          } else if (node.getDimensionName().equals(dimName)) {
            dimValues[i] = node.getDimensionValue();
          } else {
            dimValues[i] = StarTreeConstants.OTHER;
          }
        }
        DimensionKey dimensionKey = new DimensionKey(dimValues);
        BootstrapPhaseMapOutputValue value = new BootstrapPhaseMapOutputValue(
            dimensionKey, emptyTimeSeries);
        BytesWritable valWritable = new BytesWritable(value.toBytes());
        context.write(new BytesWritable(uuidBytes), valWritable);
      }
    }
  }

  public static class BootstrapPhaseTwoReducer extends

  Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private StarTreeBootstrapPhaseTwoConfig config;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    private StarTreeNode starTreeRootNode;
    private String collectionName;
    private Map<UUID, StarTreeNode> leafNodesMap;
    private String localInputDataDir = "./leaf-data-input";
    private int numTimeBuckets;
    private boolean keepMetricValues = true;
    int inputCount = 0;
    int outputCount = 0;
    long totalTime = 0;
    private Map<String, Map<String, Integer>> forwardIndex;
    private Map<String, Map<Integer, String>> reverseForwardIndex;
    private String localOutputDataDir = "./leaf-data-output";
    private String hdfsOutputDir;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOG.info("BootstrapPhaseTwo.BootstrapPhaseTwoReducer.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem dfs = FileSystem.get(configuration);
      Path configPath = new Path(
          configuration.get(STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH.toString()));
      hdfsOutputDir = configuration.get(STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH
          .toString());
      try {
        StarTreeConfig starTreeConfig = StarTreeConfig.decode(dfs.open(configPath));
        config = StarTreeBootstrapPhaseTwoConfig.fromStarTreeConfig(starTreeConfig);
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
      // copy the tree locally and load it
      String starTreeOutputPath = context.getConfiguration().get(
          STAR_TREE_GENERATION_OUTPUT_PATH.toString());
      try {

        collectionName = config.getCollectionName();
        Path pathToTree = new Path(starTreeOutputPath + "/" + "star-tree-"
            + collectionName, collectionName + "-tree.bin");
        InputStream is = dfs.open(pathToTree);
        starTreeRootNode = StarTreePersistanceUtil.loadStarTree(is);
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

      // create the directory for output
      new File(localOutputDataDir + "/data").mkdirs();
    }

    @Override
    public void reduce(BytesWritable keyWritable,
        Iterable<BytesWritable> bootstrapMapOutputValueWritableIterable,
        Context context) throws IOException, InterruptedException {
      // for each nodeId, load the forward index and the leaf data.
      String nodeId = new String(keyWritable.copyBytes());
      LOG.info("START: processing {}", nodeId);
      Map<DimensionKey, MetricTimeSeries> map = new HashMap<DimensionKey, MetricTimeSeries>();
      List<int[]> leafRecords;
      forwardIndex = StarTreePersistanceUtil.readForwardIndex(nodeId,
          localInputDataDir + "/data");
      leafRecords = StarTreePersistanceUtil.readLeafRecords(localInputDataDir
          + "/data", nodeId, dimensionNames.size());

      // generate reverserForwardIndex
      reverseForwardIndex = StarTreeUtils.toReverseIndex(forwardIndex);
      for (BytesWritable writable : bootstrapMapOutputValueWritableIterable) {
        BootstrapPhaseMapOutputValue val = BootstrapPhaseMapOutputValue
            .fromBytes(writable.copyBytes(), metricSchema);
        if (!map.containsKey(val.getDimensionKey())) {
          map.put(val.getDimensionKey(), val.getMetricTimeSeries());
        } else {
          map.get(val.getDimensionKey()).aggregate(val.getMetricTimeSeries());
        }
      }
      String fileName = localOutputDataDir + "/data/" + nodeId.toString()
          + StarTreeConstants.BUFFER_FILE_SUFFIX;

      CircularBufferUtil.createLeafBufferFile(map, leafRecords, fileName,
          dimensionNames, metricNames, metricTypes, numTimeBuckets,
          reverseForwardIndex);
      LOG.info("Generating forward index");
      StarTreePersistanceUtil.saveLeafNodeForwardIndex(localOutputDataDir
          + "/data/", forwardIndex, nodeId);

      LOG.info("END: processing {}", nodeId);

    }

    @Override
    protected void cleanup(Context context) throws IOException,
        InterruptedException {

      LOG.info("START: In clean up");
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
      LOG.info("END: Clean up");

    }
  }

  public void run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeBootstrapPhaseTwoJob.class);

    // Map config
    job.setMapperClass(BootstrapPhaseTwoMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    job.setReducerClass(BootstrapPhaseTwoReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    job.setNumReduceTasks(10);
    // aggregation phase config
    Configuration configuration = job.getConfiguration();
    String inputPathDir = getAndSetConfiguration(configuration,
        STAR_TREE_BOOTSTRAP_PHASE2_INPUT_PATH);
    getAndSetConfiguration(configuration, STAR_TREE_GENERATION_OUTPUT_PATH);
    getAndSetConfiguration(configuration,
        STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH);
    getAndSetConfiguration(configuration,
        STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH);
    LOG.info("Input path dir: " + inputPathDir);
    for (String inputPath : inputPathDir.split(",")) {
      LOG.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }

    FileOutputFormat.setOutputPath(job, new Path(
        getAndCheck(STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH.toString())));

    job.waitForCompletion(true);
  }

  private String getAndSetConfiguration(Configuration configuration,
      StarTreeBootstrapPhaseTwoConstants constant) {
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

    StarTreeBootstrapPhaseTwoJob job = new StarTreeBootstrapPhaseTwoJob(
        "star_tree_bootstrap_phase2_job", props);
    job.run();
  }
}
