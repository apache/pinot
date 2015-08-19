package com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.bootstrap.ThirdEyeJobConstants;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.BootstrapPhaseMapOutputKey;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.BootstrapPhaseMapOutputValue;
import com.linkedin.thirdeye.impl.storage.IndexFormat;
import com.linkedin.thirdeye.impl.storage.VariableSizeBufferUtil;
import com.linkedin.thirdeye.impl.storage.IndexMetadata;
import com.linkedin.thirdeye.bootstrap.util.DFSUtil;
import com.linkedin.thirdeye.bootstrap.util.TarGzBuilder;
import com.linkedin.thirdeye.bootstrap.util.TarGzCompressionUtils;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import com.linkedin.thirdeye.impl.storage.DimensionDictionary;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
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

import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoConstants.STAR_TREE_GENERATION_OUTPUT_PATH;

public class StarTreeBootstrapPhaseTwoJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeBootstrapPhaseTwoJob.class);

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
    private String localStagingDir = "./staging";

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("AggregatePhaseJob.AggregationMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem dfs = FileSystem.get(configuration);

      Path configPath =
          new Path(configuration.get(STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH.toString()));
      try {
        StarTreeConfig starTreeConfig = StarTreeConfig.decode(dfs.open(configPath));
        config = StarTreeBootstrapPhaseTwoConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        dimensionValues = new String[dimensionNames.size()];
      } catch (Exception e) {
        throw new IOException(e);
      }

      // copy the tree locally and load it
      String starTreeOutputPath =
          context.getConfiguration().get(STAR_TREE_GENERATION_OUTPUT_PATH.toString());
      try {

        collectionName = config.getCollectionName();
        Path pathToTree = new Path(starTreeOutputPath + "/" + "tree.bin");
        InputStream is = dfs.open(pathToTree);
        starTreeRootNode = StarTreePersistanceUtil.loadStarTree(is);
        LinkedList<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
        StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTreeRootNode);
        LOGGER.info("Num leaf Nodes in star tree:" + leafNodes.size());
        leafNodesMap = new HashMap<UUID, StarTreeNode>();
        for (StarTreeNode node : leafNodes) {
          leafNodesMap.put(node.getId(), node);
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void map(BytesWritable keyWritable, BytesWritable valueWritable, Context context)
        throws IOException, InterruptedException {
      // write the key:nodeId value:leaf record, metrics
      BootstrapPhaseMapOutputKey key;
      key = BootstrapPhaseMapOutputKey.fromBytes(keyWritable.copyBytes());
      BootstrapPhaseMapOutputValue value;
      value = BootstrapPhaseMapOutputValue.fromBytes(valueWritable.copyBytes(), metricSchema);
      UUID nodeId = key.getNodeId();
      leafNodesMap.remove(nodeId);
      context.write(new BytesWritable(nodeId.toString().getBytes()), valueWritable);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
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
        BootstrapPhaseMapOutputValue value =
            new BootstrapPhaseMapOutputValue(dimensionKey, emptyTimeSeries);
        BytesWritable valWritable = new BytesWritable(value.toBytes());
        context.write(new BytesWritable(uuidBytes), valWritable);
      }

      // clean-up
      // Delete all the local files
      File f = new File(localStagingDir);
      FileUtils.deleteDirectory(f);
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
    private String localInputDataDir = "./staging-input";
    private String localTmpDataDir = "./staging-tmp";
    private String localOutputDataDir = "./staging-output";
    private String hdfsOutputDir;
    private StarTreeConfig starTreeConfig;
    private Path pathToTree;
    private Long minDataTime = Long.MAX_VALUE;
    private Long maxDataTime = 0L;
    private Long startTime = 0L;
    private Long endTime = Long.MAX_VALUE;
    private String schedule = "";
    private IndexMetadata indexMetadata;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("BootstrapPhaseTwo.BootstrapPhaseTwoReducer.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem dfs = FileSystem.get(configuration);
      Path configPath =
          new Path(configuration.get(STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH.toString()));
      hdfsOutputDir = configuration.get(STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH.toString());
      try {
        starTreeConfig = StarTreeConfig.decode(dfs.open(configPath));
        config = StarTreeBootstrapPhaseTwoConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
      } catch (Exception e) {
        throw new IOException(e);
      }
      // copy the tree locally and load it
      String starTreeOutputPath =
          context.getConfiguration().get(STAR_TREE_GENERATION_OUTPUT_PATH.toString());
      try {

        collectionName = config.getCollectionName();
        pathToTree = new Path(starTreeOutputPath + "/" + "tree.bin");
        InputStream is = dfs.open(pathToTree);
        starTreeRootNode = StarTreePersistanceUtil.loadStarTree(is);
        LinkedList<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
        StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTreeRootNode);
        LOGGER.info("Num leaf Nodes in star tree:" + leafNodes.size());
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
        String tarGZName = "dimensionStore.tar.gz";
        Path pathToDimensionIndexTarGz = new Path(starTreeOutputPath, tarGZName);
        dfs.copyToLocalFile(pathToDimensionIndexTarGz, new Path(tarGZName));
        new File(localInputDataDir).mkdirs();
        TarGzCompressionUtils.unTar(new File(tarGZName), new File(localInputDataDir));
      } catch (Exception e) {
        throw new IOException(e);
      }

      // create the directory for output
      new File(localOutputDataDir + "/metricStore").mkdirs();
    }

    @Override
    public void reduce(BytesWritable keyWritable,
        Iterable<BytesWritable> bootstrapMapOutputValueWritableIterable, Context context)
        throws IOException, InterruptedException {

      String nodeId = new String(keyWritable.copyBytes());

      LOGGER.info("START: processing {}", nodeId);

      Map<DimensionKey, MetricTimeSeries> records = new HashMap<DimensionKey, MetricTimeSeries>();


      String dimensionStoreIndexDir = localInputDataDir + "/dimensionStore";
      DimensionDictionary dictionary =
          new DimensionDictionary(StarTreePersistanceUtil.readForwardIndex(nodeId,
              dimensionStoreIndexDir));

      // Read all combinations
      List<int[]> allCombinations =
          StarTreePersistanceUtil.readLeafRecords(dimensionStoreIndexDir, nodeId, starTreeConfig
              .getDimensions().size());

      // Aggregate time series for each dimension combination
      for (BytesWritable recordBytes : bootstrapMapOutputValueWritableIterable) {
        BootstrapPhaseMapOutputValue record =
            BootstrapPhaseMapOutputValue.fromBytes(recordBytes.copyBytes(), metricSchema);

        if (!records.containsKey(record.getDimensionKey())) {
          records.put(record.getDimensionKey(), record.getMetricTimeSeries());
        } else {
          records.get(record.getDimensionKey()).aggregate(record.getMetricTimeSeries());
        }

        List<Long> timeWindowSet = new ArrayList<Long>(record.getMetricTimeSeries().getTimeWindowSet());

        // Get metadata for each record
        if (timeWindowSet != null && timeWindowSet.size() != 0)
        {
          Collections.sort(timeWindowSet);

          long recordMin = timeWindowSet.get(0);
          long recordMax = timeWindowSet.get(timeWindowSet.size() - 1);
          if (recordMin < minDataTime)
          {
            minDataTime = recordMin;
          }
          if (recordMax > maxDataTime)
          {
            maxDataTime = recordMax;
          }
        }

      }

      // Add an empty record for each dimension not represented
      for (int[] combination : allCombinations) {
        DimensionKey dimensionKey =
            dictionary.translate(starTreeConfig.getDimensions(), combination);
        if (!records.containsKey(dimensionKey)) {
          MetricTimeSeries emptyTimeSeries = new MetricTimeSeries(metricSchema);
          records.put(dimensionKey, emptyTimeSeries);
        }
      }

      // Write dimension / metric buffers
      VariableSizeBufferUtil.createLeafBufferFiles(new File(localTmpDataDir), nodeId, starTreeConfig,
          records, dictionary);

      // Create metadata object
      long minDataTimeMillis = TimeUnit.MILLISECONDS.convert
          (minDataTime * config.getBucketSize() , TimeUnit.valueOf(config.getAggregationGranularity()));

      long maxDataTimeMillis = TimeUnit.MILLISECONDS.convert
          (maxDataTime * config.getBucketSize() , TimeUnit.valueOf(config.getAggregationGranularity()));

      indexMetadata = new IndexMetadata
          (minDataTime, maxDataTime, minDataTimeMillis, maxDataTimeMillis,
              config.getAggregationGranularity(), config.getBucketSize(), IndexFormat.VARIABLE_SIZE);

      LOGGER.info("END: processing {}", nodeId);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

      LOGGER.info("START: In clean up");

      FileSystem dfs = FileSystem.get(context.getConfiguration());

      String outputTarGz = "data-" + context.getTaskAttemptID().getTaskID().getId() + ".tar.gz";

      LOGGER.info("Generating " + outputTarGz + " from " + localOutputDataDir);

      Collection<File> listFiles = FileUtils.listFiles(new File(localOutputDataDir), null, true);
      LOGGER.info("Files under " + localOutputDataDir + " before combining");
      for (File f : listFiles) {
        LOGGER.info(f.getAbsolutePath());
      }
      // Combine
      VariableSizeBufferUtil.combineDataFiles(dfs.open(pathToTree), new File(localTmpDataDir), new File(
          localOutputDataDir));
      Collection<File> listFilesAfterCombine =
          FileUtils.listFiles(new File(localOutputDataDir), null, true);
      LOGGER.info("Files under " + localOutputDataDir + " after combining");
      for (File f : listFilesAfterCombine) {
        LOGGER.info(f.getAbsolutePath());
      }

      FileSystem localFS = FileSystem.getLocal(context.getConfiguration());
      // Create tar gz of directory
      TarGzBuilder builder = new TarGzBuilder(outputTarGz, localFS, localFS);
      //add tree
      builder.addFileEntry(new Path(localOutputDataDir, StarTreeConstants.TREE_FILE_NAME));

      // add metadata
      if (indexMetadata != null) {
        VariableSizeBufferUtil.writeMetadataBootstrap(indexMetadata, new File(localOutputDataDir));
        builder.addFileEntry(new Path(localOutputDataDir, StarTreeConstants.METADATA_FILE_NAME));
      }

      Collection<File> dimFiles = FileUtils.listFiles(new File(localOutputDataDir + "/dimensionStore"), null, true);
      for (File f : dimFiles) {
        builder.addFileEntry(new Path(f.getAbsolutePath()), "dimensionStore/"+ f.getName());
      }
      Collection<File> metricFiles = FileUtils.listFiles(new File(localOutputDataDir + "/metricStore"), null, true);
      for (File f : metricFiles) {
        builder.addFileEntry(new Path(f.getAbsolutePath()), "metricStore/"+ f.getName());
      }


      builder.finish();
      //TarGzCompressionUtils.createTarGzOfDirectory(localOutputDataDir, outputTarGz);
      Path src, dst;
      // Copy to HDFS
      src = FileSystem.getLocal(new Configuration()).makeQualified(new Path(outputTarGz));
      dst = dfs.makeQualified(new Path(hdfsOutputDir, outputTarGz));
      LOGGER.info("Copying " + src + " to " + dst);
      dfs.copyFromLocalFile(src, dst);
      LOGGER.info("END: Clean up");

      // Delete all the local files
      File f = new File(localInputDataDir);
      FileUtils.deleteDirectory(f);
      f = new File(localOutputDataDir);
      FileUtils.deleteDirectory(f);
      f = new File(localTmpDataDir);
      FileUtils.deleteDirectory(f);
      f = new File(outputTarGz);
      f.delete();
    }
  }

  public Job run() throws Exception {
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
    job.setOutputFormatClass(NullOutputFormat.class);

    String numReducers = props.getProperty("num.reducers");
    if (numReducers != null) {
      job.setNumReduceTasks(Integer.parseInt(numReducers));
    } else {
      job.setNumReduceTasks(10);
    }
    LOGGER.info("Setting number of reducers : " + job.getNumReduceTasks());

    // aggregation phase config
    Configuration configuration = job.getConfiguration();
    String inputPathDir =
        getAndSetConfiguration(configuration, STAR_TREE_BOOTSTRAP_PHASE2_INPUT_PATH);

    getAndSetConfiguration(configuration, STAR_TREE_GENERATION_OUTPUT_PATH);
    getAndSetConfiguration(configuration, STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH);
    getAndSetConfiguration(configuration, STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH);
    LOGGER.info("Input path dir: " + inputPathDir);
    for (String inputPath : inputPathDir.split(",")) {
      LOGGER.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }

    FileOutputFormat.setOutputPath(job,
        new Path(getAndCheck(STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH.toString())));

    // Optionally push to a thirdeye server
    String thirdEyeUri = props.getProperty(ThirdEyeJobConstants.THIRDEYE_SERVER_URI.getName());
    if (thirdEyeUri != null) {
      job.getConfiguration().set(ThirdEyeJobConstants.THIRDEYE_SERVER_URI.getName(), thirdEyeUri);
    }

    job.waitForCompletion(true);

    return job;
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

    StarTreeBootstrapPhaseTwoJob job =
        new StarTreeBootstrapPhaseTwoJob("star_tree_bootstrap_phase2_job", props);
    job.run();
  }
}
