package com.linkedin.thirdeye.bootstrap.startree.generation;

import static com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationConstants.STAR_TREE_GEN_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.SplitSpec;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.bootstrap.util.TarGzCompressionUtils;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryLogBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;

/**
 * @author kgopalak
 */
public class StarTreeGenerationJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeGenerationJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;
  private Properties props;

  public StarTreeGenerationJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class StarTreeGenerationMapper
      extends Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private StarTreeConfig starTreeConfig;
    private StarTreeGenerationConfig config;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    MultipleOutputs<BytesWritable, BytesWritable> mos;
    Map<String, Integer> dimensionNameToIndexMapping;
    StarTree starTree;
    String collectionName;
    private String hdfsOutputPath;
    private MetricTimeSeries emptyTimeSeries;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("StarTreeGenerationJob.StarTreeGenerationMapper.setup()");
      mos = new MultipleOutputs<BytesWritable, BytesWritable>(context);
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(STAR_TREE_GEN_CONFIG_PATH.toString()));

      try {
        starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
        config = StarTreeGenerationConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        dimensionNameToIndexMapping = new HashMap<String, Integer>();

        for (int i = 0; i < dimensionNames.size(); i++) {
          dimensionNameToIndexMapping.put(dimensionNames.get(i), i);
        }
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);

        List<MetricSpec> metricSpecs = new ArrayList<MetricSpec>();
        for (int i = 0; i < metricNames.size(); i++) {
          metricSpecs.add(new MetricSpec(metricNames.get(i), metricTypes.get(i)));
        }

        List<DimensionSpec> dimensionSpecs = new ArrayList<DimensionSpec>();
        for (String dimensionName : dimensionNames) {
          dimensionSpecs.add(new DimensionSpec(dimensionName));
        }

        // set up star tree builder
        collectionName = config.getCollectionName();
        List<String> splitOrder = config.getSplitOrder();
        int maxRecordStoreEntries = config.getSplitThreshold();
        StarTreeConfig genConfig = new StarTreeConfig.Builder()
            .setRecordStoreFactoryClass(
                StarTreeRecordStoreFactoryLogBufferImpl.class.getCanonicalName())
            .setCollection(collectionName) //
            .setDimensions(dimensionSpecs)//
            .setMetrics(metricSpecs).setTime(starTreeConfig.getTime()) //
            .setSplit(new SplitSpec(maxRecordStoreEntries, splitOrder)).setFixed(false).build();

        starTree = new StarTreeImpl(genConfig);
        starTree.open();

        hdfsOutputPath = context.getConfiguration().get(STAR_TREE_GEN_OUTPUT_PATH.toString());
        LOGGER.info(genConfig.encode());
        emptyTimeSeries = new MetricTimeSeries(metricSchema);
      } catch (Exception e) {
        throw new IOException(e);
      }

    }

    @Override
    public void map(BytesWritable dimensionKeyWritable, BytesWritable timeSeriesWritable,
        Context context) throws IOException, InterruptedException {
      // construct dimension key from raw bytes
      DimensionKey dimensionKey = DimensionKey.fromBytes(dimensionKeyWritable.copyBytes());
      StarTreeRecord record = new StarTreeRecordImpl(starTreeConfig, dimensionKey, emptyTimeSeries);
      starTree.add(record);

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      // add catch all other node under every leaf.

      LOGGER.info("START: serializing star tree and the leaf record dimension store");
      String localOutputDir = "./staging";
      // add catch all node to every leaf node

      // get the leaf nodes
      LinkedList<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
      starTree.close();
      StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTree.getRoot());
      int prevLeafNodes;
      do {
        prevLeafNodes = leafNodes.size();
        LOGGER.info("Number of leaf Nodes" + prevLeafNodes);
        for (StarTreeNode node : leafNodes) {
          // For the dimensions that are not yet split, set them to OTHER
          String[] values = new String[starTreeConfig.getDimensions().size()];
          for (int i = 0; i < starTreeConfig.getDimensions().size(); i++) {
            String dimensionName = starTreeConfig.getDimensions().get(i).getName();

            if (node.getAncestorDimensionValues().containsKey(dimensionName)) {
              values[i] = node.getAncestorDimensionValues().get(dimensionName);
            } else if (node.getDimensionName().equals(dimensionName)) {
              values[i] = node.getDimensionValue();
            } else {
              values[i] = StarTreeConstants.OTHER;
            }
          }

          // create the catch all record under this leaf node
          // TODO: the node might split after adding this record. we should
          // support a mode in star tree to stop splitting.
          StarTreeRecord record =
              new StarTreeRecordImpl(starTreeConfig, new DimensionKey(values), emptyTimeSeries);
          starTree.add(record);
        }
        // Adding a catch all node might split an existing leaf node and create
        // more leaf nodes, in which case we will have to add the catch all node
        // to the leaf nodes again. This will go on until no leaf nodes split
        // TODO: this is a temporary work around. Eventually we need to freeze
        // the tree and avoid further splitting
        leafNodes.clear();
        StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTree.getRoot());
        LOGGER.info("Number of leaf Nodes" + prevLeafNodes);

      } while (prevLeafNodes != leafNodes.size());

      context.getCounter(StarTreeGenerationCounters.NUM_LEAF_NODES)
          .setValue(starTree.getStats().getLeafCount());
      context.getCounter(StarTreeGenerationCounters.STARTREE_DEPTH)
          .setValue(StarTreeUtils.getDepth(starTree.getRoot()));
      context.getCounter(StarTreeGenerationCounters.NODE_COUNT)
          .setValue(starTree.getStats().getNodeCount());

      // close will invoke compaction
      starTree.close();

      FileSystem dfs = FileSystem.get(context.getConfiguration());
      Path src, dst;
      // generate tree and copy the tree to HDFS
      StarTreePersistanceUtil.saveTree(starTree, localOutputDir);

      String treeOutputFileName = collectionName + "-tree.bin";
      src = FileSystem.getLocal(new Configuration())
          .makeQualified(new Path(localOutputDir + "/" + treeOutputFileName));
      dst = dfs.makeQualified(new Path(hdfsOutputPath, "tree.bin"));
      LOGGER.info("Copying " + src + " to " + dst);
      dfs.copyFromLocalFile(src, dst);

      // generate and copy leaf record to HDFS
      String dimensionStoreOutputDir = localOutputDir + "/" + "dimensionStore";
      new File(dimensionStoreOutputDir).mkdirs();
      StarTreePersistanceUtil.saveLeafDimensionData(starTree, dimensionStoreOutputDir);
      LOGGER.info("END: serializing the leaf record dimension store");
      String dimensionStoreTarGz = localOutputDir + "/dimensionStore.tar.gz";
      LOGGER.info("Generating " + dimensionStoreTarGz + " from " + dimensionStoreOutputDir);
      // generate the tar file
      TarGzCompressionUtils.createTarGzOfDirectory(dimensionStoreOutputDir, dimensionStoreTarGz);
      src = FileSystem.getLocal(new Configuration()).makeQualified(new Path(dimensionStoreTarGz));
      dst = dfs.makeQualified(new Path(hdfsOutputPath, "dimensionStore.tar.gz"));
      LOGGER.info("Copying " + src + " to " + dst);
      dfs.copyFromLocalFile(src, dst);

      // Delete all the local files
      File f = new File(localOutputDir);
      FileUtils.deleteDirectory(f);
      f = new File(dimensionStoreTarGz);
      f.delete();
    }
  }

  public Job run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeGenerationJob.class);

    // Map config
    job.setMapperClass(StarTreeGenerationMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    // Reduce config
    job.setNumReduceTasks(0);
    // rollup phase 2 config
    Configuration configuration = job.getConfiguration();
    FileSystem fs = FileSystem.get(configuration);
    String inputPathDir =
        getAndSetConfiguration(configuration, StarTreeGenerationConstants.STAR_TREE_GEN_INPUT_PATH);
    getAndSetConfiguration(configuration, StarTreeGenerationConstants.STAR_TREE_GEN_CONFIG_PATH);
    Path outputPath = new Path(getAndSetConfiguration(configuration, StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH));
    LOGGER.info("Running star tree generation job");
    LOGGER.info("Input path dir: " + inputPathDir);
    for (String inputPath : inputPathDir.split(",")) {
      LOGGER.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }

    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);
    LOGGER.info("Finished running star tree generation job");

    Counters counters = job.getCounters();
    for (Enum e : StarTreeGenerationCounters.values()) {
      Counter counter = counters.findCounter(e);
      LOGGER.info(counter.getDisplayName() + " : " + counter.getValue());
    }

    return job;

  }

  private String getAndSetConfiguration(Configuration configuration,
      StarTreeGenerationConstants constant) {
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

  public static enum StarTreeGenerationCounters {
    STARTREE_DEPTH,
    NUM_LEAF_NODES,
    NODE_COUNT
  }
}
