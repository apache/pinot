package com.linkedin.thirdeye.bootstrap.rollup.phase2;

import static com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoConstants.ROLLUP_PHASE2_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoConstants.ROLLUP_PHASE2_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoConstants.ROLLUP_PHASE2_OUTPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoConstants.ROLLUP_PHASE2_ANALYSIS_PATH;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.linkedin.thirdeye.api.StarTreeConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.bootstrap.aggregation.DimensionStats;
import com.linkedin.thirdeye.bootstrap.analysis.AnalysisPhaseStats;

public class RollupPhaseTwoJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(RollupPhaseTwoJob.class);

  private String name;
  private Properties props;

  public RollupPhaseTwoJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class RollupPhaseTwoMapper extends
      Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private RollupPhaseTwoConfig config;
    private List<String> dimensionNames;
    private List<String> metricNames;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    private List<String> rollupOrder;
    Map<String, Integer> dimensionNameToIndexMapping;
    BytesWritable keyWritable;
    BytesWritable valWritable;
    private Path analysisResults;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("RollupPhaseOneJob.RollupPhaseOneMapper.setup()");
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(ROLLUP_PHASE2_CONFIG_PATH.toString()));
      analysisResults = new Path(configuration.get(ROLLUP_PHASE2_ANALYSIS_PATH.toString()));
      try {
        StarTreeConfig starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
        config = RollupPhaseTwoConfig.fromStarTreeConfig(starTreeConfig);
        dimensionNames = config.getDimensionNames();
        dimensionNameToIndexMapping = new HashMap<String, Integer>();

        for (int i = 0; i < dimensionNames.size(); i++) {
          dimensionNameToIndexMapping.put(dimensionNames.get(i), i);
        }
        metricNames = config.getMetricNames();
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        rollupOrder = config.getRollupOrder();
        if (rollupOrder == null) {
          getRollupOrder();
        }
        keyWritable = new BytesWritable();
        valWritable = new BytesWritable();

      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void map(BytesWritable dimensionKeyWritable, BytesWritable metricTimeSeriesWritable,
        Context context) throws IOException, InterruptedException {

      DimensionKey rawDimensionKey;
      rawDimensionKey = DimensionKey.fromBytes(dimensionKeyWritable.copyBytes());
      MetricTimeSeries rawTimeSeries;
      byte[] bytes = metricTimeSeriesWritable.copyBytes();
      rawTimeSeries = MetricTimeSeries.fromBytes(bytes, metricSchema);
      // generate all combinations from the original dimension.

      // LOGGER.info("Map.key {}", rawDimensionKey);
      List<DimensionKey> combinations = generateCombinations(rawDimensionKey);
      // LOGGER.info("combinations:{}", combinations);

      for (DimensionKey combination : combinations) {
        // key
        byte[] md5 = combination.toMD5();
        keyWritable.set(md5, 0, md5.length);
        // value
        RollupPhaseTwoMapOutput wrapper;
        wrapper = new RollupPhaseTwoMapOutput(combination, rawDimensionKey, rawTimeSeries);
        byte[] valBytes = wrapper.toBytes();
        valWritable.set(valBytes, 0, valBytes.length);
        // LOGGER.info("Map.combination:{}", combination);
        // LOGGER.info("Map.raw Dimension:{}", wrapper.dimensionKey);
        context.write(keyWritable, valWritable);
      }

    }

    private List<DimensionKey> generateCombinations(DimensionKey dimensionKey) throws IOException {
      String[] dimensionsValues = dimensionKey.getDimensionValues();
      List<DimensionKey> combinations = new ArrayList<DimensionKey>();
      String[] comb = Arrays.copyOf(dimensionsValues, dimensionsValues.length);

      for (String dimensionToRollup : rollupOrder) {
        comb = Arrays.copyOf(comb, comb.length);
        comb[dimensionNameToIndexMapping.get(dimensionToRollup)] = "?";
        combinations.add(new DimensionKey(comb));
      }
      return combinations;
    }

    /**
     * Get rollup order from dimension stats results
     */
    private void getRollupOrder() throws IOException {

      try {
        FileSystem fileSystem = FileSystem.get(new Configuration());

        DimensionStats aggDimensionStats = new DimensionStats();
        ObjectMapper objectMapper = new ObjectMapper();

        FileStatus[] dimensionStatsFiles = fileSystem.listStatus(analysisResults);
        for (FileStatus fileStatus : dimensionStatsFiles) {
          Path dimensionStatsFile = fileStatus.getPath();
          DimensionStats dimensionStats = objectMapper.readValue(new InputStreamReader(fileSystem.open(dimensionStatsFile)), DimensionStats.class);
          aggDimensionStats.update(dimensionStats);
        }

        SortedSet<Entry<String, Set<String>>> sortedDimensionCardinality =
            new TreeSet<Entry<String, Set<String>>> (new Comparator<Entry<String, Set<String>>>() {

             @Override
             public int compare(Entry<String, Set<String>> dimension1, Entry<String, Set<String>> dimension2) {
               return (new Integer(dimension2.getValue().size())).compareTo(dimension1.getValue().size());
             }
       });
        sortedDimensionCardinality.addAll(aggDimensionStats.getDimensionValues().entrySet());
        LOGGER.info("Sorted order {}", sortedDimensionCardinality);

        Iterator<Entry<String, Set<String>>> it = sortedDimensionCardinality.iterator();
        rollupOrder = new ArrayList<String>();
        while (it.hasNext()) {
          rollupOrder.add(it.next().getKey());
        }
      } catch (Exception e){
        // Continue with earlier rollup config
        LOGGER.info("exception {}", e);
      }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
    }

  }

  public static class RollupPhaseTwoReducer extends
      Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    private RollupPhaseTwoConfig config;
    private List<MetricType> metricTypes;
    private MetricSchema metricSchema;
    BytesWritable keyWritable;
    BytesWritable valWritable;
    Map<DimensionKey, MetricTimeSeries> map;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration configuration = context.getConfiguration();
      FileSystem fileSystem = FileSystem.get(configuration);
      Path configPath = new Path(configuration.get(ROLLUP_PHASE2_CONFIG_PATH.toString()));
      try {
        StarTreeConfig starTreeConfig = StarTreeConfig.decode(fileSystem.open(configPath));
        config = RollupPhaseTwoConfig.fromStarTreeConfig(starTreeConfig);
        metricTypes = config.getMetricTypes();
        metricSchema = new MetricSchema(config.getMetricNames(), metricTypes);
        keyWritable = new BytesWritable();
        valWritable = new BytesWritable();
        map = new HashMap<DimensionKey, MetricTimeSeries>();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(BytesWritable rollupDimensionKeyMD5Writable,
        Iterable<BytesWritable> rollupMapOutputWritableIterable, Context context)
        throws IOException, InterruptedException {
      DimensionKey rollupDimensionKey = null;
      MetricTimeSeries rollupTimeSeries = new MetricTimeSeries(metricSchema);
      // LOGGER.info("rollup Dimension:{}", rollupDimensionKey);
      map.clear();
      for (BytesWritable writable : rollupMapOutputWritableIterable) {
        RollupPhaseTwoMapOutput temp;
        temp = RollupPhaseTwoMapOutput.fromBytes(writable.copyBytes(), metricSchema);
        if (rollupDimensionKey == null) {
          rollupDimensionKey = temp.getRollupDimensionKey();
        }
        // LOGGER.info("temp.dimensionKey:{}", temp.dimensionKey);
        map.put(temp.rawDimensionKey, temp.getRawTimeSeries());
        rollupTimeSeries.aggregate(temp.getRawTimeSeries());
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(String.format("processing key :%s, size:%d",
            rollupDimensionKeyMD5Writable.toString(), map.size()));
      }
      for (Entry<DimensionKey, MetricTimeSeries> entry : map.entrySet()) {
        RollupPhaseTwoReduceOutput output;
        output =
            new RollupPhaseTwoReduceOutput(rollupDimensionKey, rollupTimeSeries, entry.getKey(),
                entry.getValue());
        // key
        byte[] md5 = entry.getKey().toMD5();
        keyWritable.set(md5, 0, md5.length);
        // value
        byte[] outBytes = output.toBytes();
        valWritable.set(outBytes, 0, outBytes.length);
        context.write(keyWritable, valWritable);
        // LOGGER.info("Phase 2 raw dimension:{}, raw dimension timeseries:{}",
        // entry.getKey(), entry.getValue());
        // LOGGER.info("Phase 2 Reduce output value rollup dimension {}, timeseries:{}",
        // rollupDimensionKey,rollupTimeSeries );
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(String.format("end processing key :%s", rollupDimensionKeyMD5Writable.toString()));
      }
    }
  }

  public Job run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(RollupPhaseTwoJob.class);

    // Map config
    job.setMapperClass(RollupPhaseTwoMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    // job.setCombinerClass(RollupPhaseTwoReducer.class);
    job.setReducerClass(RollupPhaseTwoReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    String numReducers = props.getProperty("num.reducers");
    if (numReducers != null) {
      job.setNumReduceTasks(Integer.parseInt(numReducers));
    } else {
      job.setNumReduceTasks(10);
    }
    LOGGER.info("Setting number of reducers : " + job.getNumReduceTasks());
    // rollup phase 2 config
    Configuration configuration = job.getConfiguration();
    String inputPathDir = getAndSetConfiguration(configuration, ROLLUP_PHASE2_INPUT_PATH);
    getAndSetConfiguration(configuration, ROLLUP_PHASE2_CONFIG_PATH);
    getAndSetConfiguration(configuration, ROLLUP_PHASE2_OUTPUT_PATH);
    getAndSetConfiguration(configuration, ROLLUP_PHASE2_ANALYSIS_PATH);
    LOGGER.info("Input path dir: " + inputPathDir);
    for (String inputPath : inputPathDir.split(",")) {
      LOGGER.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }

    FileOutputFormat
        .setOutputPath(job, new Path(getAndCheck(ROLLUP_PHASE2_OUTPUT_PATH.toString())));

    job.waitForCompletion(true);

    return job;
  }

  private String getAndSetConfiguration(Configuration configuration,
      RollupPhaseTwoConstants constant) {
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

}
