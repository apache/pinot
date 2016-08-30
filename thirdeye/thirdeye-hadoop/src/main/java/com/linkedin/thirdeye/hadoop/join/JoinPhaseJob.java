package com.linkedin.thirdeye.hadoop.join;

import static com.linkedin.thirdeye.hadoop.join.JoinPhaseConstants.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

/**
 * This is a generic join job that can be used to prepare the data for Third
 * Eye. Many teams just need a way to join multiple data sets into one.
 * Currently they do this by using pig script which is highly inefficient, since
 * it does a pair wise join. The idea is as follows there are N named sources,
 * there is a join key common across all these sources. <br/>
 * S1: join key s1_key <br/>
 * S2: join key s2_key <br/>
 * ... <br/>
 * SN: join key sn_key<br/>
 */
public class JoinPhaseJob extends Configured {
  private static final Logger LOGGER = LoggerFactory.getLogger(JoinPhaseJob.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private String name;
  private Properties props;

  public JoinPhaseJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class GenericJoinMapper
      extends Mapper<AvroKey<GenericRecord>, NullWritable, BytesWritable, BytesWritable> {
    String sourceName;
    JoinKeyExtractor joinKeyExtractor;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      LOGGER.info("GenericAvroJoinJob.GenericJoinMapper.setup()");
      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      LOGGER.info("split name:" + fileSplit.toString());
      Configuration configuration = context.getConfiguration();

      try {
        sourceName = DelegatingAvroKeyInputFormat.getSourceNameFromPath(fileSplit, configuration);
        LOGGER.info("Input: {} belongs to Source:{}", fileSplit, sourceName);
        String joinKeyExtractorClass = configuration.get(JOIN_KEY_EXTRACTOR_CLASS.toString());

        Map<String, String> params = new HashMap<>();
        List<String> sourceNames = Lists.newArrayList(configuration.get(JOIN_SOURCE_NAMES.toString()).split(","));
        for (String sourceName : sourceNames) {
          String joinKeyExtractorConfig = configuration.get(sourceName + "." + JOIN_KEY_EXTRACTOR_CONFIG.toString());
          if (StringUtils.isNotBlank(joinKeyExtractorConfig)) {
            params.put(sourceName, joinKeyExtractorConfig);
          }
        }
        LOGGER.info("Initializing JoinKeyExtractorClass:{} with params:{}", joinKeyExtractorClass, params);
        Constructor<?> constructor = Class.forName(joinKeyExtractorClass).getConstructor(Map.class);
        joinKeyExtractor = (JoinKeyExtractor) constructor.newInstance(params);
      } catch (Exception e) {
        throw new IOException(e);
      }

    }

    @Override
    public void map(AvroKey<GenericRecord> recordWrapper, NullWritable value, Context context)
        throws IOException, InterruptedException {
      GenericRecord record = recordWrapper.datum();
      MapOutputValue mapOutputValue = new MapOutputValue(record.getSchema().getName(), record);
      String joinKeyValue = joinKeyExtractor.extractJoinKey(sourceName, record);
      LOGGER.info("Join Key:{}", joinKeyValue);

      if (!"INVALID".equals(joinKeyValue)) {
        context.write(new BytesWritable(joinKeyValue.toString().getBytes()),
            new BytesWritable(mapOutputValue.toBytes()));
      }
    }

  }

  public static class GenericJoinReducer
      extends Reducer<BytesWritable, BytesWritable, AvroKey<GenericRecord>, NullWritable> {

    String statOutputDir;
    private FileSystem fileSystem;
    private static TypeReference MAP_STRING_STRING_TYPE = new TypeReference<Map<String, String>>() {
    };
    private Map<String, Schema> schemaMap = new HashMap<String, Schema>();
    private JoinUDF joinUDF;
    private Map<String, AtomicInteger> countersMap = new HashMap<String, AtomicInteger>();
    private List<String> sourceNames;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration configuration = context.getConfiguration();
      fileSystem = FileSystem.get(configuration);

      try {

        Map<String, String> schemaJSONMapping = new ObjectMapper().readValue(
            context.getConfiguration().get("schema.json.mapping"), MAP_STRING_STRING_TYPE);

        LOGGER.info("Schema JSON Mapping: {}", schemaJSONMapping);
        for (String sourceName : schemaJSONMapping.keySet()) {
          Schema schema = new Schema.Parser().parse(schemaJSONMapping.get(sourceName));
          schemaMap.put(sourceName, schema);
        }
        sourceNames = Lists.newArrayList(configuration.get(JOIN_SOURCE_NAMES.toString()).split(","));
        String joinUDFClass = configuration.get(JOIN_UDF_CLASS.toString());
        Map<String, String> params = new HashMap<>();
        for (String sourceName : sourceNames) {
          String joinUdfConfig = configuration.get(sourceName + "." + JOIN_UDF_CONFIG.toString());
          if (StringUtils.isNotBlank(joinUdfConfig)) {
            params.put(sourceName, joinUdfConfig);
          }
        }

        Constructor<?> constructor = Class.forName(joinUDFClass).getConstructor(Map.class);
        LOGGER.info("Initializing JoinUDFClass:{} with params:{}", joinUDFClass, params);
        joinUDF = (JoinUDF) constructor.newInstance(params);
        String outputSchemaPath = configuration.get(JOIN_OUTPUT_SCHEMA.toString());
        // Avro schema
        Schema.Parser parser = new Schema.Parser();
        Schema outputSchema = parser.parse(fileSystem.open(new Path(outputSchemaPath)));
        LOGGER.info("Setting outputschema:{}", outputSchema);
        joinUDF.init(outputSchema);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }

    @Override
    public void reduce(BytesWritable joinKeyWritable, Iterable<BytesWritable> recordBytesWritable,
        Context context) throws IOException, InterruptedException {
      Map<String, List<GenericRecord>> joinInput = new HashMap<String, List<GenericRecord>>();
      for (BytesWritable writable : recordBytesWritable) {

        byte[] bytes = writable.copyBytes();
        MapOutputValue mapOutputValue = MapOutputValue.fromBytes(bytes, schemaMap);
        String schemaName = mapOutputValue.getSchemaName();
        if (!joinInput.containsKey(schemaName)) {
          joinInput.put(schemaName, new ArrayList<GenericRecord>());
        }
        joinInput.get(schemaName).add(mapOutputValue.getRecord());
      }

      int[] exists = new int[sourceNames.size()];
      for (int i = 0; i < sourceNames.size(); i++) {
        String source = sourceNames.get(i);
        if (joinInput.containsKey(source)) {
          exists[i] = 1;
        } else {
          exists[i] = 0;
        }
      }
      String counterName = Arrays.toString(exists);
      if (!countersMap.containsKey(counterName)) {
        countersMap.put(counterName, new AtomicInteger(0));
      }
      countersMap.get(counterName).incrementAndGet();
      // invoke the udf and pass in the join data
      List<GenericRecord> outputRecords =
          joinUDF.performJoin(new String(joinKeyWritable.copyBytes()), joinInput);
      if (outputRecords != null) {
        for (GenericRecord outputRecord : outputRecords) {
          context.write(new AvroKey<GenericRecord>(outputRecord), NullWritable.get());
        }
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (String counterName : countersMap.keySet()) {
        context.getCounter("DynamicCounter", counterName)
            .increment(countersMap.get(counterName).get());
      }
    }
  }

  public Job run() throws Exception {
    Job job = Job.getInstance(getConf());
    Configuration conf = job.getConfiguration();
    job.setJobName(name);
    job.setJarByClass(JoinPhaseJob.class);

    FileSystem fs = FileSystem.get(conf);

    String outputSchemaPath = getAndSetConfiguration(conf, JOIN_OUTPUT_SCHEMA);
    Schema.Parser parser = new Schema.Parser();
    Schema outputSchema = parser.parse(fs.open(new Path(outputSchemaPath)));
    LOGGER.info("{}", outputSchema);

    // Set custom config like adding distributed caches
    String joinConfigUDFClass = getAndSetConfiguration(conf, JoinPhaseConstants.JOIN_CONFIG_UDF_CLASS);
    LOGGER.info("Initializing JoinConfigUDFClass:{} with params:{}", joinConfigUDFClass);
    Constructor<?> constructor = Class.forName(joinConfigUDFClass).getConstructor();
    JoinConfigUDF joinConfigUDF = (JoinConfigUDF) constructor.newInstance();
    joinConfigUDF.setJoinConfig(job);
    getAndSetConfiguration(conf, JOIN_KEY_EXTRACTOR_CLASS);
    getAndSetConfiguration(conf, JOIN_UDF_CLASS);

    List<String> sourceNames = Lists.newArrayList(
        getAndSetConfiguration(conf, JoinPhaseConstants.JOIN_SOURCE_NAMES).split(","));

    // Map config
    job.setMapperClass(GenericJoinMapper.class);
    // AvroJob.setInputKeySchema(job, unionSchema);
    job.setInputFormatClass(DelegatingAvroKeyInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    job.setReducerClass(GenericJoinReducer.class);
    AvroJob.setOutputKeySchema(job, outputSchema);
    job.setOutputFormatClass(AvroKeyOutputFormat.class);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);

    String numReducers = props.getProperty("num.reducers");
    if (numReducers != null) {
      job.setNumReduceTasks(Integer.parseInt(numReducers));
    } else {
      job.setNumReduceTasks(10);
    }
    LOGGER.info("Setting number of reducers : " + job.getNumReduceTasks());
    Map<String, String> schemaMap = new HashMap<String, String>();
    Map<String, String> schemaPathMapping = new HashMap<String, String>();

    for (String sourceName : sourceNames) {
      // load schema for each source
      LOGGER.info("Loading Schema for {}", sourceName);

      FSDataInputStream schemaStream =
          fs.open(new Path(getAndCheck(sourceName + "." + JOIN_INPUT_SCHEMA.toString())));
      Schema schema = new Schema.Parser().parse(schemaStream);
      schemaMap.put(sourceName, schema.toString());
      LOGGER.info("Schema for {}:  \n{}", sourceName, schema);

      // configure input data for each source
      String inputPathDir = getAndCheck(sourceName + "." + JOIN_INPUT_PATH.toString());
      LOGGER.info("Input path dir for " + sourceName + ": " + inputPathDir);
      for (String inputPath : inputPathDir.split(",")) {
        Path input = new Path(inputPath);
        FileStatus[] listFiles = fs.listStatus(input);
        boolean isNested = false;
        for (FileStatus fileStatus : listFiles) {
          if (fileStatus.isDirectory()) {
            isNested = true;
            Path path = fileStatus.getPath();
            LOGGER.info("Adding input:" + path);
            FileInputFormat.addInputPath(job, path);
            schemaPathMapping.put(path.toString(), sourceName);
          }
        }
        if (!isNested) {
          LOGGER.info("Adding input:" + inputPath);
          FileInputFormat.addInputPath(job, input);
          schemaPathMapping.put(input.toString(), sourceName);
        }
      }
    }
    StringWriter temp = new StringWriter();
    OBJECT_MAPPER.writeValue(temp, schemaPathMapping);
    job.getConfiguration().set("schema.path.mapping", temp.toString());

    temp = new StringWriter();
    OBJECT_MAPPER.writeValue(temp, schemaMap);
    job.getConfiguration().set("schema.json.mapping", temp.toString());

    Path outputPath = new Path(getAndCheck(JOIN_OUTPUT_PATH.toString()));
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }
    FileOutputFormat.setOutputPath(job, new Path(getAndCheck(JOIN_OUTPUT_PATH.toString())));

    for (Object key : props.keySet()) {
      conf.set(key.toString(), props.getProperty(key.toString()));
    }

    job.waitForCompletion(true);

    dumpSummary(job, sourceNames);

    return job;
  }

  private void dumpSummary(Job job, List<String> sourceNames) throws IOException {
    System.out.println("Join Input Matrix.");
    CounterGroup group = job.getCounters().getGroup("DynamicCounter");
    for (String source : sourceNames) {
      System.out.print(String.format("%25s\t", source));
    }
    if (group != null) {
      Iterator<Counter> iterator = group.iterator();
      while (iterator.hasNext()) {
        Counter counter = iterator.next();
        String displayName = counter.getDisplayName();
        String[] split = displayName.replace("[", "").replace("[", "").split(",");
        for (String str : split) {
          if (str.trim().equals("1")) {
            System.out.print(String.format("%25s\t", "1"));
          } else {
            System.out.print(String.format("%25s\t", "-"));
          }
        }
      }
    }
  }

  private String getAndSetConfiguration(Configuration configuration,
      JoinPhaseConstants constant) {
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

    JoinPhaseJob job = new JoinPhaseJob("aggregate_avro_job", props);
    job.run();
  }

}
