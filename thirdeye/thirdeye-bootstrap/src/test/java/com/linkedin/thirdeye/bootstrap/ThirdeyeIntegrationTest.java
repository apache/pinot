package com.linkedin.thirdeye.bootstrap;

import static org.junit.Assert.assertTrue;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.ThirdEyeApplication;
import com.linkedin.thirdeye.tools.DataGeneratorConfig;
import com.linkedin.thirdeye.tools.DataGeneratorTool;
import com.linkedin.thirdeye.util.DropWizardApplicationRunner;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.bootstrap.ThirdEyeJob.FlowSpec;
import com.linkedin.thirdeye.bootstrap.aggregation.AggregatePhaseJob;
import com.linkedin.thirdeye.bootstrap.aggregation.AggregatePhaseJob.*;
import com.linkedin.thirdeye.bootstrap.rollup.phase1.RollupPhaseOneJob;
import com.linkedin.thirdeye.bootstrap.rollup.phase1.RollupPhaseOneJob.*;
import com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoJob;
import com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoJob.*;
import com.linkedin.thirdeye.bootstrap.rollup.phase3.RollupPhaseThreeJob;
import com.linkedin.thirdeye.bootstrap.rollup.phase3.RollupPhaseThreeJob.*;
import com.linkedin.thirdeye.bootstrap.rollup.phase4.RollupPhaseFourJob;
import com.linkedin.thirdeye.bootstrap.rollup.phase4.RollupPhaseFourJob.*;
import com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationJob;
import com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationJob.*;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneJob;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneJob.*;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoJob;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoJob.*;

import static com.linkedin.thirdeye.bootstrap.aggregation.AggregationJobConstants.*;
import static com.linkedin.thirdeye.bootstrap.rollup.phase1.RollupPhaseOneConstants.*;
import static com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoConstants.*;
import static com.linkedin.thirdeye.bootstrap.rollup.phase3.RollupPhaseThreeConstants.*;
import static com.linkedin.thirdeye.bootstrap.rollup.phase4.RollupPhaseFourConstants.*;
import static com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationConstants.*;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneConstants.*;
import static com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoConstants.*;

import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoConstants;
import com.linkedin.thirdeye.client.ThirdEyeRawResponse;



public class ThirdeyeIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdeyeIntegrationTest.class);

  protected static MiniDFSCluster cluster = null;
  protected static DistributedFileSystem fs = null;
  protected static Configuration conf = new Configuration();
  protected static int numDataNodes = 5;
  protected static int replicationFactor = 3;
  protected static long blockSize = (long) Math.pow(2, 20); // should be power of 2

  private File rootDir;

  private static String ROOT;
  private static String COLLECTION;
  private static DateTime MIN_TIME;
  private static DateTime MAX_TIME;
  private static String INPUT_DIR;
  private static String DATA_DIR;
  private static String THIRDEYE_SERVER;

  private static final String CONFIG_FILE = "config.yml";
  private static final String SCHEMA_FILE = "schema.avsc";
  private static final String PROPERTIES_FILE = "integrationTest.properties";

  private static Path inputFilePath;
  private static Path configFilePath;
  private static Path schemaFilePath;
  private static File propertiesFile;
  Properties properties;
  Path aggregationOutputPath;
  Path dimensionStatsPath;
  Path rollupPhase1OutputPath;
  Path belowThresholdPath;
  Path aboveThresholdPath;
  Path rollupPhase2OutputPath;
  Path rollupPhase3OutputPath;
  Path rollupPhase4OutputPath;
  Path starTreeGenerationOutput;
  Path starTreeBootstrapPhase1Output;
  Path starTreeBootstrapPhase2Output;
  Path serverPackageOutput;


  @BeforeClass
  public static void setup() throws InterruptedException, IOException {
    if (cluster == null) {
      conf.set("dfs.namenode.replication.min", "" + replicationFactor);
      conf.set("dfs.block.size", "" + blockSize);
      conf.set("io.bytes.per.checksum", "" + 4);
      cluster = new MiniDFSCluster
          .Builder(conf)
          .numDataNodes(numDataNodes)
          .build();
      fs = cluster.getFileSystem();
    }
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Before
  public void beforeMethod() throws Exception {

    propertiesFile = new File(getClass().getClassLoader().getResource("integrationTest/"+PROPERTIES_FILE).getFile());
    properties = new Properties();
    properties.load(new InputStreamReader(new FileInputStream(propertiesFile)));

    ROOT = properties.getProperty(ThirdEyeJobConstants.THIRDEYE_ROOT.getName());
    COLLECTION = properties.getProperty(ThirdEyeJobConstants.THIRDEYE_COLLECTION.getName());
    String minTimeProp = properties.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_MIN.getName());
    String maxTimeProp = properties.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_MAX.getName());
    MIN_TIME = ISODateTimeFormat.dateTimeParser().parseDateTime(minTimeProp);
    MAX_TIME = ISODateTimeFormat.dateTimeParser().parseDateTime(maxTimeProp);
    INPUT_DIR = properties.getProperty(ThirdEyeJobConstants.INPUT_PATHS.getName());
    DATA_DIR = properties.getProperty(ThirdEyeJobConstants.THIRDEYE_DIMENSION_INDEX_REF.getName());
    THIRDEYE_SERVER = properties.getProperty(ThirdEyeJobConstants.THIRDEYE_SERVER_URI.getName());

    rootDir = new File(System.getProperty("java.io.tmpdir"), ThirdeyeIntegrationTest.class.getName());

    try { FileUtils.forceDelete(rootDir); } catch (Exception e) { }
    try { FileUtils.forceMkdir(rootDir); } catch (Exception e) {}
  }

  @After
  public void afterMethod() throws Exception {
    try { FileUtils.forceDelete(rootDir); } catch (Exception e) {  }
  }


 @Test
  public void testEndToEndIntegration() throws Exception {

    LOGGER.info("End to end integration testing starting");

    // data generation
    testDataGeneration();

    // aggreggation
    testAggregation();

    // RollupPhase1
    testRollupPhase1();

    // RollupPhase2
    testRollupPhase2();

    // RollupPhase3
    testRollupPhase3();

    // RollupPhase4
    testRollupPhase4();

    // StartreeGeneration
    testStartreeGeneration();

    // StartreeBootstrapPhase1
    testStartreeBootstrapPhase1();

    // StartreeBootstrapPhase2
    testStartreeBootstrapPhase2();

    // ServerPackage
    testServerPackage();

    // Start and test server
    testCreateServer();

    LOGGER.info("End to end integration testing completed");
}


   private void checkData() throws IOException {

     long metric1SumActual = 0;
     long metric1SumExpected = 0;

     // calculate metric1 from /query
     String sql = "SELECT metric1 FROM " + COLLECTION + " WHERE time BETWEEN '" +getDateString(MIN_TIME)+ "' AND '" + getDateString(MAX_TIME) +"'";
     URL url = new URL(THIRDEYE_SERVER + "/query/" + URLEncoder.encode(sql, "UTF-8"));
     ObjectMapper OBJECT_MAPPER = new ObjectMapper();
     ThirdEyeRawResponse queryResult =  OBJECT_MAPPER.readValue((new InputStreamReader(url.openStream(), "UTF-8")), ThirdEyeRawResponse.class);
     for (Entry<String, Map<String, Number[]>> entry : queryResult.getData().entrySet()) {
       for (Entry<String, Number[]> metricEntry : entry.getValue().entrySet()) {
         metric1SumActual += metricEntry.getValue()[0].longValue();
       }
     }

     // calculate metric1 from input files
     File schemaFile = new File(getClass().getClassLoader().getResource("integrationTest/"+SCHEMA_FILE).getFile());
     Schema schema = new Schema.Parser().parse(schemaFile);
     GenericRecord record = new GenericData.Record(schema);
     File avroDataInput = new File(rootDir, COLLECTION);
     avroDataInput = new File(avroDataInput, INPUT_DIR);

     for (File avroFile : avroDataInput.listFiles()) {
       DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
       DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(avroFile, datumReader);

       while (dataFileReader.hasNext()) {
         record = dataFileReader.next();
         long metric1 = (long) record.get("metric1");
         metric1SumExpected += metric1;
       }
     }
     assertTrue("Data mismatch between input files and server", metric1SumActual == metric1SumExpected);
   }


  private void testServerUpload() throws Exception {

    LOGGER.info("Starting server_upload job");

    // server_upload
    new ThirdEyeJob("server_upload", properties).serverUpload(fs, ROOT, COLLECTION, FlowSpec.METRIC_INDEX, MIN_TIME, MAX_TIME);

    // restore
    String uri = "http://localhost:8081/tasks/restore?collection=" + COLLECTION;
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    HttpPost postRequest = new HttpPost(uri);
    HttpResponse response = httpClient.execute(postRequest);
    httpClient.close();
    assertTrue("failed to restore collection "+COLLECTION, response.getStatusLine().getStatusCode() == 200);

    // check
    uri = THIRDEYE_SERVER + "/collections/" + COLLECTION;
    httpClient = HttpClientBuilder.create().build();
    HttpGet getRequest = new HttpGet(uri);
    response = httpClient.execute(getRequest);
    httpClient.close();
    assertTrue("failed to upload to server", response.getStatusLine().getStatusCode() == 200);

    LOGGER.info("server_upload job completed");
  }

  private void testCreateServer() throws Exception
  {

    // start server
    File ROOT_DIR = new File(rootDir, "thirdeye-server");
    int REQUEST_TIMEOUT_MILLIS = 10000;
    ThirdEyeApplication.Config config = new ThirdEyeApplication.Config();
    config.setRootDir(ROOT_DIR.getAbsolutePath());

    DropWizardApplicationRunner.DropWizardServer<ThirdEyeApplication.Config> server
            = DropWizardApplicationRunner.createServer(config, ThirdEyeApplication.class);

    assertTrue(server.getMetricRegistry() != null);

    server.start();

    // Try to contact it
    long startTime = System.currentTimeMillis();
    boolean success = false;
    while (System.currentTimeMillis() - startTime < REQUEST_TIMEOUT_MILLIS)
    {
      try
      {
        HttpURLConnection conn = (HttpURLConnection) URI.create("http://localhost:8080/admin").toURL().openConnection();
        byte[] res = IOUtils.toByteArray(conn.getInputStream());
        if (Arrays.equals(res, "GOOD".getBytes()))
        {
          success = true;
          break;
        }
      }
      catch (Exception e)
      {
        // Re-try
      }
    }
    assertTrue(success);

    // server upload
    testServerUpload();

    // test data integrity
    checkData();

    // stop server
    server.stop();
  }

  private void testServerPackage() throws Exception {

    LOGGER.info("Starting server_package job");

    // server_package
    new ThirdEyeJob("server_package", properties).serverPackage(fs, ROOT, COLLECTION, FlowSpec.METRIC_INDEX, MIN_TIME, MAX_TIME);
    assertTrue("failed to create data.tar.gz in server_package", fs.exists(new Path(serverPackageOutput, "data.tar.gz")));

    LOGGER.info("server_package job completed");
  }

  private void testStartreeBootstrapPhase2() throws ClassNotFoundException, IOException, InterruptedException {
    LOGGER.info("Starting startree_bootstrap_phase2 job");

    // Startree Bootstrap Phase 2
    Job job = Job.getInstance(conf);
    job.setJobName("StartreeBootstrapPhase2");
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
    job.setNumReduceTasks(10);

    // startree bootstrap phase2 phase config
    Configuration configuration = job.getConfiguration();
    configuration.set(STAR_TREE_BOOTSTRAP_PHASE2_INPUT_PATH.toString(), starTreeBootstrapPhase1Output.toString());
    configuration.set(StarTreeBootstrapPhaseTwoConstants.STAR_TREE_GENERATION_OUTPUT_PATH.toString(), starTreeGenerationOutput.toString());
    configuration.set(STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH.toString(), configFilePath.toString());
    configuration.set(STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH.toString(), starTreeBootstrapPhase2Output.toString());

    // execute job
    FileInputFormat.addInputPath(job, new Path(configuration.get(STAR_TREE_BOOTSTRAP_PHASE2_INPUT_PATH.toString())));
    FileOutputFormat.setOutputPath(job, new Path(configuration.get(STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH.toString())));
    job.waitForCompletion(true);

    // tests
    assertTrue("startree_bootstrap_phase2 job failed", job.isSuccessful());
    assertTrue("startree_bootstrap_phase2 folder not created", fs.exists(starTreeBootstrapPhase2Output));
    FileStatus[] startreeBootstrapPhase2Status = fs.listStatus(starTreeBootstrapPhase2Output, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith("data");
      }
    });
    assertTrue("data.tar.gz not generated in startree_bootstrap_phase2", startreeBootstrapPhase2Status.length != 0);

    LOGGER.info("startree_bootstrap_phase2 job completed");
  }

  private void testStartreeBootstrapPhase1() throws ClassNotFoundException, IOException, InterruptedException {
    LOGGER.info("Starting startree_bootstrap_phase1 job");

    // Startree Bootstrap Phase1
    Job job = Job.getInstance(conf);
    job.setJobName("StartreeBootstrapPhase1");
    job.setJarByClass(StarTreeBootstrapPhaseOneJob.class);

    // Avro schema
    Schema schema = new Schema.Parser().parse(fs.open(schemaFilePath));

    // Map config
    job.setMapperClass(BootstrapMapper.class);
    AvroJob.setInputKeySchema(job, schema);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);
    job.setPartitionerClass(NodeIdBasedPartitioner.class);

    // Reduce config
    job.setReducerClass(StarTreeBootstrapReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setNumReduceTasks(10);

    // star tree bootstrap phase 1 config
    Configuration configuration = job.getConfiguration();
    configuration.set(STAR_TREE_BOOTSTRAP_INPUT_PATH.toString(), inputFilePath.toString());
    configuration.set(StarTreeBootstrapPhaseTwoConstants.STAR_TREE_GENERATION_OUTPUT_PATH.toString(), starTreeGenerationOutput.toString());
    configuration.set(STAR_TREE_BOOTSTRAP_CONFIG_PATH.toString(), configFilePath.toString());
    configuration.set(STAR_TREE_BOOTSTRAP_OUTPUT_PATH.toString(), starTreeBootstrapPhase1Output.toString());
    configuration.set(STAR_TREE_BOOTSTRAP_INPUT_AVRO_SCHEMA.toString(), schemaFilePath.toString());

    // execute job
    FileInputFormat.addInputPath(job, new Path(configuration.get(STAR_TREE_BOOTSTRAP_INPUT_PATH.toString())));
    FileOutputFormat.setOutputPath(job, new Path(configuration.get(STAR_TREE_BOOTSTRAP_OUTPUT_PATH.toString())));
    job.waitForCompletion(true);

    // tests
    assertTrue("startree_bootstrap_phase1 job failed", job.isSuccessful());
    assertTrue("startree_bootstrap_phase1 folder not created", fs.exists(starTreeBootstrapPhase1Output));
    FileStatus[] startreeBootstrapPhase1Status = fs.listStatus(starTreeBootstrapPhase1Output, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith("part");
      }
    });
    assertTrue("startree_bootstrap_phase1 data not generated", startreeBootstrapPhase1Status.length != 0);

    LOGGER.info("startree_bootstrap_phase1 job completed");
  }

  private void testStartreeGeneration() throws IOException, ClassNotFoundException, InterruptedException {
    LOGGER.info("Starting startree_generation job");

    // Startree Generation
    Job job = Job.getInstance(conf);
    job.setJobName("StartreeGeneration");
    job.setJarByClass(StarTreeGenerationJob.class);

    // Map config
    job.setMapperClass(StarTreeGenerationMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    // Reduce config
    job.setNumReduceTasks(0);

    // startree generation config
    Configuration configuration = job.getConfiguration();
    configuration.set(STAR_TREE_GEN_INPUT_PATH.toString(), rollupPhase4OutputPath.toString());
    configuration.set(STAR_TREE_GEN_CONFIG_PATH.toString(), configFilePath.toString());
    configuration.set(STAR_TREE_GEN_OUTPUT_PATH.toString(), starTreeGenerationOutput.toString());

    // execute job
    FileInputFormat.addInputPath(job, new Path(configuration.get(STAR_TREE_GEN_INPUT_PATH.toString())));
    FileOutputFormat.setOutputPath(job, new Path(configuration.get(STAR_TREE_GEN_OUTPUT_PATH.toString())));
    job.waitForCompletion(true);

    // tests
    assertTrue("startree_generation job failed", job.isSuccessful());
    assertTrue("startree_generation folder not created", fs.exists(starTreeGenerationOutput));
    assertTrue("tree.bin not generated in startree_generation", fs.exists(new Path(starTreeGenerationOutput, "tree.bin")));
    assertTrue("dimensionStore.tar.gz not generated in startree_generation", fs.exists(new Path(starTreeGenerationOutput, "dimensionStore.tar.gz")));

    LOGGER.info("startree_generation job completed");
  }

  private void testRollupPhase4() throws ClassNotFoundException, IOException, InterruptedException {
    LOGGER.info("Starting rollup_phase4 job");

    // Rollup Phase 4
    Job job = Job.getInstance(conf);
    job.setJobName("RollupPhase4Test");
    job.setJarByClass(RollupPhaseFourJob.class);

    // Map config
    job.setMapperClass(RollupPhaseFourMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    job.setCombinerClass(RollupPhaseFourReducer.class);
    job.setReducerClass(RollupPhaseFourReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setNumReduceTasks(1);

    // Rollup phase 4 config
    Configuration configuration = job.getConfiguration();
    configuration.set(ROLLUP_PHASE4_INPUT_PATH.toString(), rollupPhase3OutputPath.toString()+","+aboveThresholdPath.toString());
    configuration.set(ROLLUP_PHASE4_CONFIG_PATH.toString(), configFilePath.toString());
    configuration.set(ROLLUP_PHASE4_OUTPUT_PATH.toString(), rollupPhase4OutputPath.toString());

    // execute job
    for (String inputPath : configuration.get(ROLLUP_PHASE4_INPUT_PATH.toString()).split(",")) {
      FileInputFormat.addInputPath(job, new Path(inputPath));
    }
    FileOutputFormat.setOutputPath(job, new Path(configuration.get(ROLLUP_PHASE4_OUTPUT_PATH.toString())));
    job.waitForCompletion(true);

    // tests
    assertTrue("rollup_phase4 job failed", job.isSuccessful());
    assertTrue("rollup_phase4 folder not created", fs.exists(rollupPhase4OutputPath));
    FileStatus[] rollupPhase4Status = fs.listStatus(rollupPhase4OutputPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith("part");
      }
    });
    assertTrue("rollup_phase4 data not generated", rollupPhase4Status.length != 0);

    LOGGER.info("rollup_phase4 job completed");
  }

  private void testRollupPhase3() throws IOException, ClassNotFoundException, InterruptedException {
    LOGGER.info("Starting rollup_phase3 job");

    // Rollup Phase 3
    Job job = Job.getInstance(conf);
    job.setJobName("RollupPhase3Test");
    job.setJarByClass(RollupPhaseThreeJob.class);
    job.getConfiguration().set("mapreduce.reduce.shuffle.input.buffer.percent", "0.40");

    // Map config
    job.setMapperClass(RollupPhaseThreeMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    job.setReducerClass(RollupPhaseThreeReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setNumReduceTasks(10);

    // rollup phase 2 config
    Configuration configuration = job.getConfiguration();
    configuration.set(ROLLUP_PHASE3_INPUT_PATH.toString(), rollupPhase2OutputPath.toString());
    configuration.set(ROLLUP_PHASE3_CONFIG_PATH.toString(), configFilePath.toString());
    configuration.set(ROLLUP_PHASE3_OUTPUT_PATH.toString(), rollupPhase3OutputPath.toString());

    // execute job
    FileInputFormat.addInputPath(job, new Path(configuration.get(ROLLUP_PHASE3_INPUT_PATH.toString())));
    FileOutputFormat.setOutputPath(job, new Path(configuration.get(ROLLUP_PHASE3_OUTPUT_PATH.toString())));
    job.waitForCompletion(true);

    // tests
    assertTrue("rollup_phase3 job failed", job.isSuccessful());
    assertTrue("rollup_phase3 folder not created", fs.exists(rollupPhase3OutputPath));
    FileStatus[] rollupPhase3Status = fs.listStatus(rollupPhase3OutputPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith("part");
      }
    });
    assertTrue("rollup_phase3 data not generated", rollupPhase3Status.length != 0);

    LOGGER.info("rollup_phase3 job completed");
  }

  private void testRollupPhase2() throws ClassNotFoundException, IOException, InterruptedException {
    LOGGER.info("Starting rollup_phase2 job");

    // Rollup Phase 2
    Job job = Job.getInstance(conf);
    job.setJobName("RollupPhase2Test");
    job.setJarByClass(RollupPhaseTwoJob.class);

    // Map config
    job.setMapperClass(RollupPhaseTwoMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    job.setReducerClass(RollupPhaseTwoReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setNumReduceTasks(10);

    // Rollup phase 2 config
    Configuration configuration = job.getConfiguration();
    configuration.set(ROLLUP_PHASE2_INPUT_PATH.toString(), belowThresholdPath.toString());
    configuration.set(ROLLUP_PHASE2_CONFIG_PATH.toString(), configFilePath.toString());
    configuration.set(ROLLUP_PHASE2_OUTPUT_PATH.toString(), rollupPhase2OutputPath.toString());
    configuration.set(ROLLUP_PHASE2_ANALYSIS_PATH.toString(), dimensionStatsPath.toString());

    // execute job
    FileInputFormat.addInputPath(job, new Path(configuration.get(ROLLUP_PHASE2_INPUT_PATH.toString())));
    FileOutputFormat.setOutputPath(job, new Path(configuration.get(ROLLUP_PHASE2_OUTPUT_PATH.toString())));
    job.waitForCompletion(true);

    // tests
    assertTrue("rollup_phase2 job failed", job.isSuccessful());
    assertTrue("rollup_phase2 folder not created", fs.exists(rollupPhase2OutputPath));
    FileStatus[] rollupPhase2Status = fs.listStatus(rollupPhase2OutputPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith("part");
      }
    });
    assertTrue("rollup_phase2 data not generated", rollupPhase2Status.length != 0);

    LOGGER.info("rollup_phase2 job completed");
  }

  private void testRollupPhase1() throws ClassNotFoundException, IOException, InterruptedException {

    LOGGER.info("Starting rollup_phase1 job");

    // Rollup phase 1
    Job job = Job.getInstance(conf);
    job.setJobName("RollupPhase1Test");
    job.setJarByClass(RollupPhaseOneJob.class);

    // Map config
    job.setMapperClass(RollupPhaseOneMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setNumReduceTasks(0);

    // rollup phase 1 config
    Configuration configuration = job.getConfiguration();
    configuration.set(ROLLUP_PHASE1_INPUT_PATH.toString(), aggregationOutputPath.toString());
    configuration.set(ROLLUP_PHASE1_CONFIG_PATH.toString(), configFilePath.toString());
    configuration.set(ROLLUP_PHASE1_OUTPUT_PATH.toString(), rollupPhase1OutputPath.toString());

    // execute job
    FileInputFormat.addInputPath(job, new Path(configuration.get(ROLLUP_PHASE1_INPUT_PATH.toString())));
    MultipleOutputs.addNamedOutput(job, "aboveThreshold", SequenceFileOutputFormat.class, BytesWritable.class, BytesWritable.class);
    MultipleOutputs.addNamedOutput(job, "belowThreshold", SequenceFileOutputFormat.class, BytesWritable.class, BytesWritable.class);
    FileOutputFormat.setOutputPath(job, new Path(configuration.get(ROLLUP_PHASE1_OUTPUT_PATH.toString())));
    job.waitForCompletion(true);

    // tests
    assertTrue("rollup_phase1 job failed", job.isSuccessful());
    assertTrue("rollup_phase1 folder not created", fs.exists(rollupPhase1OutputPath));
    assertTrue("aboveThreshold folder not created", fs.exists(aboveThresholdPath));
    assertTrue("belowThreshold folder not created", fs.exists(belowThresholdPath));
    FileStatus[] aboveThresholdStatus = fs.listStatus(aboveThresholdPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith("above");
      }
    });
    FileStatus[] belowThresholdStatus = fs.listStatus(belowThresholdPath, new PathFilter() {
      @Override
      public boolean accept(Path path) {
        return path.getName().startsWith("below");
      }
    });
    assertTrue("aboveThreshold data not generated", aboveThresholdStatus.length != 0);
    assertTrue("belowThreshold data not generated", belowThresholdStatus.length != 0);

    LOGGER.info("rollup_phase1 job completed");
  }

  private void testAggregation() throws IOException, ClassNotFoundException, InterruptedException {

    LOGGER.info("Starting aggregation job");

    // Aggregation
    Job job = Job.getInstance(conf);
    job.setJobName("AggregationTest");
    job.setJarByClass(AggregatePhaseJob.class);

    // Avro schema
    Schema schema = new Schema.Parser().parse(fs.open(schemaFilePath));

    // Map config
    job.setMapperClass(AggregationMapper.class);
    AvroJob.setInputKeySchema(job, schema);
    job.setInputFormatClass(AvroKeyInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    job.setCombinerClass(AggregationReducer.class);
    job.setReducerClass(AggregationReducer.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setNumReduceTasks(10);

    // aggregation phase config
    Configuration configuration = job.getConfiguration();
    configuration.set(AGG_INPUT_PATH.toString(), inputFilePath.toString());
    configuration.set(AGG_CONFIG_PATH.toString(), configFilePath.toString());
    configuration.set(AGG_OUTPUT_PATH.toString(), aggregationOutputPath.toString());
    configuration.set(AGG_INPUT_AVRO_SCHEMA.toString(), schemaFilePath.toString());
    configuration.set(AGG_DIMENSION_STATS_PATH.toString(), dimensionStatsPath.toString());

    // execute job
    FileInputFormat.addInputPath(job, new Path(configuration.get(AGG_INPUT_PATH.toString())));
    FileOutputFormat.setOutputPath(job, new Path(configuration.get(AGG_OUTPUT_PATH.toString())));
    job.waitForCompletion(true);

    // tests
    assertTrue("aggregation job failed", job.isSuccessful());
    // TODO: Disabling these because they cause HDFS quotas to be hit too quickly when many tasks are used (gbrandt, 2015-08-27)
//    assertTrue("Dimension stats folder not created", fs.exists(dimensionStatsPath));
//    FileStatus[] dimensionStatus = fs.listStatus(dimensionStatsPath, new PathFilter() {
//      @Override
//      public boolean accept(Path path) {
//        return path.getName().endsWith(".stat");
//      }
//    });
//    assertTrue("Dimension stats not generated", dimensionStatus.length != 0);
//    assertTrue("Aggregation ouput folder not created", fs.exists(aggregationOutputPath));
//    FileStatus[] aggregationStatus = fs.listStatus(aggregationOutputPath, new PathFilter() {
//      @Override
//      public boolean accept(Path path) {
//        return path.getName().startsWith("part");
//      }
//    });
//    assertTrue("Aggregation results not generated", aggregationStatus.length != 0);

    LOGGER.info("aggregation job completed");
  }

  private void testDataGeneration() throws Exception {
    File configFile = new File(getClass().getClassLoader().getResource("integrationTest/"+CONFIG_FILE).getFile());
    File schemaFile = new File(getClass().getClassLoader().getResource("integrationTest/"+SCHEMA_FILE).getFile());

    // Data generation
    File avroDataInput = new File(rootDir, COLLECTION);
    avroDataInput = new File(avroDataInput, INPUT_DIR);
    if (!avroDataInput.exists())
    {
      FileUtils.forceMkdir(avroDataInput);
    }

    DataGeneratorConfig config = new DataGeneratorConfig();
    config.setMinTime(MIN_TIME.toString().substring(0, MIN_TIME.toString().indexOf('.')));
    config.setMaxTime(MAX_TIME.toString().substring(0, MAX_TIME.toString().indexOf('.')));
    config.setNumFiles(3);
    config.setNumRecords(5000);
    config.setCardinality("4,5,6,7");
    config.setSchemaFile(schemaFile);
    config.setStarTreeConfig(StarTreeConfig.decode(new FileInputStream(configFile)));
    config.setOutputDataDirectory(avroDataInput);
    DataGeneratorTool dataGeneratorTool = new DataGeneratorTool();
    dataGeneratorTool.generateData(config);

    assertTrue("Data generation failed", avroDataInput.list().length == 3);
    LOGGER.info("Data generation completed");

    // Setup input and output dirs on hdfs
    inputFilePath = new Path(ROOT, INPUT_DIR);
    Path outputDir = new Path(ROOT, COLLECTION);
    fs.delete(inputFilePath, true);
    fs.delete(outputDir,true);

    // Copy input
    fs.copyFromLocalFile(new Path(avroDataInput.getAbsolutePath()), inputFilePath);
    assertTrue("Failed to copy input folder to hdfs", fs.exists(inputFilePath));
    assertTrue("Failed to copy avro files to hdfs", fs.listStatus(inputFilePath).length == 3);
    // Copy config
    configFilePath = new Path(outputDir, CONFIG_FILE);
    fs.copyFromLocalFile(new Path(configFile.getAbsolutePath()), configFilePath);
    assertTrue("Failed to copy config file", fs.exists(configFilePath));
    // Copy schema
    schemaFilePath = new Path(outputDir, SCHEMA_FILE);
    fs.copyFromLocalFile(new Path(schemaFile.getAbsolutePath()), schemaFilePath);
    assertTrue("Failed to copy schema file", fs.exists(schemaFilePath));
    LOGGER.info("Moved data to hdfs");

    Path dimensionIndexDir = new Path(outputDir, "DIMENSION_INDEX");
    dimensionIndexDir = new Path(dimensionIndexDir, DATA_DIR);
    aggregationOutputPath = new Path(dimensionIndexDir, "aggregation");
    dimensionStatsPath = new Path(dimensionIndexDir, "dimension_stats");
    rollupPhase1OutputPath = new Path(dimensionIndexDir, "rollup_phase1");
    belowThresholdPath = new Path(rollupPhase1OutputPath, "belowThreshold");
    aboveThresholdPath = new Path(rollupPhase1OutputPath, "aboveThreshold");
    rollupPhase2OutputPath = new Path(dimensionIndexDir, "rollup_phase2");
    rollupPhase3OutputPath = new Path(dimensionIndexDir, "rollup_phase3");
    rollupPhase4OutputPath = new Path(dimensionIndexDir, "rollup_phase4");
    starTreeGenerationOutput = new Path(dimensionIndexDir, "startree_generation");
    Path metricIndexDir = new Path(outputDir, "METRIC_INDEX");
    metricIndexDir = new Path(metricIndexDir, DATA_DIR);
    starTreeBootstrapPhase1Output = new Path(metricIndexDir, "startree_bootstrap_phase1");
    starTreeBootstrapPhase2Output = new Path(metricIndexDir, "startree_bootstrap_phase2");
    serverPackageOutput = metricIndexDir;
  }

  public static String getDateString(DateTime dateTime) {
    return ISODateTimeFormat.dateTimeNoMillis().print(dateTime.toDateTime(DateTimeZone.UTC));
  }

}