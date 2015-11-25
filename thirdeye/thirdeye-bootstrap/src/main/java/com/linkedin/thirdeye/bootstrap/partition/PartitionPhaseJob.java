package com.linkedin.thirdeye.bootstrap.partition;

import static com.linkedin.thirdeye.bootstrap.partition.PartitionPhaseConstants.PARTITION_PHASE_CONFIG_PATH;
import static com.linkedin.thirdeye.bootstrap.partition.PartitionPhaseConstants.PARTITION_PHASE_INPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.partition.PartitionPhaseConstants.PARTITION_PHASE_OUTPUT_PATH;
import static com.linkedin.thirdeye.bootstrap.partition.PartitionPhaseConstants.PARTITION_PHASE_NUM_PARTITIONS;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.StarTreeConstants;


public class PartitionPhaseJob extends Configured {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(PartitionPhaseJob.class);

  private String name;

  private Properties props;

  /**
   *
   * @param name
   * @param props
   */
  public PartitionPhaseJob(String name, Properties props) {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class PartitionMapper extends
      Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    MultipleOutputs<BytesWritable, BytesWritable> mos;
    private int numPartitions;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      LOGGER.info("PartitionPhaseJon.PartitionPhaseMapper.setup()");
      Configuration configuration = context.getConfiguration();
      mos = new MultipleOutputs<BytesWritable, BytesWritable>(context);
      numPartitions = Integer.valueOf(configuration.get(PARTITION_PHASE_NUM_PARTITIONS.toString()));
    }

    @Override
    public void map(BytesWritable dimensionKeyBytes,
        BytesWritable metricTimeSeriesBytes, Context context)
        throws IOException, InterruptedException {

      int partition = Math.abs(dimensionKeyBytes.hashCode()) % numPartitions;

      mos.write(dimensionKeyBytes, metricTimeSeriesBytes,
          StarTreeConstants.PARTITION_FOLDER_PREFIX + partition + "/" + "part");
    }

    @Override
    public void cleanup(Context context) throws IOException,
        InterruptedException {
      mos.close();
    }

  }



  public Job run() throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(PartitionPhaseJob.class);

    // Map config
    job.setMapperClass(PartitionMapper.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(BytesWritable.class);

    // Reduce config
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    // partition phase config
    Configuration configuration = job.getConfiguration();
    String inputPathDir = getAndSetConfiguration(configuration, PARTITION_PHASE_INPUT_PATH);
    getAndSetConfiguration(configuration, PARTITION_PHASE_CONFIG_PATH);
    getAndSetConfiguration(configuration, PARTITION_PHASE_OUTPUT_PATH);
    int numPartitions = Integer.valueOf(getAndSetConfiguration(configuration, PARTITION_PHASE_NUM_PARTITIONS));
    LOGGER.info("Input path dir: " + inputPathDir);
    for (String inputPath : inputPathDir.split(",")) {
      LOGGER.info("Adding input:" + inputPath);
      Path input = new Path(inputPath);
      FileInputFormat.addInputPath(job, input);
    }

    for (int i = 0 ; i < numPartitions; i ++) {
      MultipleOutputs.addNamedOutput(job, StarTreeConstants.PARTITION_FOLDER_PREFIX + i,
          SequenceFileOutputFormat.class, BytesWritable.class, BytesWritable.class);
    }

    FileOutputFormat.setOutputPath(job, new Path(
        getAndCheck(PARTITION_PHASE_OUTPUT_PATH.toString())));

    job.waitForCompletion(true);

    return job;

  }

  private String getAndSetConfiguration(Configuration configuration,
      PartitionPhaseConstants constant) {
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
