package com.linkedin.thirdeye.bootstrap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class StarTreeBootstrapJob extends Configured
{
  private final String name;
  private final Properties props;

  public StarTreeBootstrapJob(String name, Properties props)
  {
    this.name = name;
    this.props = props;
  }

  public static class StarTreeBootstrapMapper extends Mapper<Object, Text, Text, Text>
  {
    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {

    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {

    }
  }

  public void run() throws Exception
  {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeBootstrapJob.class);

    job.setNumReduceTasks(0);
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length != 1)
    {
      throw new IllegalArgumentException("usage: config.properties");
    }

    Properties props = new Properties();
    props.load(new FileInputStream(args[0]));

    StarTreeBootstrapJob bootstrapJob = new StarTreeBootstrapJob("star_tree_bootstrap_job", props);
    bootstrapJob.run();
  }
}
