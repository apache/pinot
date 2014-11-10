package com.linkedin.thirdeye.bootstrap;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class StarTreeBootstrapJob extends Configured
{
  public static final String PROP_STARTREE_COLLECTION = "startree.collection";
  public static final String PROP_STARTREE_CONFIG = "startree.config";
  public static final String PROP_STARTREE_DATA = "startree.data";
  public static final String PROP_STARTREE_ROOT = "startree.root";
  public static final String PROP_INPUT_PATHS = "input.paths";
  public static final String PROP_OUTPUT_PATH = "output.path";

  private final String name;
  private final Properties props;

  public StarTreeBootstrapJob(String name, Properties props)
  {
    super(new Configuration());
    this.name = name;
    this.props = props;
  }

  public static class StarTreeBootstrapMapper extends Mapper<Object, Text, Text, Text>
  {
    private String collection;
    private ExecutorService executorService;
    private StarTreeManager starTreeManager;

    @Override
    public void setup(Context context) throws IOException, InterruptedException
    {
      collection = context.getConfiguration().get(PROP_STARTREE_COLLECTION);

      FileSystem fileSystem = FileSystem.get(context.getConfiguration());
      executorService = Executors.newSingleThreadExecutor();
      starTreeManager = new StarTreeManagerImpl(executorService);

      Path rootPath = new Path(context.getConfiguration().get(PROP_STARTREE_ROOT));
      Path configPath = new Path(context.getConfiguration().get(PROP_STARTREE_CONFIG));

      try
      {
        starTreeManager.restore(collection,
                                fileSystem.open(rootPath),
                                fileSystem.open(configPath));
      }
      catch (Exception e)
      {
        throw new IOException(e);
      }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
      StarTree starTree = starTreeManager.getStarTree(collection);

      int idx = 0;
      String[] tokens = value.toString().split("\t");

      StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();

      for (String dimensionName : starTree.getConfig().getDimensionNames())
      {
        builder.setDimensionValue(dimensionName, tokens[idx++]);
      }

      for (String metricName : starTree.getConfig().getMetricNames())
      {
        builder.setMetricValue(metricName, Long.valueOf(tokens[idx++]));
      }

      builder.setTime(Long.valueOf(tokens[idx]));

      starTree.add(builder.build());
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException
    {
      Text nodeId = new Text();
      Text leafData = new Text();

      writeLeafData(context, starTreeManager.getStarTree(collection).getRoot(), nodeId, leafData);

      if (executorService != null)
      {
        executorService.shutdown();
      }
    }

    private void writeLeafData(Context context,
                               StarTreeNode node,
                               Text nodeId,
                               Text leafData) throws IOException, InterruptedException
    {
      if (node.isLeaf())
      {
        nodeId.set(node.getId().toString());
        leafData.set(node.getRecordStore().encode());
        context.write(nodeId, leafData);
      }
      else
      {
        for (StarTreeNode child : node.getChildren())
        {
          writeLeafData(context, child, nodeId, leafData);
        }
        writeLeafData(context, node.getOtherNode(), nodeId, leafData);
        writeLeafData(context, node.getStarNode(), nodeId, leafData);
      }
    }
  }

  public static class StarTreeBootstrapReducer extends Reducer<Text, Text, NullWritable, Text>
  {
    // TODO: Merge record stores and write to node ID file
  }

  public void run() throws Exception
  {
    Job job = Job.getInstance(getConf());
    job.setJobName(name);
    job.setJarByClass(StarTreeBootstrapJob.class);

    job.setMapperClass(StarTreeBootstrapMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(0);

    job.getConfiguration().set(PROP_STARTREE_COLLECTION, getAndCheck(PROP_STARTREE_COLLECTION));
    job.getConfiguration().set(PROP_STARTREE_CONFIG, getAndCheck(PROP_STARTREE_CONFIG));
    job.getConfiguration().set(PROP_STARTREE_DATA, getAndCheck(PROP_STARTREE_DATA));
    job.getConfiguration().set(PROP_STARTREE_ROOT, getAndCheck(PROP_STARTREE_ROOT));

    for (String inputPath : getAndCheck(PROP_INPUT_PATHS).split(","))
    {
      FileInputFormat.addInputPath(job, new Path(inputPath));
    }

    FileOutputFormat.setOutputPath(job, new Path(getAndCheck(PROP_OUTPUT_PATH)));

    job.waitForCompletion(true);
  }

  private String getAndCheck(String propName)
  {
    String propValue = props.getProperty(propName);
    if (propValue == null)
    {
      throw new IllegalArgumentException(propName + " required property");
    }
    return propValue;
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
