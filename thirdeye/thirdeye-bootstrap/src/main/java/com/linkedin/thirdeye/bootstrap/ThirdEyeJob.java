package com.linkedin.thirdeye.bootstrap;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.bootstrap.aggregation.AggregatePhaseJob;
import com.linkedin.thirdeye.bootstrap.aggregation.AggregationJobConstants;
import com.linkedin.thirdeye.bootstrap.analysis.AnalysisJobConstants;
import com.linkedin.thirdeye.bootstrap.analysis.AnalysisPhaseJob;
import com.linkedin.thirdeye.bootstrap.analysis.AnalysisPhaseStats;
import com.linkedin.thirdeye.bootstrap.rollup.phase1.RollupPhaseOneConstants;
import com.linkedin.thirdeye.bootstrap.rollup.phase1.RollupPhaseOneJob;
import com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoConstants;
import com.linkedin.thirdeye.bootstrap.rollup.phase2.RollupPhaseTwoJob;
import com.linkedin.thirdeye.bootstrap.rollup.phase3.RollupPhaseThreeConstants;
import com.linkedin.thirdeye.bootstrap.rollup.phase3.RollupPhaseThreeJob;
import com.linkedin.thirdeye.bootstrap.rollup.phase4.RollupPhaseFourConstants;
import com.linkedin.thirdeye.bootstrap.rollup.phase4.RollupPhaseFourJob;
import com.linkedin.thirdeye.bootstrap.startree.StarTreeJobUtils;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneConstants;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1.StarTreeBootstrapPhaseOneJob;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoConstants;
import com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2.StarTreeBootstrapPhaseTwoJob;
import com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationConstants;
import com.linkedin.thirdeye.bootstrap.startree.generation.StarTreeGenerationJob;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Properties;

/*
  thirdeye.root={/path/to/user}
  thirdeye.collection={collection}
  input.time.min={collectionTime}
  input.time.max={collectionTime}
  input.paths=/path1,/path2,/path3

    {root}/
    {collection}/
      config.yml
      schema.avsc
      data_{start}-{end}/
        aggregation/
        rollup/
          phase1/
          phase2/
          phase3/
          phase4/
        startree/
          generation/
            star-tree-{collection}/
              {collection}-tree.bin
          bootstrap_phase1/
          bootstrap_phase2/
            task_*
      data_{start}-{end}/
        startree/
          bootstrap_phase1/
          bootstrap_phase2/
            task_*


 */
public class ThirdEyeJob
{
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeJob.class);

  private static final String USAGE = "usage: phase_name job.properties";

  private static final String AVRO_SCHEMA = "schema.avsc";

  private enum PhaseSpec
  {
    ANALYSIS
            {
              @Override
              Class<?> getKlazz()
              {
                return AnalysisPhaseJob.class;
              }

              @Override
              String getDescription()
              {
                return "Analyzes input Avro data to compute information necessary for job";
              }

              @Override
              Properties getJobProperties(String root, String collection, long minTime, long maxTime, String inputPaths)
              {
                Properties config = new Properties();

                config.setProperty(AnalysisJobConstants.ANALYSIS_INPUT_AVRO_SCHEMA.toString(),
                                   getSchemaPath(root, collection));
                config.setProperty(AnalysisJobConstants.ANALYSIS_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(AnalysisJobConstants.ANALYSIS_INPUT_PATH.toString(),
                                   inputPaths);
                config.setProperty(AnalysisJobConstants.ANALYSIS_OUTPUT_PATH.toString(),
                                   getAnalysisPath(root, collection));

                return config;
              }
            },
    AGGREGATION
            {
              @Override
              Class<?> getKlazz()
              {
                return AggregatePhaseJob.class;
              }

              @Override
              String getDescription()
              {
                return "Aggregates input data";
              }

              @Override
              Properties getJobProperties(String root, String collection, long minTime, long maxTime, String inputPaths)
              {
                Properties config = new Properties();

                config.setProperty(AggregationJobConstants.AGG_INPUT_AVRO_SCHEMA.toString(),
                                   getSchemaPath(root, collection));
                config.setProperty(AggregationJobConstants.AGG_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(AggregationJobConstants.AGG_INPUT_PATH.toString(),
                                   inputPaths);
                config.setProperty(AggregationJobConstants.AGG_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + AGGREGATION.getName());

                return config;
              }
            },
    ROLLUP_PHASE1
            {
              @Override
              Class<?> getKlazz()
              {
                return RollupPhaseOneJob.class;
              }

              @Override
              String getDescription()
              {
                return "Splits input data into above / below threshold using function";
              }

              @Override
              Properties getJobProperties(String root, String collection, long minTime, long maxTime, String inputPaths)
              {
                Properties config = new Properties();

                config.setProperty(RollupPhaseOneConstants.ROLLUP_PHASE1_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(RollupPhaseOneConstants.ROLLUP_PHASE1_INPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + AGGREGATION.getName());
                config.setProperty(RollupPhaseOneConstants.ROLLUP_PHASE1_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + ROLLUP_PHASE1.getName());

                return config;
              }
            },
    ROLLUP_PHASE2
            {
              @Override
              Class<?> getKlazz()
              {
                return RollupPhaseTwoJob.class;
              }

              @Override
              String getDescription()
              {
                return "Aggregates all possible combinations of raw dimension combination below threshold";
              }

              @Override
              Properties getJobProperties(String root, String collection, long minTime, long maxTime, String inputPaths)
              {
                Properties config = new Properties();

                config.setProperty(RollupPhaseTwoConstants.ROLLUP_PHASE2_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(RollupPhaseTwoConstants.ROLLUP_PHASE2_INPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + ROLLUP_PHASE1.getName() + File.separator + "belowThreshold");
                config.setProperty(RollupPhaseTwoConstants.ROLLUP_PHASE2_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + ROLLUP_PHASE2.getName());

                return config;
              }
            },
    ROLLUP_PHASE3
            {
              @Override
              Class<?> getKlazz()
              {
                return RollupPhaseThreeJob.class;
              }

              @Override
              String getDescription()
              {
                return "Selects the rolled-up dimension key for each raw dimension combination";
              }

              @Override
              Properties getJobProperties(String root, String collection, long minTime, long maxTime, String inputPaths)
              {
                Properties config = new Properties();

                config.setProperty(RollupPhaseThreeConstants.ROLLUP_PHASE3_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(RollupPhaseThreeConstants.ROLLUP_PHASE3_INPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + ROLLUP_PHASE2.getName());
                config.setProperty(RollupPhaseThreeConstants.ROLLUP_PHASE3_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + ROLLUP_PHASE3.getName());

                return config;
              }
            },
    ROLLUP_PHASE4
            {
              @Override
              Class<?> getKlazz()
              {
                return RollupPhaseFourJob.class;
              }

              @Override
              String getDescription()
              {
                return "Sums metric time series by the rolled-up dimension key";
              }

              @Override
              Properties getJobProperties(String root, String collection, long minTime, long maxTime, String inputPaths)
              {
                Properties config = new Properties();

                config.setProperty(RollupPhaseFourConstants.ROLLUP_PHASE4_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(RollupPhaseFourConstants.ROLLUP_PHASE4_INPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + ROLLUP_PHASE3.getName() + "," +
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + ROLLUP_PHASE1.getName() + File.separator + "aboveThreshold");
                config.setProperty(RollupPhaseFourConstants.ROLLUP_PHASE4_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + ROLLUP_PHASE4.getName());

                return config;
              }
            },
    STARTREE_GENERATION
            {
              @Override
              Class<?> getKlazz()
              {
                return StarTreeGenerationJob.class;
              }

              @Override
              String getDescription()
              {
                return "Builds star tree index structure using rolled up dimension combinations and those above threshold";
              }

              @Override
              Properties getJobProperties(String root, String collection, long minTime, long maxTime, String inputPaths)
              {
                Properties config = new Properties();

                config.setProperty(StarTreeGenerationConstants.STAR_TREE_GEN_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(StarTreeGenerationConstants.STAR_TREE_GEN_INPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + ROLLUP_PHASE4.getName());
                config.setProperty(StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + STARTREE_GENERATION.getName());

                return config;
              }
            },
    STARTREE_BOOTSTRAP_PHASE1
            {
              @Override
              Class<?> getKlazz()
              {
                return StarTreeBootstrapPhaseOneJob.class;
              }

              @Override
              String getDescription()
              {
                return "Sums raw Avro time-series data by dimension key";
              }

              @Override
              Properties getJobProperties(String root, String collection, long minTime, long maxTime, String inputPaths)
              {
                Properties config = new Properties();

                config.setProperty(StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_INPUT_AVRO_SCHEMA.toString(),
                                   getSchemaPath(root, collection));
                config.setProperty(StarTreeBootstrapPhaseOneConstants.STAR_TREE_GENERATION_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + STARTREE_GENERATION.getName());
                config.setProperty(StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_INPUT_PATH.toString(),
                                   inputPaths);
                config.setProperty(StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + STARTREE_BOOTSTRAP_PHASE1.getName());

                return config;
              }
            },
    STARTREE_BOOTSTRAP_PHASE2
            {
              @Override
              Class<?> getKlazz()
              {
                return StarTreeBootstrapPhaseTwoJob.class;
              }

              @Override
              String getDescription()
              {
                return "Groups records by star tree leaf node and creates leaf buffers";
              }

              @Override
              Properties getJobProperties(String root, String collection, long minTime, long maxTime, String inputPaths)
              {
                Properties config = new Properties();

                config.setProperty(StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(StarTreeBootstrapPhaseTwoConstants.STAR_TREE_GENERATION_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + STARTREE_GENERATION.getName());
                config.setProperty(StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_INPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + STARTREE_BOOTSTRAP_PHASE1.getName());
                config.setProperty(StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, minTime, maxTime) + File.separator + STARTREE_BOOTSTRAP_PHASE2.getName());

                return config;
              }
            },
    SERVER_UPDATE
            {
              @Override
              Class<?> getKlazz()
              {
                return null; // unused
              }

              @Override
              String getDescription()
              {
                return "Pushes metric data from startree_bootstrap_phase2 to thirdeye.server.uri";
              }

              @Override
              Properties getJobProperties(String root, String collection, long minTime, long maxTime, String inputPaths)
              {
                return null; // unused
              }
            },
    SERVER_BOOTSTRAP
            {
              @Override
              Class<?> getKlazz()
              {
                return null;
              }

              @Override
              String getDescription()
              {
                return "Pushes star tree, dimension, and metric data from startree_bootstrap_phase2 to thirdeye.server.uri";
              }

              @Override
              Properties getJobProperties(String root, String collection, long minTime, long maxTime, String inputPaths)
              {
                return null;
              }
            };

    abstract Class<?> getKlazz();

    abstract String getDescription();

    abstract Properties getJobProperties(String root, String collection, long minTime, long maxTime, String inputPaths);

    String getName()
    {
      return this.name().toLowerCase();
    }

    String getCollectionDir(String root, String collection)
    {
      return root == null ? collection : root + File.separator + collection;
    }

    String getAnalysisPath(String root, String collection)
    {
      return getCollectionDir(root, collection) + File.separator + "analysis";
    }

    String getTimeDir(String root, String collection, long minTime, long maxTime)
    {
      return getCollectionDir(root, collection) + File.separator + "data_" + minTime + "-" + maxTime;
    }

    String getConfigPath(String root, String collection)
    {
      return getCollectionDir(root, collection) + File.separator + StarTreeConstants.CONFIG_FILE_NAME;
    }

    String getSchemaPath(String root, String collection)
    {
      return getCollectionDir(root, collection) + File.separator + AVRO_SCHEMA;
    }
  }

  private static void usage()
  {
    System.err.println(USAGE);
    for (PhaseSpec phase : PhaseSpec.values())
    {
      System.err.printf("%-30s : %s\n", phase.getName(), phase.getDescription());
    }
  }

  private static String getAndCheck(String name, Properties properties)
  {
    String value = properties.getProperty(name);
    if (value == null)
    {
      throw new IllegalArgumentException("Must provide " + name);
    }
    return value;
  }

  private final String phaseName;
  private final Properties config;

  public ThirdEyeJob(String phaseName, Properties config)
  {
    this.phaseName = phaseName;
    this.config = config;
  }

  public void run() throws Exception
  {
    PhaseSpec phaseSpec;
    try
    {
      phaseSpec = PhaseSpec.valueOf(phaseName.toUpperCase());
    }
    catch (Exception e)
    {
      usage();
      throw e;
    }

    String root = getAndCheck(ThirdEyeJobConstants.THIRDEYE_ROOT.getPropertyName(), config);
    String collection = getAndCheck(ThirdEyeJobConstants.THIRDEYE_COLLECTION.getPropertyName(), config);
    String inputPaths = getAndCheck(ThirdEyeJobConstants.INPUT_PATHS.getPropertyName(), config);
    long minTime = -1;
    long maxTime = -1;

    if (!PhaseSpec.ANALYSIS.equals(phaseSpec)) // analysis phase computes these values
    {
      FileSystem fileSystem = FileSystem.get(new Configuration());

      // Load analysis results
      InputStream inputStream
              = fileSystem.open(new Path(phaseSpec.getAnalysisPath(root, collection),
                                         AnalysisJobConstants.ANALYSIS_FILE_NAME.toString()));
      AnalysisPhaseStats stats = AnalysisPhaseStats.fromBytes(IOUtils.toByteArray(inputStream));
      inputStream.close();

      // Check input paths
      if (!inputPaths.equals(stats.getInputPath()))
      {
        throw new IllegalStateException("Last analysis was done for input paths "
                                                + stats.getInputPath() + " not " + inputPaths);
      }

      minTime = stats.getMinTime();
      maxTime = stats.getMaxTime();
    }

    if (PhaseSpec.SERVER_UPDATE.equals(phaseSpec))
    {
      String thirdEyeServerUri = config.getProperty(ThirdEyeJobConstants.THIRDEYE_SERVER_URI.getPropertyName());
      if (thirdEyeServerUri == null)
      {
        throw new IllegalArgumentException(
                "Must provide " + ThirdEyeJobConstants.THIRDEYE_SERVER_URI.getPropertyName() + " in properties");
      }

      FileSystem fileSystem = FileSystem.get(new Configuration());

      // Push data (no dimensions)
      Path dataPath = new Path(PhaseSpec.STARTREE_BOOTSTRAP_PHASE2.getTimeDir(root, collection, minTime, maxTime)
                                       + File.separator + PhaseSpec.STARTREE_BOOTSTRAP_PHASE2.getName());
      RemoteIterator<LocatedFileStatus> itr = fileSystem.listFiles(dataPath, false);
      while (itr.hasNext())
      {
        LocatedFileStatus fileStatus = itr.next();
        if (fileStatus.getPath().getName().startsWith("task_"))
        {
          InputStream leafData = fileSystem.open(fileStatus.getPath());
          int responseCode = StarTreeJobUtils.pushData(leafData, thirdEyeServerUri, collection, false);
          leafData.close();
          LOG.info("Load {} #=> {}", fileStatus.getPath(), responseCode);
        }
      }
    }
    else if (PhaseSpec.SERVER_BOOTSTRAP.equals(phaseSpec))
    {
      String thirdEyeServerUri = config.getProperty(ThirdEyeJobConstants.THIRDEYE_SERVER_URI.getPropertyName());
      if (thirdEyeServerUri == null)
      {
        throw new IllegalArgumentException(
                "Must provide " + ThirdEyeJobConstants.THIRDEYE_SERVER_URI.getPropertyName() + " in properties");
      }

      FileSystem fileSystem = FileSystem.get(new Configuration());

      // Push config
      Path configPath = new Path(root + File.separator + collection
                                         + File.separator + StarTreeConstants.CONFIG_FILE_NAME);
      InputStream configData = fileSystem.open(configPath);
      int responseCode = StarTreeJobUtils.pushConfig(configData, thirdEyeServerUri, collection);
      configData.close();
      LOG.info("Load {} #=> {}", configPath, responseCode);

      // Push star tree
      Path treePath = new Path(PhaseSpec.STARTREE_GENERATION.getTimeDir(root, collection, minTime, maxTime)
                                       + File.separator + PhaseSpec.STARTREE_GENERATION.getName()
                                       + File.separator + "star-tree-" + collection
                                       + File.separator + collection + "-" + StarTreeConstants.TREE_FILE_NAME);
      InputStream treeData = fileSystem.open(treePath);
      responseCode = StarTreeJobUtils.pushTree(treeData, thirdEyeServerUri, collection);
      treeData.close();
      LOG.info("Load {} #=> {}", treePath, responseCode);

      // Push data (with dimensions)
      Path dataPath = new Path(PhaseSpec.STARTREE_BOOTSTRAP_PHASE2.getTimeDir(root, collection, minTime, maxTime)
                                       + File.separator + PhaseSpec.STARTREE_BOOTSTRAP_PHASE2.getName());
      RemoteIterator<LocatedFileStatus> itr = fileSystem.listFiles(dataPath, false);
      while (itr.hasNext())
      {
        LocatedFileStatus fileStatus = itr.next();
        if (fileStatus.getPath().getName().startsWith("task_"))
        {
          InputStream leafData = fileSystem.open(fileStatus.getPath());
          responseCode = StarTreeJobUtils.pushData(leafData, thirdEyeServerUri, collection, true);
          leafData.close();
          LOG.info("Load {} #=> {}", fileStatus.getPath(), responseCode);
        }
      }
    }
    else // Hadoop job
    {
      // Construct job properties
      Properties jobProperties = phaseSpec.getJobProperties(root, collection, minTime, maxTime, inputPaths);

      // Instantiate the job
      Constructor<?> constructor = phaseSpec.getKlazz ().getConstructor(String.class, Properties.class);
      Object instance = constructor.newInstance(phaseSpec.getName(), jobProperties);

      // Run the job
      Method runMethod = instance.getClass().getMethod("run");
      runMethod.invoke(instance);
    }
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length != 2)
    {
      usage();
      System.exit(1);
    }

    String phaseName = args[0];

    Properties config = new Properties();
    config.load(new FileInputStream(args[1]));

    new ThirdEyeJob(phaseName, config).run();
  }
}
