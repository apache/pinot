package com.linkedin.thirdeye.bootstrap;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.bootstrap.aggregation.AggregatePhaseJob;
import com.linkedin.thirdeye.bootstrap.aggregation.AggregationJobConstants;
import com.linkedin.thirdeye.bootstrap.analysis.AnalysisJobConstants;
import com.linkedin.thirdeye.bootstrap.analysis.AnalysisPhaseJob;
import com.linkedin.thirdeye.bootstrap.join.JoinPhaseJob;
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

/**
 * Wrapper to manage Hadoop flows for ThirdEye.
 *
 * <h1>Config</h1>
 *
 * <table>
 *   <tr>
 *     <th>Property</th>
 *     <th>Description</th>
 *   </tr>
 *   <tr>
 *     <td>thirdeye.flow</td>
 *     <td>One of {@link com.linkedin.thirdeye.bootstrap.ThirdEyeJob.FlowSpec}</td>
 *   </tr>
 *   <tr>
 *     <td>thirdeye.flow.schedule</td>
 *     <td>A string describing the flow schedule (used to tag segments)</td>
 *   </tr>
 *   <tr>
 *     <td>thirdeye.phase</td>
 *     <td>One of {@link com.linkedin.thirdeye.bootstrap.ThirdEyeJob.PhaseSpec}</td>
 *   </tr>
 *   <tr>
 *     <td>thirdeye.root</td>
 *     <td>Root directory on HDFS, under which all collection data is stored</td>
 *   </tr>
 *   <tr>
 *     <td>thirdeye.collection</td>
 *     <td>Collection name (data stored at ${thirdeye.root}/${thirdeye.collection}</td>
 *   </tr>
 *   <tr>
 *     <td>thirdeye.server.uri</td>
 *     <td>URI prefix for thirdeye server (e.g. http://some-machine:10283)</td>
 *   </tr>
 *   <tr>
 *     <td>thirdeye.time.path</td>
 *     <td>A path to a properties file on HDFS containing thirdeye.time.min, thirdeye.time.max</td>
 *   </tr>
 *   <tr>
 *     <td>thirdeye.time.min</td>
 *     <td>Manually override thirdeye.time.min from thirdeye.time.path</td>
 *   </tr>
 *   <tr>
 *     <td>thirdeye.time.max</td>
 *     <td>Manually override thirdeye.time.max from thirdeye.time.path</td>
 *   </tr>
 * </table>
 */
public class ThirdEyeJob
{
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeJob.class);

  private static final String ENCODING = "UTF-8";
  private static final String USAGE = "usage: phase_name job.properties";
  private static final String AVRO_SCHEMA = "schema.avsc";
  private static final String TREE_FILE_FORMAT = ".bin";

  private enum FlowSpec
  {
    BOOTSTRAP,
    INCREMENT,
    PATCH
  }

  private enum PhaseSpec
  {
    JOIN
    {
      @Override
      Class<?> getKlazz()
      {
        return JoinPhaseJob.class;
      }

      @Override
      String getDescription()
      {
        return "Joins multiple data sets based on join key";
      }

      @Override
      Properties getJobProperties(Properties inputConfig,
                                  String root,
                                  String collection,
                                  FlowSpec flowSpec,
                                  DateTime minTime,
                                  DateTime maxTime,
                                  String inputPaths)
      {
        return inputConfig;
      }
    },
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
              Properties getJobProperties(Properties inputConfig,
                                          String root,
                                          String collection,
                                          FlowSpec flowSpec,
                                          DateTime minTime,
                                          DateTime maxTime,
                                          String inputPaths)
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
              Properties getJobProperties(Properties inputConfig,
                                          String root,
                                          String collection,
                                          FlowSpec flowSpec,
                                          DateTime minTime,
                                          DateTime maxTime,
                                          String inputPaths) throws Exception
              {
                Properties config = new Properties();

                
                config.setProperty(AggregationJobConstants.AGG_INPUT_AVRO_SCHEMA.toString(),
                                   getSchemaPath(root, collection));
                config.setProperty(AggregationJobConstants.AGG_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(AggregationJobConstants.AGG_INPUT_PATH.toString(),
                                   inputPaths);
                config.setProperty(AggregationJobConstants.AGG_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + AGGREGATION.getName());

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
              Properties getJobProperties(Properties inputConfig,
                                          String root,
                                          String collection,
                                          FlowSpec flowSpec,
                                          DateTime minTime,
                                          DateTime maxTime,
                                          String inputPaths) throws Exception
              {
                Properties config = new Properties();

                config.setProperty(RollupPhaseOneConstants.ROLLUP_PHASE1_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(RollupPhaseOneConstants.ROLLUP_PHASE1_INPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + AGGREGATION.getName());
                config.setProperty(RollupPhaseOneConstants.ROLLUP_PHASE1_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + ROLLUP_PHASE1.getName());

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
              Properties getJobProperties(Properties inputConfig,
                                          String root,
                                          String collection,
                                          FlowSpec flowSpec,
                                          DateTime minTime,
                                          DateTime maxTime,
                                          String inputPaths) throws Exception
              {
                Properties config = new Properties();

                config.setProperty(RollupPhaseTwoConstants.ROLLUP_PHASE2_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(RollupPhaseTwoConstants.ROLLUP_PHASE2_INPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + ROLLUP_PHASE1.getName() + File.separator + "belowThreshold");
                config.setProperty(RollupPhaseTwoConstants.ROLLUP_PHASE2_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + ROLLUP_PHASE2.getName());

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
              Properties getJobProperties(Properties inputConfig,
                                          String root,
                                          String collection,
                                          FlowSpec flowSpec,
                                          DateTime minTime,
                                          DateTime maxTime,
                                          String inputPaths) throws Exception
              {
                Properties config = new Properties();

                config.setProperty(RollupPhaseThreeConstants.ROLLUP_PHASE3_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(RollupPhaseThreeConstants.ROLLUP_PHASE3_INPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + ROLLUP_PHASE2.getName());
                config.setProperty(RollupPhaseThreeConstants.ROLLUP_PHASE3_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + ROLLUP_PHASE3.getName());

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
              Properties getJobProperties(Properties inputConfig,
                                          String root,
                                          String collection,
                                          FlowSpec flowSpec,
                                          DateTime minTime,
                                          DateTime maxTime,
                                          String inputPaths) throws Exception
              {
                Properties config = new Properties();

                config.setProperty(RollupPhaseFourConstants.ROLLUP_PHASE4_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(RollupPhaseFourConstants.ROLLUP_PHASE4_INPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + ROLLUP_PHASE3.getName() + "," +
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + ROLLUP_PHASE1.getName() + File.separator + "aboveThreshold");
                config.setProperty(RollupPhaseFourConstants.ROLLUP_PHASE4_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + ROLLUP_PHASE4.getName());

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
              Properties getJobProperties(Properties inputConfig,
                                          String root,
                                          String collection,
                                          FlowSpec flowSpec,
                                          DateTime minTime,
                                          DateTime maxTime,
                                          String inputPaths) throws Exception
              {
                Properties config = new Properties();

                config.setProperty(StarTreeGenerationConstants.STAR_TREE_GEN_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(StarTreeGenerationConstants.STAR_TREE_GEN_INPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + ROLLUP_PHASE4.getName());
                config.setProperty(StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + STARTREE_GENERATION.getName());

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
              Properties getJobProperties(Properties inputConfig,
                                          String root,
                                          String collection,
                                          FlowSpec flowSpec,
                                          DateTime minTime,
                                          DateTime maxTime,
                                          String inputPaths) throws Exception
              {
                Properties config = new Properties();
                config.setProperty(StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_INPUT_AVRO_SCHEMA.toString(),
                                   getSchemaPath(root, collection));
                config.setProperty(StarTreeBootstrapPhaseOneConstants.STAR_TREE_GENERATION_OUTPUT_PATH.toString(),
                                   getLatestTreeDirPath(root, collection) + File.separator + STARTREE_GENERATION.getName());
                config.setProperty(StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_INPUT_PATH.toString(),
                                   inputPaths);
                config.setProperty(StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + STARTREE_BOOTSTRAP_PHASE1.getName());

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
              Properties getJobProperties(Properties inputConfig,
                                          String root,
                                          String collection,
                                          FlowSpec flowSpec,
                                          DateTime minTime,
                                          DateTime maxTime,
                                          String inputPaths) throws Exception
              {
                Properties config = new Properties();

                config.setProperty(StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH.toString(),
                                   getConfigPath(root, collection));
                config.setProperty(StarTreeBootstrapPhaseTwoConstants.STAR_TREE_GENERATION_OUTPUT_PATH.toString(),
                                   getLatestTreeDirPath(root, collection) + File.separator + STARTREE_GENERATION.getName());
                config.setProperty(StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_INPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + STARTREE_BOOTSTRAP_PHASE1.getName());
                config.setProperty(StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH.toString(),
                                   getTimeDir(root, collection, flowSpec, minTime, maxTime) + File.separator + STARTREE_BOOTSTRAP_PHASE2.getName());

                return config;
              }
            },
    SERVER_PUSH
            {
              @Override
              Class<?> getKlazz()
              {
                return null; // unused
              }

              @Override
              String getDescription()
              {
                return "Pushes data to thirdeye.server.uri";
              }

              @Override
              Properties getJobProperties(Properties inputConfig, String root, String collection, FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths) throws Exception
              {
                return null; // unused
              }
            };

    abstract Class<?> getKlazz();
    
    abstract String getDescription();
    
    abstract Properties getJobProperties(Properties inputConfig,
                                         String root,
                                         String collection,
                                         FlowSpec flowSpec,
                                         DateTime minTime,
                                         DateTime maxTime,
                                         String inputPaths) throws Exception;

    String getName()
    {
      return this.name().toLowerCase();
    }

    String getAnalysisPath(String root, String collection)
    {
      return getCollectionDir(root, collection) + File.separator + "analysis";
    }

    String getTimeDir(String root,
                      String collection,
                      FlowSpec flowSpec,
                      DateTime minTime,
                      DateTime maxTime) throws IOException
    {
      return getCollectionDir(root, collection)
              + File.separator + flowSpec.name()
              + File.separator + "data_"
              + StarTreeConstants.DATE_TIME_FORMATTER.print(minTime) + "_"
              + StarTreeConstants.DATE_TIME_FORMATTER.print(maxTime);
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
  private final Properties inputConfig;

  public ThirdEyeJob(String jobName, Properties config)
  {
    String phaseFromConfig = config.getProperty(ThirdEyeJobConstants.THIRDEYE_PHASE.getPropertyName());
    if (phaseFromConfig != null)
    {
      this.phaseName = phaseFromConfig;
    }
    else
    {
      this.phaseName = jobName;
    }

    this.inputConfig = config;
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
    /**
     * This phase is optional for the pipeline
     */
    if (PhaseSpec.JOIN.equals(phaseSpec)) {
      JoinPhaseJob job = new JoinPhaseJob("Join Job", inputConfig);
      job.run();
      return;
    }

    String root = getAndCheck(ThirdEyeJobConstants.THIRDEYE_ROOT.getPropertyName(), inputConfig);
    String collection = getAndCheck(ThirdEyeJobConstants.THIRDEYE_COLLECTION.getPropertyName(), inputConfig);
    String inputPaths = getAndCheck(ThirdEyeJobConstants.INPUT_PATHS.getPropertyName(), inputConfig);
    FlowSpec flowSpec = FlowSpec.valueOf(getAndCheck(ThirdEyeJobConstants.THIRDEYE_FLOW.getPropertyName(), inputConfig).toUpperCase());
    String numberReducersProp = StarTreeBootstrapPhaseTwoConstants.THIRDEYE_STARTREE_BOOTSTRAP_PHASE2_REDUCERS.name();
    String numberOfReducers = inputConfig.getProperty(numberReducersProp);

    // Get min / max time
    DateTime minTime;
    DateTime maxTime;

    String minTimeProp
            = inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_MIN.getPropertyName());
    String maxTimeProp
            = inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_MAX.getPropertyName());
    String timePathProp
            = inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_PATH.getPropertyName());

    if (minTimeProp != null && maxTimeProp != null) // user provided, override
    {
      minTime = ISODateTimeFormat.dateTimeParser().parseDateTime(minTimeProp);
      maxTime = ISODateTimeFormat.dateTimeParser().parseDateTime(maxTimeProp);
    }
    else if (timePathProp != null) // use path managed by preparation jobs
    {
      FileSystem fileSystem = FileSystem.get(new Configuration());
      InputStream inputStream = fileSystem.open(new Path(timePathProp));

      Properties timePathProps = new Properties();
      timePathProps.load(inputStream);
      inputStream.close();

      minTimeProp = timePathProps.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_MIN.getPropertyName());
      maxTimeProp = timePathProps.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_MAX.getPropertyName());

      minTime = ISODateTimeFormat.dateTimeParser().parseDateTime(minTimeProp);
      maxTime = ISODateTimeFormat.dateTimeParser().parseDateTime(maxTimeProp);
    }
    else
    {
      throw new IllegalStateException(
              "Must specify either "
                      + ThirdEyeJobConstants.THIRDEYE_TIME_PATH.getPropertyName() + " or "
                      + ThirdEyeJobConstants.THIRDEYE_TIME_MIN.getPropertyName() + " and "
                      + ThirdEyeJobConstants.THIRDEYE_TIME_MAX.getPropertyName());
    }

    if (PhaseSpec.SERVER_PUSH.equals(phaseSpec))
    {
      String thirdEyeServerUri = inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_SERVER_URI.getPropertyName());
      if (thirdEyeServerUri == null)
      {
        throw new IllegalArgumentException(
                "Must provide " + ThirdEyeJobConstants.THIRDEYE_SERVER_URI.getPropertyName() + " in properties");
      }

      FileSystem fileSystem = FileSystem.get(new Configuration());

      // Push config (may 409 but that's okay)
      Path configPath = new Path(root + File.separator + collection
                                     + File.separator + StarTreeConstants.CONFIG_FILE_NAME);
      InputStream configData = fileSystem.open(configPath);
      int responseCode = StarTreeJobUtils.pushConfig(configData, thirdEyeServerUri, collection);
      configData.close();
      LOG.info("Load {} #=> {}", configPath, responseCode);

      // Push data
      Path dataPath = new Path(PhaseSpec.STARTREE_BOOTSTRAP_PHASE2.getTimeDir(root, collection, flowSpec, minTime, maxTime)
                                       + File.separator + PhaseSpec.STARTREE_BOOTSTRAP_PHASE2.getName());
      RemoteIterator<LocatedFileStatus> itr = fileSystem.listFiles(dataPath, false);
      while (itr.hasNext())
      {
        LocatedFileStatus fileStatus = itr.next();
        if (fileStatus.getPath().getName().startsWith("task_"))
        {
          InputStream leafData = fileSystem.open(fileStatus.getPath());
          responseCode = StarTreeJobUtils.pushData(
                  leafData,
                  thirdEyeServerUri,
                  collection,
                  minTime,
                  maxTime,
                  inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_FLOW_SCHEDULE.getPropertyName()));
          leafData.close();
          LOG.info("Load {} #=> {}", fileStatus.getPath(), responseCode);
        }
      }
    }
    else // Hadoop job
    {
      // Construct job properties
      Properties jobProperties = phaseSpec.getJobProperties(inputConfig, root, collection, flowSpec, minTime, maxTime, inputPaths);

      if(PhaseSpec.STARTREE_BOOTSTRAP_PHASE2.equals(phaseSpec) && numberOfReducers != null){
         jobProperties.setProperty(numberReducersProp, numberOfReducers);
      }
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

  private static String getCollectionDir(String root, String collection)
  {
    return root == null ? collection : root + File.separator + collection;
  }

  /*
   * Iterates in the data dir's generated in reverse order and returns the path
   * of the latest dir which contains tree.bin file.
   */
  private static String getLatestTreeDirPath(String root, String collection) throws IOException
  {
    FileSystem fs = FileSystem.get(new Configuration());
    Path bootstrapDir = new Path(getCollectionDir(root, collection), FlowSpec.BOOTSTRAP.name());

    PathFilter dataDirFilter = new PathFilter()
    {
      public boolean accept(Path path)
      {
        return path.getName().startsWith("data_");
      }
    };

    Comparator<FileStatus> dataDirComparator = new Comparator<FileStatus>()
    {

      public int compare(FileStatus dataDir1, FileStatus dataDir2)
      {
        return (int) (getMaxTimeFromPath(dataDir2.getPath().toString()) - getMaxTimeFromPath(dataDir1.getPath().toString()));
      }

      private long getMaxTimeFromPath(String path)
      {
        try
        {
          String[] pathTokens = path.split(File.separator);
          String[] dataDirTokens = pathTokens[pathTokens.length - 1].split("_");
          String maxTimeString = URLDecoder.decode(dataDirTokens[dataDirTokens.length - 1], ENCODING);
          DateTime maxTime = StarTreeConstants.DATE_TIME_FORMATTER.parseDateTime(maxTimeString);
          return maxTime.getMillis();
        }
        catch (Exception e)
        {
          throw new IllegalStateException(e);
        }
      }
    };

    List<FileStatus> listFiles = Arrays.asList(fs.listStatus(bootstrapDir, dataDirFilter));
    Collections.sort(listFiles, dataDirComparator);
    for (int i = 0; i < listFiles.size(); i++)
    {
      RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(listFiles.get(i).getPath(), true);

      while (fileStatusListIterator.hasNext())
      {
        LocatedFileStatus fileStatus = fileStatusListIterator.next();
        if (fileStatus.getPath().getName().endsWith(TREE_FILE_FORMAT))
        {
          return listFiles.get(i).getPath().toString();
        }
      }
    }
    throw new IllegalStateException("Could not find star tree directory");
  }
}
