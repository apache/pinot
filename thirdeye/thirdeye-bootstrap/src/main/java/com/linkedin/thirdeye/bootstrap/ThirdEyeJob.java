package com.linkedin.thirdeye.bootstrap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapred.Merger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Job.JobState;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.security.AccessControlException;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
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
import com.linkedin.thirdeye.bootstrap.util.TarGzBuilder;
import com.linkedin.thirdeye.impl.storage.IndexFormat;
import com.linkedin.thirdeye.impl.storage.IndexMetadata;

/**
 * Wrapper to manage Hadoop flows for ThirdEye. <h1>Config</h1>
 * <table>
 * <tr>
 * <th>Property</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>thirdeye.flow</td>
 * <td>One of {@link com.linkedin.thirdeye.bootstrap.ThirdEyeJob.FlowSpec}</td>
 * </tr>
 * <tr>
 * <td>thirdeye.flow.schedule</td>
 * <td>A string describing the flow schedule (used to tag segments)</td>
 * </tr>
 * <tr>
 * <td>thirdeye.phase</td>
 * <td>One of {@link com.linkedin.thirdeye.bootstrap.ThirdEyeJob.PhaseSpec}</td>
 * </tr>
 * <tr>
 * <td>thirdeye.root</td>
 * <td>Root directory on HDFS, under which all collection data is stored</td>
 * </tr>
 * <tr>
 * <td>thirdeye.collection</td>
 * <td>Collection name (data stored at ${thirdeye.root}/${thirdeye.collection}</td>
 * </tr>
 * <tr>
 * <td>thirdeye.server.uri</td>
 * <td>URI prefix for thirdeye server (e.g. http://some-machine:10283)</td>
 * </tr>
 * <tr>
 * <td>thirdeye.time.path</td>
 * <td>A path to a properties file on HDFS containing thirdeye.time.min, thirdeye.time.max</td>
 * </tr>
 * <tr>
 * <td>thirdeye.time.min</td>
 * <td>Manually override thirdeye.time.min from thirdeye.time.path</td>
 * </tr>
 * <tr>
 * <td>thirdeye.time.max</td>
 * <td>Manually override thirdeye.time.max from thirdeye.time.path</td>
 * </tr>
 * <tr>
 * <td>thirdeye.dimension.index.ref</td>
 * <td>Manually override thirdeye.dimension.index.dir to use, by default it will use the latest
 * directory under thirdeye.root/DIMENSION_INDEX</td>
 * </tr>
 * * <tr>
 * <td>thirdeye.cleanup.skip</td>
 * <td>Disable cleanup of old folders if this is set to true</td>
 * </tr>
 * * <tr>
 * <td>thirdeye.cleanup.daysago</td>
 * <td>Number of days ago to look while cleaning up old folders generated by hadoop workflow</td>
 * </tr>
 * * <tr>
 * <td>thirdeye.skip.missing</td>
 * <td>Filter all input paths in input.paths which do not exist if set to true</td>
 * </tr>
 * * <tr>
 * <td>thirdeye.folder.permission</td>
 * <td>The permissions to be set on thirdeye output folders</td>
 * </tr>
 * </table>
 */
public class ThirdEyeJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeJob.class);

  private static final String ENCODING = "UTF-8";
  private static final String USAGE = "usage: phase_name job.properties";
  private static final String AVRO_SCHEMA = "schema.avsc";
  private static final String TREE_FILE_FORMAT = ".bin";
  private static final String DEFAULT_CLEANUP_DAYS_AGO = "7";
  private static final String DEFAULT_CLEANUP_SKIP = "false";
  private static final String DEFAULT_SKIP_MISSING = "true";
  private static final String DEFAULT_FOLDER_PERMISSION = "755";
  private static final String FOLDER_PERMISSION_REGEX = "0?[1-7]{3}";
  private static final String DEFAULT_POLL_ENABLE = "false";
  private static final String DEFAULT_POLL_FREQUENCY = "60000";
  private static final String DEFAULT_POLL_TIMEOUT = "3600000";
  private static final String INPUT_PATHS_JOINER = ",";
  private static final String DATA_FOLDER_JOINER = "_";
  private static final String TEMPORARY_FOLDER = "_temporary";

  protected enum FlowSpec {
    DIMENSION_INDEX,
    METRIC_INDEX
  }

  private enum FlowSchedule {
    HOURLY,
    DAILY,
    MONTHLY
  }

  private enum PhaseSpec {
    JOIN {
      @Override
      Class<?> getKlazz() {
        return JoinPhaseJob.class;
      }

      @Override
      String getDescription() {
        return "Joins multiple data sets based on join key";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths) {
        return inputConfig;
      }
    },
    WAIT {
      @Override
      Class<?> getKlazz() {
        return null;
      }

      @Override
      String getDescription() {
        return "Polls a pre-determined amount of time for the existence of input paths";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection, FlowSpec flowSpec,
          DateTime minTime, DateTime maxTime, String inputPaths) throws Exception {
        return null;
      }
    },
    ANALYSIS {
      @Override
      Class<?> getKlazz() {
        return AnalysisPhaseJob.class;
      }

      @Override
      String getDescription() {
        return "Analyzes input Avro data to compute information necessary for job";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths)
              throws Exception {
        Properties config = new Properties();
        config.setProperty(AnalysisJobConstants.ANALYSIS_INPUT_AVRO_SCHEMA.toString(),
            getSchemaPath(root, collection));
        config.setProperty(AnalysisJobConstants.ANALYSIS_CONFIG_PATH.toString(),
            getConfigPath(root, collection));
        config.setProperty(AnalysisJobConstants.ANALYSIS_INPUT_PATH.toString(), inputPaths);
        config.setProperty(AnalysisJobConstants.ANALYSIS_OUTPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator + ANALYSIS.getName());


        return config;
      }
    },
    AGGREGATION {
      @Override
      Class<?> getKlazz() {
        return AggregatePhaseJob.class;
      }

      @Override
      String getDescription() {
        return "Aggregates input data";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths)
          throws Exception {
        Properties config = new Properties();

        config.setProperty(AggregationJobConstants.AGG_INPUT_AVRO_SCHEMA.toString(),
            getSchemaPath(root, collection));
        config.setProperty(AggregationJobConstants.AGG_CONFIG_PATH.toString(),
            getConfigPath(root, collection));

        boolean skipMissing = Boolean.parseBoolean(inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_SKIP_MISSING.getName(), DEFAULT_SKIP_MISSING));
        if (skipMissing) {
          inputPaths = getFilteredInputPaths(inputPaths);
        }

        config.setProperty(AggregationJobConstants.AGG_INPUT_PATH.toString(), inputPaths);
        config.setProperty(AggregationJobConstants.AGG_OUTPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + AGGREGATION.getName());

        config.setProperty(AggregationJobConstants.AGG_DIMENSION_STATS_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
            + StarTreeConstants.DIMENSION_STATS_FOLDER);

        return config;
      }
    },
    ROLLUP_PHASE1 {
      @Override
      Class<?> getKlazz() {
        return RollupPhaseOneJob.class;
      }

      @Override
      String getDescription() {
        return "Splits input data into above / below threshold using function";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths)
          throws Exception {
        Properties config = new Properties();

        config.setProperty(RollupPhaseOneConstants.ROLLUP_PHASE1_CONFIG_PATH.toString(),
            getConfigPath(root, collection));
        config.setProperty(RollupPhaseOneConstants.ROLLUP_PHASE1_INPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + AGGREGATION.getName());
        config.setProperty(RollupPhaseOneConstants.ROLLUP_PHASE1_OUTPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + ROLLUP_PHASE1.getName());

        return config;
      }
    },
    ROLLUP_PHASE2 {
      @Override
      Class<?> getKlazz() {
        return RollupPhaseTwoJob.class;
      }

      @Override
      String getDescription() {
        return "Aggregates all possible combinations of raw dimension combination below threshold";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths)
          throws Exception {
        Properties config = new Properties();

        config.setProperty(RollupPhaseTwoConstants.ROLLUP_PHASE2_CONFIG_PATH.toString(),
            getConfigPath(root, collection));
        config.setProperty(RollupPhaseTwoConstants.ROLLUP_PHASE2_INPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + ROLLUP_PHASE1.getName() + File.separator + "belowThreshold");
        config.setProperty(RollupPhaseTwoConstants.ROLLUP_PHASE2_OUTPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + ROLLUP_PHASE2.getName());
        config.setProperty(RollupPhaseTwoConstants.ROLLUP_PHASE2_ANALYSIS_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
            + StarTreeConstants.DIMENSION_STATS_FOLDER);

        return config;
      }
    },
    ROLLUP_PHASE3 {
      @Override
      Class<?> getKlazz() {
        return RollupPhaseThreeJob.class;
      }

      @Override
      String getDescription() {
        return "Selects the rolled-up dimension key for each raw dimension combination";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths)
          throws Exception {
        Properties config = new Properties();

        config.setProperty(RollupPhaseThreeConstants.ROLLUP_PHASE3_CONFIG_PATH.toString(),
            getConfigPath(root, collection));
        config.setProperty(RollupPhaseThreeConstants.ROLLUP_PHASE3_INPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + ROLLUP_PHASE2.getName());
        config.setProperty(RollupPhaseThreeConstants.ROLLUP_PHASE3_OUTPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + ROLLUP_PHASE3.getName());

        return config;
      }
    },
    ROLLUP_PHASE4 {
      @Override
      Class<?> getKlazz() {
        return RollupPhaseFourJob.class;
      }

      @Override
      String getDescription() {
        return "Sums metric time series by the rolled-up dimension key";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths)
          throws Exception {
        Properties config = new Properties();

        config.setProperty(RollupPhaseFourConstants.ROLLUP_PHASE4_CONFIG_PATH.toString(),
            getConfigPath(root, collection));
        config.setProperty(
            RollupPhaseFourConstants.ROLLUP_PHASE4_INPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + ROLLUP_PHASE3.getName() + ","
                + getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + ROLLUP_PHASE1.getName() + File.separator + "aboveThreshold");
        config.setProperty(RollupPhaseFourConstants.ROLLUP_PHASE4_OUTPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + ROLLUP_PHASE4.getName());

        return config;
      }
    },
    STARTREE_GENERATION {
      @Override
      Class<?> getKlazz() {
        return StarTreeGenerationJob.class;
      }

      @Override
      String getDescription() {
        return "Builds star tree index structure using rolled up dimension combinations and those above threshold";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths)
          throws Exception {
        Properties config = new Properties();

        config.setProperty(StarTreeGenerationConstants.STAR_TREE_GEN_CONFIG_PATH.toString(),
            getConfigPath(root, collection));
        config.setProperty(StarTreeGenerationConstants.STAR_TREE_GEN_INPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + ROLLUP_PHASE4.getName());
        config.setProperty(StarTreeGenerationConstants.STAR_TREE_GEN_OUTPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + STARTREE_GENERATION.getName());

        return config;
      }
    },
    STARTREE_BOOTSTRAP_PHASE1 {
      @Override
      Class<?> getKlazz() {
        return StarTreeBootstrapPhaseOneJob.class;
      }

      @Override
      String getDescription() {
        return "Sums raw Avro time-series data by dimension key";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths)
          throws Exception {
        Properties config = new Properties();
        String dimensionIndexDirRef =
            inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_DIMENSION_INDEX_REF.getName());
        config.setProperty(
            StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_CONFIG_PATH.toString(),
            getConfigPath(root, collection));
        config.setProperty(
            StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_INPUT_AVRO_SCHEMA.toString(),
            getSchemaPath(root, collection));
        config.setProperty(
            StarTreeBootstrapPhaseTwoConstants.STAR_TREE_GENERATION_OUTPUT_PATH.toString(),
            getDimensionIndexDir(root, collection, dimensionIndexDirRef) + File.separator
                + STARTREE_GENERATION.getName());

        boolean skipMissing = Boolean.parseBoolean(inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_SKIP_MISSING.getName(), DEFAULT_SKIP_MISSING));
        if (skipMissing) {
          inputPaths = getFilteredInputPaths(inputPaths);
        }

        config.setProperty(
            StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_INPUT_PATH.toString(),
            inputPaths);
        config.setProperty(
            StarTreeBootstrapPhaseOneConstants.STAR_TREE_BOOTSTRAP_OUTPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + STARTREE_BOOTSTRAP_PHASE1.getName());

        return config;
      }
    },
    STARTREE_BOOTSTRAP_PHASE2 {
      @Override
      Class<?> getKlazz() {
        return StarTreeBootstrapPhaseTwoJob.class;
      }

      @Override
      String getDescription() {
        return "Groups records by star tree leaf node and creates leaf buffers";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths)
          throws Exception {
        Properties config = new Properties();
        String dimensionIndexDirRef =
            inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_DIMENSION_INDEX_REF.getName());
        config.setProperty(
            StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_CONFIG_PATH.toString(),
            getConfigPath(root, collection));
        config.setProperty(
            StarTreeBootstrapPhaseTwoConstants.STAR_TREE_GENERATION_OUTPUT_PATH.toString(),
            getDimensionIndexDir(root, collection, dimensionIndexDirRef) + File.separator
                + STARTREE_GENERATION.getName());
        config.setProperty(
            StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_INPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + STARTREE_BOOTSTRAP_PHASE1.getName());
        config.setProperty(
            StarTreeBootstrapPhaseTwoConstants.STAR_TREE_BOOTSTRAP_PHASE2_OUTPUT_PATH.toString(),
            getMetricIndexDir(root, collection, flowSpec, minTime, maxTime) + File.separator
                + STARTREE_BOOTSTRAP_PHASE2.getName());

        return config;
      }
    },
    SERVER_PACKAGE {
      @Override
      Class<?> getKlazz() {
        return null; // unused
      }

      @Override
      String getDescription() {
        return "Packages data to upload to thirdeye.server.uri";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths)
          throws Exception {
        return null; // unused
      }
    },
    SERVER_UPLOAD {
      @Override
      Class<?> getKlazz() {
        return null; // unused
      }

      @Override
      String getDescription() {
        return "Pushes packaged data to thirdeye.server.uri";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection,
          FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths)
          throws Exception {
        return null; // unused
      }
    },
    CLEANUP {

      @Override
      Class<?> getKlazz() {
        return null;
      }

      @Override
      String getDescription() {

        return "Cleans up folders older than thirdeye.cleanup.daysago ago if "
            + "thirdeye.cleanup.skip is false";
      }

      @Override
      Properties getJobProperties(Properties inputConfig, String root, String collection, FlowSpec flowSpec,
          DateTime minTime, DateTime maxTime, String inputPaths) throws Exception {

        Properties config = new Properties();

        String thirdeyeCleanupDaysAgo =
            inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_CLEANUP_DAYSAGO.getName(),
                DEFAULT_CLEANUP_DAYS_AGO );
        config.setProperty(
            ThirdEyeJobConstants.THIRDEYE_CLEANUP_DAYSAGO.getName(),
            thirdeyeCleanupDaysAgo);

        String thirdeyeCleanupSkip =
            inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_CLEANUP_DAYSAGO.getName(),
                DEFAULT_CLEANUP_SKIP);
        config.setProperty(
            ThirdEyeJobConstants.THIRDEYE_CLEANUP_DAYSAGO.getName(),
            thirdeyeCleanupSkip);

        String schedule =
            inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_FLOW_SCHEDULE.getName());
        config.setProperty(
            ThirdEyeJobConstants.THIRDEYE_FLOW_SCHEDULE.getName(), schedule);



        return config;
      }

    };

    abstract Class<?> getKlazz();

    abstract String getDescription();

    abstract Properties getJobProperties(Properties inputConfig, String root, String collection,
        FlowSpec flowSpec, DateTime minTime, DateTime maxTime, String inputPaths) throws Exception;

    String getName() {
      return this.name().toLowerCase();
    }

    String getAnalysisPath(String root, String collection) {
      return getCollectionDir(root, collection) + File.separator + FlowSpec.DIMENSION_INDEX + File.separator + ANALYSIS.getName();
    }

    String getMetricIndexDir(String root, String collection, FlowSpec flowSpec, DateTime minTime,
        DateTime maxTime) throws IOException {
      return getCollectionDir(root, collection) + File.separator + flowSpec.name() + File.separator
          + "data_" + StarTreeConstants.DATE_TIME_FORMATTER.print(minTime) + "_"
          + StarTreeConstants.DATE_TIME_FORMATTER.print(maxTime);
    }

    String getDimensionIndexDir(String root, String collection, String dimensionIndexDirRef)
        throws IOException {
      return getCollectionDir(root, collection) + File.separator + FlowSpec.DIMENSION_INDEX
          + File.separator + dimensionIndexDirRef;
    }

    String getConfigPath(String root, String collection) {
      return getCollectionDir(root, collection) + File.separator
          + StarTreeConstants.CONFIG_FILE_NAME;
    }

    String getSchemaPath(String root, String collection) {
      return getCollectionDir(root, collection) + File.separator + AVRO_SCHEMA;
    }
  }

  private static void usage() {
    System.err.println(USAGE);
    for (PhaseSpec phase : PhaseSpec.values()) {
      System.err.printf("%-30s : %s\n", phase.getName(), phase.getDescription());
    }
  }

  private static String getAndCheck(String name, Properties properties) {
    String value = properties.getProperty(name);
    if (value == null) {
      throw new IllegalArgumentException("Must provide " + name);
    }
    return value;
  }


  private final String phaseName;
  private final Properties inputConfig;

  public ThirdEyeJob(String jobName, Properties config) {
    String phaseFromConfig = config.getProperty(ThirdEyeJobConstants.THIRDEYE_PHASE.getName());
    if (phaseFromConfig != null) {
      this.phaseName = phaseFromConfig;
    } else {
      this.phaseName = jobName;
    }

    this.inputConfig = config;
  }



  private IndexMetadata mergeIndexMetadata(List<IndexMetadata> indexMetdataList,
      Long startTimeMillis, Long endTimeMillis, String schedule)
  {
    Long min = Long.MAX_VALUE;
    Long minMillis = Long.MAX_VALUE;
    Long max = 0L;
    Long maxMillis = 0L;
    Long startTime = Long.MAX_VALUE;
    Long endTime = 0L;
    String aggregationGranularity = "";
    int bucketSize = 0;


    for (IndexMetadata indexMetadata : indexMetdataList)
    {
      if (aggregationGranularity.equals(""))
      {
        aggregationGranularity = indexMetadata.getAggregationGranularity();
        bucketSize = indexMetadata.getBucketSize();
      }
      if (indexMetadata.getMinDataTime() != null && indexMetadata.getMinDataTime() < min)
      {
        min = indexMetadata.getMinDataTime();
        minMillis = indexMetadata.getMinDataTimeMillis();
      }
      if (indexMetadata.getMaxDataTime() != null && indexMetadata.getMaxDataTime() > max)
      {
        max = indexMetadata.getMaxDataTime();
        maxMillis = indexMetadata.getMaxDataTimeMillis();
      }
    }

    startTime = TimeUnit.valueOf(aggregationGranularity).convert(startTimeMillis, TimeUnit.MILLISECONDS) / bucketSize;
    endTime = TimeUnit.valueOf(aggregationGranularity).convert(endTimeMillis, TimeUnit.MILLISECONDS) / bucketSize;

    IndexMetadata mergedIndexMetadata = new IndexMetadata
        (min, max, minMillis, maxMillis,
            startTime, endTime, startTimeMillis, endTimeMillis,
            schedule, aggregationGranularity, bucketSize, IndexFormat.VARIABLE_SIZE);

    return mergedIndexMetadata;
  }

  private void writeMergedIndexMetadataServerPush(FileSystem fileSystem, Path metadataPath, IndexMetadata mergedIndexMetadata) throws IOException {
    OutputStream os = null;
    try {
      os = fileSystem.create(metadataPath);

      Properties mergedProperties = mergedIndexMetadata.toProperties();
      mergedProperties.store(os, "Merged index metadata properties");



    } finally {

      if (os != null) os.close();
    }
  }

  private static String getFilteredInputPaths(String inputPaths) throws IOException {

    FileSystem fileSystem = FileSystem.get(new Configuration());

    String[] inputs = inputPaths.split(INPUT_PATHS_JOINER);
    List<String> filteredInputs = new ArrayList<String>();

    for (String input : inputs) {
      if (fileSystem.exists(new Path(input))) {
        filteredInputs.add(input);
      } else {
        LOGGER.info("Skipping missing input path {}", input);
      }
    }

    if (filteredInputs.isEmpty()) {
      throw new IllegalArgumentException("Must provide valid existing "+ThirdEyeJobConstants.INPUT_PATHS.getName());
    }

    return Joiner.on(INPUT_PATHS_JOINER).join(filteredInputs);
  }


  public void serverPackage(FileSystem fileSystem, String root, String collection, FlowSpec flowSpec, DateTime minTime, DateTime maxTime) throws Exception {

    // Get config file to package
    Path configPath =
        new Path(root + File.separator + collection + File.separator
            + StarTreeConstants.CONFIG_FILE_NAME);

    // Get data to package : overwrites existing data.tar.gz
    String schedule =
        inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_FLOW_SCHEDULE.getName());
    String metricIndexDir =
        PhaseSpec.STARTREE_BOOTSTRAP_PHASE2.getMetricIndexDir(root, collection, flowSpec,
            minTime, maxTime);
    String bootstrapPhase2Output =
        metricIndexDir + File.separator + PhaseSpec.STARTREE_BOOTSTRAP_PHASE2.getName();
    Path dataPath = new Path(bootstrapPhase2Output);
    String outputTarGzFile = metricIndexDir + "/data.tar.gz";
    Path outputTarGzFilePath = new Path(outputTarGzFile);

    LOGGER.info("START: Creating output {} to upload to server ", outputTarGzFilePath.getName());
    fileSystem.delete(new Path(dataPath, StarTreeConstants.METADATA_FILE_NAME), false);
    TarGzBuilder builder = new TarGzBuilder(outputTarGzFile, fileSystem, fileSystem);
    RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(dataPath, false);

    List<IndexMetadata> indexMetadataList = new ArrayList<IndexMetadata>();
    // get merged metadata file
    while (listFiles.hasNext()) {
      Path path = listFiles.next().getPath();
      IndexMetadata localIndexMetadata = builder.getMetadataObjectBootstrap(path);
      if (localIndexMetadata != null) {
        indexMetadataList.add(localIndexMetadata);
      }
    }
    if (indexMetadataList.size() == 0) {
      throw new IllegalStateException("No metadata files found");
    }
    Path metadataPath = new Path(bootstrapPhase2Output, StarTreeConstants.METADATA_FILE_NAME);
    IndexMetadata mergedIndexMetadata = mergeIndexMetadata(indexMetadataList,
        minTime.getMillis(), maxTime.getMillis(), schedule);
    writeMergedIndexMetadataServerPush(fileSystem, metadataPath, mergedIndexMetadata);

    listFiles = fileSystem.listFiles(dataPath, false);
    while (listFiles.hasNext()) {
      Path path = listFiles.next().getPath();
      LOGGER.info("Adding {}, to {}", path, outputTarGzFile);
      if (path.getName().equals(StarTreeConstants.TREE_FILE_NAME) ||
          path.getName().equals(StarTreeConstants.METADATA_FILE_NAME))
      {
        builder.addFileEntry(path);
      }
      else
      {
        // its either dimensionStore.tar.gz or metricStore-x.tar.gz
        builder.addTarGzFile(path);
      }
    }

    builder.addFileEntry(configPath);

    builder.finish();
    if (fileSystem.exists(outputTarGzFilePath)) {
      LOGGER.info("Successfully created {}.", outputTarGzFilePath);
    } else {
      throw new RuntimeException("Creation of" + outputTarGzFile + " failed");
    }
  }


  public void serverUpload(FileSystem fileSystem, String root, String collection, FlowSpec flowSpec, DateTime minTime, DateTime maxTime) throws Exception {
    String thirdEyeServerUri =
        inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_SERVER_URI.getName());
    if (thirdEyeServerUri == null) {
      throw new IllegalArgumentException("Must provide "
          + ThirdEyeJobConstants.THIRDEYE_SERVER_URI.getName() + " in properties");
    }

    // Push config (may 409 but that's okay)
    Path configPath =
        new Path(root + File.separator + collection + File.separator
            + StarTreeConstants.CONFIG_FILE_NAME);
    InputStream configData = fileSystem.open(configPath);
    int responseCode = StarTreeJobUtils.pushConfig(configData, thirdEyeServerUri, collection);
    configData.close();
    LOGGER.info("Load {} #=> {}", configPath, responseCode);

    // Push data
    String schedule =
        inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_FLOW_SCHEDULE.getName());
    String metricIndexDir =
        PhaseSpec.STARTREE_BOOTSTRAP_PHASE2.getMetricIndexDir(root, collection, flowSpec,
            minTime, maxTime);
    String outputTarGzFile = metricIndexDir + "/data.tar.gz";
    Path outputTarGzFilePath = new Path(outputTarGzFile);

    if (fileSystem.exists(outputTarGzFilePath)) {
      LOGGER.info("Uploading {} of size:{} to ThirdEye Server: {}", outputTarGzFile, fileSystem
          .getFileStatus(outputTarGzFilePath).getLen(), thirdEyeServerUri);
      FSDataInputStream outputDataStream = fileSystem.open(outputTarGzFilePath);
      responseCode =
          StarTreeJobUtils.pushData(outputDataStream, thirdEyeServerUri, collection, minTime,
              maxTime, schedule);
      LOGGER.info("Load {} #=> response code: {}", outputTarGzFile, responseCode);
    } else {
      throw new RuntimeException("Uploading of " + outputTarGzFile + " failed : package does not exist");
    }

    String thirdeyeFolderPermission = inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_FOLDER_PERMISSION.getName(), DEFAULT_FOLDER_PERMISSION);
    if (!thirdeyeFolderPermission.matches(FOLDER_PERMISSION_REGEX))
    {
      throw new IllegalArgumentException("Invalid folder permission mode. Must provide octal "+FOLDER_PERMISSION_REGEX);
    }

    String dimensionIndexDir = PhaseSpec.STARTREE_BOOTSTRAP_PHASE2.getDimensionIndexDir
        (root, collection, Joiner.on(DATA_FOLDER_JOINER).join(
            StarTreeConstants.DATA_DIR_PREFIX,
            StarTreeConstants.DATE_TIME_FORMATTER.print(minTime),
            StarTreeConstants.DATE_TIME_FORMATTER.print(maxTime)));

    FsShell shell = new FsShell(fileSystem.getConf());
    LOGGER.info("Changing permission of {} to {}", dimensionIndexDir, thirdeyeFolderPermission);
    shell.run(new String[]{"-chmod","-R",thirdeyeFolderPermission, dimensionIndexDir});
    LOGGER.info("Changing permission of {} to {}", metricIndexDir, thirdeyeFolderPermission);
    shell.run(new String[]{"-chmod","-R",thirdeyeFolderPermission, metricIndexDir});
  }

  private void purgeLowerGranularity(DateTime minTime, DateTime maxTime, String schedule, Path dimensionIndexDir, Path metricIndexDir) throws IOException {

    LOGGER.info("Created folder with higher granularity {} ", new Path(dimensionIndexDir,
        Joiner.on(DATA_FOLDER_JOINER).join(
          StarTreeConstants.DATA_DIR_PREFIX,
          StarTreeConstants.DATE_TIME_FORMATTER.print(minTime),
          StarTreeConstants.DATE_TIME_FORMATTER.print(maxTime))
          ));

    LOGGER.info("Created folder with higher granularity {} ", new Path(metricIndexDir,
        Joiner.on(DATA_FOLDER_JOINER).join(
          StarTreeConstants.DATA_DIR_PREFIX,
          StarTreeConstants.DATE_TIME_FORMATTER.print(minTime),
          StarTreeConstants.DATE_TIME_FORMATTER.print(maxTime))
          ));

    DateTime start = minTime;
    DateTime end ;
    FileSystem fileSystem = FileSystem.get(new Configuration());

    while (start.compareTo(maxTime) < 0)
    {
      if (schedule.equals(FlowSchedule.DAILY.name())) {
        end = start.plusHours(1);
      }
      else if (schedule.equals(FlowSchedule.MONTHLY.name())) {
        end = start.plusDays(1);
      }
      else {
        return;
      }

      Path folder = new Path(Joiner.on(DATA_FOLDER_JOINER).join(
          StarTreeConstants.DATA_DIR_PREFIX,
          StarTreeConstants.DATE_TIME_FORMATTER.print(start),
          StarTreeConstants.DATE_TIME_FORMATTER.print(end)));

      Path dimensionFolder = new Path(dimensionIndexDir, folder);
      Path metricFolder = new Path(metricIndexDir, folder);

      if (fileSystem.exists(dimensionFolder)) {
        LOGGER.info("Deleting folder {} with overlapping data and lower granularity", dimensionFolder);
        fileSystem.delete(dimensionFolder, true);
      }
      if (fileSystem.exists(metricFolder)) {
        LOGGER.info("Deleting folder {} with overlapping data and lower granularity", metricFolder);
        fileSystem.delete(metricFolder, true);
      }
      start = end;
    }

  }


  @SuppressWarnings("unchecked")
  public void run() throws Exception {
    LOGGER.info("Input config:{}", inputConfig);
    PhaseSpec phaseSpec;
    try {
      phaseSpec = PhaseSpec.valueOf(phaseName.toUpperCase());
    } catch (Exception e) {
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

    String root = getAndCheck(ThirdEyeJobConstants.THIRDEYE_ROOT.getName(), inputConfig);
    String collection =
        getAndCheck(ThirdEyeJobConstants.THIRDEYE_COLLECTION.getName(), inputConfig);
    String inputPaths = getAndCheck(ThirdEyeJobConstants.INPUT_PATHS.getName(), inputConfig);

        FlowSpec flowSpec = null;
    switch (phaseSpec) {
    case WAIT:
    case ANALYSIS:
    case AGGREGATION:
    case ROLLUP_PHASE1:
    case ROLLUP_PHASE2:
    case ROLLUP_PHASE3:
    case ROLLUP_PHASE4:
    case STARTREE_GENERATION:
      flowSpec = FlowSpec.DIMENSION_INDEX;
      break;
    case STARTREE_BOOTSTRAP_PHASE1:
    case STARTREE_BOOTSTRAP_PHASE2:
    case SERVER_PACKAGE:
    case SERVER_UPLOAD:
    case CLEANUP:
      flowSpec = FlowSpec.METRIC_INDEX;
      break;
    default:
      break;
    }

    // Get min / max time
    DateTime minTime;
    DateTime maxTime;

    String minTimeProp = inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_MIN.getName());
    String maxTimeProp = inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_MAX.getName());
    String timePathProp =
        inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_PATH.getName());
    FileSystem fileSystem = FileSystem.get(new Configuration());

    if (minTimeProp != null && maxTimeProp != null) // user provided, override
    {
      minTime = ISODateTimeFormat.dateTimeParser().parseDateTime(minTimeProp);
      maxTime = ISODateTimeFormat.dateTimeParser().parseDateTime(maxTimeProp);
    } else if (timePathProp != null) // use path managed by preparation jobs
    {
      InputStream inputStream = fileSystem.open(new Path(timePathProp));

      Properties timePathProps = new Properties();
      timePathProps.load(inputStream);
      inputStream.close();

      minTimeProp = timePathProps.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_MIN.getName());
      maxTimeProp = timePathProps.getProperty(ThirdEyeJobConstants.THIRDEYE_TIME_MAX.getName());

      minTime = ISODateTimeFormat.dateTimeParser().parseDateTime(minTimeProp);
      maxTime = ISODateTimeFormat.dateTimeParser().parseDateTime(maxTimeProp);
    } else {
      throw new IllegalStateException("Must specify either "
          + ThirdEyeJobConstants.THIRDEYE_TIME_PATH.getName() + " or "
          + ThirdEyeJobConstants.THIRDEYE_TIME_MIN.getName() + " and "
          + ThirdEyeJobConstants.THIRDEYE_TIME_MAX.getName());
    }

    if (PhaseSpec.SERVER_PACKAGE.equals(phaseSpec)) {
      serverPackage(fileSystem, root, collection, flowSpec, minTime, maxTime);
    } else if (PhaseSpec.SERVER_UPLOAD.equals(phaseSpec)) {
      serverUpload(fileSystem, root, collection, flowSpec, minTime, maxTime);
    } else if (PhaseSpec.WAIT.equals(phaseSpec)) {

      // Check if we need to poll for any input paths

      boolean pollEnable = Boolean.parseBoolean(inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_POLL_ENABLE.getName(), DEFAULT_POLL_ENABLE));

      if (pollEnable) {


        long elapsedTime = 0;
        long pollStart = (new DateTime()).getMillis();
        long pollFrequency = Long.parseLong(inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_POLL_FREQUENCY.getName(), DEFAULT_POLL_FREQUENCY));
        long pollTimeout = Long.parseLong(inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_POLL_TIMEOUT.getName(), DEFAULT_POLL_TIMEOUT));

        String[] inputs = inputConfig.getProperty(ThirdEyeJobConstants.INPUT_PATHS.getName()).split(INPUT_PATHS_JOINER);
        List<String> missingInputs;

        while (elapsedTime < pollTimeout)
        {
          missingInputs = new ArrayList<String>();
          for (String input : inputs) {

            LOGGER.info("Checking path {}", input);

            if (!fileSystem.exists(new Path(input)) ) {
              missingInputs.add(input);
              LOGGER.info("Missing input {}", input);
            }
            else {
              try {
                fileSystem.getFileStatus(new Path(input));

                if (fileSystem.exists(new Path(input, TEMPORARY_FOLDER))) {
                  missingInputs.add(input);
                  LOGGER.info("Data generation in progress");
                } else {
                  LOGGER.info("Path available {}", input);
                }
              } catch (AccessControlException e) {
                missingInputs.add(input);
                LOGGER.warn("No access to path {}", input, e);
              }
            }
          }

          if (missingInputs.size() == 0) {
            LOGGER.info("All paths available");
            break;
          }
          LOGGER.info("Poll after {} milliseconds", pollFrequency);
          TimeUnit.MILLISECONDS.sleep(pollFrequency);
          elapsedTime = new DateTime().getMillis() - pollStart;
          LOGGER.info("Time elapsed {} milliseconds", elapsedTime);
          inputs = missingInputs.toArray(new String[missingInputs.size()]);
        }

        if (elapsedTime > pollTimeout) {
          LOGGER.info("Timed out waiting for input");
        }

      }
    } else if (PhaseSpec.CLEANUP.equals(phaseSpec)) {

      boolean cleanupSkip = Boolean.parseBoolean(inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_CLEANUP_SKIP.getName(), DEFAULT_CLEANUP_SKIP));
      LOGGER.info("cleanup skip {}", cleanupSkip);
      if (cleanupSkip)
      {
        LOGGER.info("Skipping cleanup");
      }
      else
      {
        int cleanupDaysAgo = Integer.valueOf(inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_CLEANUP_DAYSAGO.getName(), DEFAULT_CLEANUP_DAYS_AGO));
        DateTime cleanupDaysAgoDate = (new DateTime()).minusDays(cleanupDaysAgo);

        Path dimensionIndexDir = new Path(getCollectionDir(root, collection) + File.separator + FlowSpec.DIMENSION_INDEX);
        Path metricIndexDir = new Path(getCollectionDir(root, collection) + File.separator + FlowSpec.METRIC_INDEX);

        LOGGER.info("Cleaning up {} {} days ago from paths {} and {}", cleanupDaysAgo, cleanupDaysAgoDate.getMillis(), dimensionIndexDir, metricIndexDir);


        // list folders in dimensionDir starting with data_
        FileStatus[] fileStatus = fileSystem.listStatus(dimensionIndexDir, new PathFilter() {

          @Override
          public boolean accept(Path path) {

            return path.getName().startsWith(StarTreeConstants.DATA_DIR_PREFIX);
          }
        });

        for (FileStatus file : fileStatus)
        {
          //get last modified date
          DateTime lastModifiedDate = new DateTime(file.getModificationTime());

          if (lastModifiedDate.isBefore(cleanupDaysAgoDate.getMillis()))
          {
            LOGGER.info("Deleting {}", file.getPath());
            fileSystem.delete(file.getPath(), true);
          }
        }


        // list folders in metricDir starting with data_
        fileStatus = fileSystem.listStatus(metricIndexDir, new PathFilter() {

          @Override
          public boolean accept(Path path) {

            return path.getName().startsWith(StarTreeConstants.DATA_DIR_PREFIX);
          }
        });

        for (FileStatus file : fileStatus)
        {
        //get last modified date
          DateTime lastModifiedDate = new DateTime(file.getModificationTime());

          if (lastModifiedDate.isBefore(cleanupDaysAgoDate.getMillis()))
          {
            Path startreeBootstrapPath1 = new Path(file.getPath(), PhaseSpec.STARTREE_BOOTSTRAP_PHASE1.getName().toLowerCase());
            if (fileSystem.exists(startreeBootstrapPath1)) {
              LOGGER.info("Deleting {}", startreeBootstrapPath1);
              fileSystem.delete(startreeBootstrapPath1, true);
            }

            Path startreeBootstrapPath2 = new Path(file.getPath(), PhaseSpec.STARTREE_BOOTSTRAP_PHASE2.getName().toLowerCase());
            if (fileSystem.exists(startreeBootstrapPath2)) {
              LOGGER.info("Deleting {}", startreeBootstrapPath2);
              fileSystem.delete(startreeBootstrapPath2, true);
            }

          }
        }

        String schedule = inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_FLOW_SCHEDULE.getName());
        LOGGER.info("schedule : {} ",schedule);

        purgeLowerGranularity(minTime, maxTime, schedule, dimensionIndexDir, metricIndexDir);

      }
    }
    else // Hadoop job
    {
      if (FlowSpec.METRIC_INDEX.equals(flowSpec)) {
        String dimensionIndexRef =
            inputConfig.getProperty(ThirdEyeJobConstants.THIRDEYE_DIMENSION_INDEX_REF.getName());
        if (dimensionIndexRef == null) {
          String msg =
              "dimensionIndexRef:" + dimensionIndexRef + ".Must provide "
                  + ThirdEyeJobConstants.THIRDEYE_DIMENSION_INDEX_REF.getName() + " in properties";
          LOGGER.error(msg);
          throw new IllegalArgumentException(msg);
        }
      }
      // Construct job properties
      Properties jobProperties =
          phaseSpec.getJobProperties(inputConfig, root, collection, flowSpec, minTime, maxTime,
              inputPaths);

      String numReducers = inputConfig.getProperty(phaseSpec.getName() + ".num.reducers");
      if (numReducers != null) {
        jobProperties.put("num.reducers", numReducers);
      }

      // Instantiate the job
      Constructor<Configured> constructor =
          (Constructor<Configured>) phaseSpec.getKlazz().getConstructor(String.class,
              Properties.class);
      Configured instance = constructor.newInstance(phaseSpec.getName(), jobProperties);

      // Run the job
      Method runMethod = instance.getClass().getMethod("run");
      Job job = (Job) runMethod.invoke(instance);
      JobStatus status = job.getStatus();
      if(status.getState() != JobStatus.State.SUCCEEDED){
        throw new RuntimeException("Job "+job.getJobName()+" failed to execute: Ran with config:"+ jobProperties);
      }
    }

  }



  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      usage();
      System.exit(1);
    }

    String phaseName = args[0];

    Properties config = new Properties();
    config.load(new FileInputStream(args[1]));
    new ThirdEyeJob(phaseName, config).run();
  }

  private static String getCollectionDir(String root, String collection) {
    return root == null ? collection : root + File.separator + collection;
  }
}
