/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.thirdeye.hadoop.segment.creation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Joiner;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec.TimeFormat;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.StatsCollectorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.creator.impl.stats.LongColumnPreIndexStatsCollector;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConfig;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConstants;
import com.linkedin.thirdeye.hadoop.util.ThirdeyePinotSchemaUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.core.segment.creator.impl.V1Constants.MetadataKeys.Segment.*;
import static com.linkedin.thirdeye.hadoop.segment.creation.SegmentCreationPhaseConstants.*;

/**
 * Mapper class for SegmentCreation job, which sets configs required for
 * segment generation with star tree index
 */
public class SegmentCreationPhaseMapReduceJob {

  public static class SegmentCreationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCreationPhaseMapReduceJob.class);
    private static ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

    private Configuration properties;

    private String inputFilePath;
    private String outputPath;
    private String tableName;

    private Path currentHdfsWorkDir;
    private String currentDiskWorkDir;

    // Temporary HDFS path for local machine
    private String localHdfsSegmentTarPath;

    private String localDiskSegmentDirectory;
    private String localDiskSegmentTarPath;

    private ThirdEyeConfig thirdeyeConfig;
    private Schema schema;

    private Long segmentWallClockStartTime;
    private Long segmentWallClockEndTime;
    private String segmentSchedule;
    private boolean isBackfill;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      currentHdfsWorkDir = FileOutputFormat.getWorkOutputPath(context);
      currentDiskWorkDir = "pinot_hadoop_tmp";

      // Temporary HDFS path for local machine
      localHdfsSegmentTarPath = currentHdfsWorkDir + "/segmentTar";

      // Temporary DISK path for local machine
      localDiskSegmentDirectory = currentDiskWorkDir + "/segments/";
      localDiskSegmentTarPath = currentDiskWorkDir + "/segmentsTar/";
      new File(localDiskSegmentTarPath).mkdirs();

      LOGGER.info("*********************************************************************");
      LOGGER.info("Configurations : {}", context.getConfiguration().toString());
      LOGGER.info("*********************************************************************");
      LOGGER.info("Current HDFS working dir : {}", currentHdfsWorkDir);
      LOGGER.info("Current DISK working dir : {}", new File(currentDiskWorkDir).getAbsolutePath());
      LOGGER.info("*********************************************************************");
      properties = context.getConfiguration();

      outputPath = properties.get(SEGMENT_CREATION_OUTPUT_PATH.toString());

      thirdeyeConfig = OBJECT_MAPPER.readValue(properties.get(SEGMENT_CREATION_THIRDEYE_CONFIG.toString()), ThirdEyeConfig.class);
      LOGGER.info(thirdeyeConfig.encode());
      schema = ThirdeyePinotSchemaUtils.createSchema(thirdeyeConfig);
      tableName = thirdeyeConfig.getCollection();

      segmentWallClockStartTime = Long.valueOf(properties.get(SEGMENT_CREATION_WALLCLOCK_START_TIME.toString()));
      segmentWallClockEndTime = Long.valueOf(properties.get(SEGMENT_CREATION_WALLCLOCK_END_TIME.toString()));
      segmentSchedule = properties.get(SEGMENT_CREATION_SCHEDULE.toString());
      isBackfill = Boolean.valueOf(properties.get(SEGMENT_CREATION_BACKFILL.toString()));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      FileUtils.deleteQuietly(new File(currentDiskWorkDir));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      String line = value.toString();
      String[] lineSplits = line.split(" ");

      LOGGER.info("*********************************************************************");
      LOGGER.info("mapper input : {}", value);
      LOGGER.info("Path to output : {}", outputPath);
      LOGGER.info("Table name : {}", tableName);
      LOGGER.info("num lines : {}", lineSplits.length);

      for (String split : lineSplits) {
        LOGGER.info("Command line : {}", split);
      }
      LOGGER.info("*********************************************************************");

      if (lineSplits.length != 3) {
        throw new RuntimeException("Input to the mapper is malformed, please contact the pinot team");
      }
      inputFilePath = lineSplits[1].trim();

      LOGGER.info("*********************************************************************");
      LOGGER.info("input data file path : {}", inputFilePath);
      LOGGER.info("local hdfs segment tar path: {}", localHdfsSegmentTarPath);
      LOGGER.info("local disk segment path: {}", localDiskSegmentDirectory);
      LOGGER.info("*********************************************************************");

      try {
        createSegment(inputFilePath, schema, lineSplits[2]);
        LOGGER.info("finished segment creation job successfully");
      } catch (Exception e) {
        LOGGER.error("Got exceptions during creating segments!", e);
      }

      context.write(new LongWritable(Long.parseLong(lineSplits[2])),
          new Text(FileSystem.get(new Configuration()).listStatus(new Path(localHdfsSegmentTarPath + "/"))[0].getPath().getName()));
      LOGGER.info("finished the job successfully");
    }

    private String createSegment(String dataFilePath, Schema schema, String seqId) throws Exception {
      final FileSystem fs = FileSystem.get(new Configuration());
      final Path hdfsDataPath = new Path(dataFilePath);
      final File dataPath = new File(currentDiskWorkDir, "data");
      if (dataPath.exists()) {
        dataPath.delete();
      }
      dataPath.mkdir();
      final Path localFilePath = new Path(dataPath + "/" + hdfsDataPath.getName());
      fs.copyToLocalFile(hdfsDataPath, localFilePath);

      LOGGER.info("Data schema is : {}", schema);

      // Set segment generator config
      LOGGER.info("*********************************************************************");
      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
      segmentGeneratorConfig.setTableName(tableName);
      segmentGeneratorConfig.setInputFilePath(new File(dataPath, hdfsDataPath.getName()).getAbsolutePath());
      LOGGER.info("Setting input path {}", segmentGeneratorConfig.getInputFilePath());
      segmentGeneratorConfig.setFormat(FileFormat.AVRO);
      segmentGeneratorConfig.setSegmentNamePostfix(seqId);
      segmentGeneratorConfig.setOutDir(localDiskSegmentDirectory);
      LOGGER.info("Setting enableStarTreeIndex");
      String minTime = ThirdEyeConstants.DATE_TIME_FORMATTER.print(segmentWallClockStartTime);
      String maxTime = ThirdEyeConstants.DATE_TIME_FORMATTER.print(segmentWallClockEndTime);
      LOGGER.info("Wall clock time : min {} max {}", minTime, maxTime);
      LOGGER.info("isBackfill : {}", isBackfill);
      if (isBackfill) {
        // if case of backfill, we have to ensure that segment name is same as original segment name
        // we are retaining the segment name through the  backfill and derived_column_transformation phases
        // in the output files generated
        // backfill will generated original_segment_name.avro
        // derived_column_transformation will generate original_segment_name-m-00000.avro etc
        String segmentName = hdfsDataPath.getName().split("-(m|r)-[0-9]{5}")[0];
        segmentName = segmentName.split(ThirdEyeConstants.AVRO_SUFFIX)[0];
        segmentGeneratorConfig.setSegmentName(segmentName);
      } else {
        String segmentName =
            Joiner.on(ThirdEyeConstants.SEGMENT_JOINER).join(tableName, segmentSchedule, minTime, maxTime, seqId);
        segmentGeneratorConfig.setSegmentName(segmentName);
      }
      LOGGER.info("Setting segment name {}", segmentGeneratorConfig.getSegmentName());


      // Set star tree config
      StarTreeIndexSpec starTreeIndexSpec = new StarTreeIndexSpec();

      // _raw dimensions should not be in star tree split order
      // if a dimension has a _topk column, we will include only
      // the column with topk, and skip _raw column for materialization in star tree
      Set<String> skipMaterializationForDimensions = new HashSet<>();
      Set<String> transformDimensionsSet = thirdeyeConfig.getTransformDimensions();
      LOGGER.info("Dimensions with _topk column {}", transformDimensionsSet);
      for (String topkTransformDimension : transformDimensionsSet) {
        skipMaterializationForDimensions.add(topkTransformDimension);
        LOGGER.info("Adding {} to skipMaterialization set", topkTransformDimension);
      }
      starTreeIndexSpec.setSkipMaterializationForDimensions(skipMaterializationForDimensions);
      LOGGER.info("Setting skipMaterializationForDimensions {}", skipMaterializationForDimensions);

      if (thirdeyeConfig.getSplit() != null) {
        starTreeIndexSpec.setMaxLeafRecords(thirdeyeConfig.getSplit().getThreshold());
        LOGGER.info("Setting split threshold to {}", starTreeIndexSpec.getMaxLeafRecords());
        List<String> splitOrder = thirdeyeConfig.getSplit().getOrder();
        if (splitOrder != null) {
          LOGGER.info("Removing from splitOrder, any dimensions which are also in skipMaterializationForDimensions");
          splitOrder.removeAll(skipMaterializationForDimensions);
          starTreeIndexSpec.setDimensionsSplitOrder(splitOrder);
        }
        LOGGER.info("Setting splitOrder {}", splitOrder);
      }
      segmentGeneratorConfig.enableStarTreeIndex(starTreeIndexSpec);
      LOGGER.info("*********************************************************************");

      // Set time for SIMPLE_DATE_FORMAT case
      String sdfPrefix = TimeFormat.SIMPLE_DATE_FORMAT.toString() + ThirdEyeConstants.SDF_SEPARATOR;
      if (thirdeyeConfig.getTime().getTimeFormat().startsWith(sdfPrefix)) {

        String pattern = thirdeyeConfig.getTime().getTimeFormat().split(ThirdEyeConstants.SDF_SEPARATOR)[1];
        DateTimeFormatter sdfFormatter = DateTimeFormat.forPattern(pattern);

        File localAvroFile = new File(dataPath, hdfsDataPath.getName());
        LongColumnPreIndexStatsCollector timeColumnStatisticsCollector =
            getTimeColumnStatsCollector(schema, localAvroFile);
        String startTime = timeColumnStatisticsCollector.getMinValue().toString();
        String endTime = timeColumnStatisticsCollector.getMaxValue().toString();
        startTime = String.valueOf(DateTime.parse(startTime, sdfFormatter).getMillis());
        endTime = String.valueOf(DateTime.parse(endTime, sdfFormatter).getMillis());

        // set start time
        segmentGeneratorConfig.getCustomProperties().put(SEGMENT_START_TIME, startTime);
        // set end time
        segmentGeneratorConfig.getCustomProperties().put(SEGMENT_END_TIME, endTime);
        // set time unit
        segmentGeneratorConfig.setSegmentTimeUnit(TimeUnit.MILLISECONDS);
      }

      // Generate segment
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(segmentGeneratorConfig);
      driver.build();

      // Tar the segment directory into file.
      String segmentName = null;
      File localDiskSegmentDirectoryFile = new File(localDiskSegmentDirectory);
      for (File file : localDiskSegmentDirectoryFile.listFiles()) {
        segmentName = file.getName();
        if (segmentName.startsWith(tableName)) {
          break;
        }
      }
      String localSegmentPath = new File(localDiskSegmentDirectory, segmentName).getAbsolutePath();

      String localTarPath = localDiskSegmentTarPath + "/" + segmentName + ".tar.gz";
      LOGGER.info("Trying to tar the segment to: {}", localTarPath);
      TarGzCompressionUtils.createTarGzOfDirectory(localSegmentPath, localTarPath);
      String hdfsTarPath = localHdfsSegmentTarPath + "/" + segmentName + ".tar.gz";

      LOGGER.info("*********************************************************************");
      LOGGER.info("Copy from : {} to {}", localTarPath, hdfsTarPath);
      LOGGER.info("*********************************************************************");
      fs.copyFromLocalFile(true, true, new Path(localTarPath), new Path(hdfsTarPath));
      return segmentName;
    }

    private LongColumnPreIndexStatsCollector getTimeColumnStatsCollector(Schema schema, File localAvroFile)
        throws FileNotFoundException, IOException {
      String timeColumnName = schema.getTimeColumnName();
      FieldSpec spec =  schema.getTimeFieldSpec();
      LOGGER.info("Spec for " + timeColumnName + " is " + spec);
      LongColumnPreIndexStatsCollector timeColumnStatisticsCollector = new LongColumnPreIndexStatsCollector(spec.getName(), new StatsCollectorConfig(schema, null));
      LOGGER.info("StatsCollector :" + timeColumnStatisticsCollector);
      DataFileStream<GenericRecord> dataStream =
          new DataFileStream<GenericRecord>(new FileInputStream(localAvroFile), new GenericDatumReader<GenericRecord>());
      while (dataStream.hasNext()) {
        GenericRecord next = dataStream.next();
        timeColumnStatisticsCollector.collect(next.get(timeColumnName));
      }
      dataStream.close();
      timeColumnStatisticsCollector.seal();

      return timeColumnStatisticsCollector;
    }

  }
}
