/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.thirdeye.hadoop.backfill;

import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.thirdeye.hadoop.config.ThirdEyeConstants;
import com.linkedin.thirdeye.hadoop.util.ThirdeyeAvroUtils;
import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.thirdeye.hadoop.backfill.BackfillPhaseConstants.*;

/**
 * Mapper class for Backfill job, which converts a pinot segment to avro files
 */
public class BackfillPhaseMapJob {

  public static class BackfillMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static Logger LOGGER = LoggerFactory.getLogger(BackfillPhaseMapJob.class);

    private Configuration properties;

    private String inputPath;
    private String outputPath;
    private String currentDiskWorkDir;
    private FileSystem fs;


    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      currentDiskWorkDir = "pinot_hadoop_tmp";
      new File(currentDiskWorkDir).mkdir();

      LOGGER.info("*********************************************************************");
      LOGGER.info("Configurations : {}", context.getConfiguration().toString());
      LOGGER.info("Current DISK working dir : {}", new File(currentDiskWorkDir).getAbsolutePath());
      LOGGER.info("*********************************************************************");

      properties = context.getConfiguration();
      fs = FileSystem.get(new Configuration());

      outputPath = properties.get(BACKFILL_PHASE_OUTPUT_PATH.toString());
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      String line = value.toString();
      String[] lineSplits = line.split(" ");

      LOGGER.info("*********************************************************************");
      LOGGER.info("mapper input : {}", value);
      LOGGER.info("Path to output : {}", outputPath);
      LOGGER.info("num lines : {}", lineSplits.length);

      for (String split : lineSplits) {
        LOGGER.info("Command line : {}", split);
      }
      if (lineSplits.length != 3) {
        throw new RuntimeException("Input to the mapper is malformed");
      }
      inputPath = lineSplits[1].trim();

      LOGGER.info("input data file path : {}", inputPath);
      LOGGER.info("*********************************************************************");

      try {
        createAvro(inputPath);
        LOGGER.info("Finished avro creation job successfully");
      } catch (Exception e) {
        LOGGER.error("Got exceptions during creating avro!", e);
      }
      LOGGER.info("Finished the job successfully!");
    }

    private void createAvro(String dataFilePath) throws Exception {

      Path hdfsDataPath = new Path(dataFilePath);
      File dataPath = new File(currentDiskWorkDir, "data");
      if (dataPath.exists()) {
        dataPath.delete();
      }
      dataPath.mkdir();
      LOGGER.info("Creating temporary data dir {}", dataPath);

      final File avroPath = new File(currentDiskWorkDir, "avro");
      if (avroPath.exists()) {
        avroPath.delete();
      }
      avroPath.mkdir();
      LOGGER.info("Creating temporary avro dir {}", avroPath);

      String segmentName = hdfsDataPath.getName();
      final Path localFilePath = new Path(dataPath + "/" + segmentName);
      fs.copyToLocalFile(hdfsDataPath, localFilePath);
      LOGGER.info("Copying segment {} from {} to local {}", segmentName, hdfsDataPath, localFilePath);
      File segmentIndexDir = new File(localFilePath.toString());
      if (!segmentIndexDir.exists()) {
        throw new IllegalStateException("Failed to copy " + hdfsDataPath + " to " + localFilePath);
      }

      LOGGER.info("Initializing PinotSegmentRecordReader with segment index dir {}", segmentIndexDir);
      PinotSegmentRecordReader pinotSegmentRecordReader = new PinotSegmentRecordReader(segmentIndexDir);
      LOGGER.info("Schema {}", pinotSegmentRecordReader.getSchema());

      Schema avroSchema = ThirdeyeAvroUtils.constructAvroSchemaFromPinotSchema(pinotSegmentRecordReader.getSchema());
      GenericDatumWriter<GenericRecord> datum = new GenericDatumWriter<GenericRecord>(avroSchema);
      DataFileWriter<GenericRecord> recordWriter = new DataFileWriter<GenericRecord>(datum);
      File localAvroFile = new File(avroPath, segmentName + ThirdEyeConstants.AVRO_SUFFIX);
      recordWriter.create(avroSchema, localAvroFile);

      LOGGER.info("Converting pinot segment to avro at {}", localAvroFile);
      while (pinotSegmentRecordReader.hasNext()) {
        GenericRecord outputRecord = new Record(avroSchema);
        GenericRow row = pinotSegmentRecordReader.next();
        for (String fieldName : row.getFieldNames()) {
          outputRecord.put(fieldName, row.getValue(fieldName));
        }
        recordWriter.append(outputRecord);
      }
      LOGGER.info("Writing to avro file at {}", localAvroFile);
      recordWriter.close();
      if (!localAvroFile.exists()) {
        LOGGER.info("Failed to write avro file to {}", localAvroFile);
      }
      pinotSegmentRecordReader.close();

      LOGGER.info("Coping avro file from {} to hdfs at {}", localAvroFile, outputPath);
      fs.copyFromLocalFile(true, true, new Path(localAvroFile.toString()), new Path(outputPath));
      if (!fs.exists(new Path(outputPath))) {
        throw new IllegalStateException("Failed to copy avro file to hdfs at " + outputPath );
      }
      LOGGER.info("Successfully copied {} to {}", localAvroFile, outputPath);
    }
  }
}
