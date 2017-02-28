/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.hadoop.job.mapper;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.data.TimeGranularitySpec;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.TarGzCompressionUtils;
import com.linkedin.pinot.core.data.readers.CSVRecordReaderConfig;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReaderConfig;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;


public class HadoopSegmentCreationMapReduceJob {

  public static class HadoopSegmentCreationMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static Logger LOGGER = LoggerFactory.getLogger(HadoopSegmentCreationMapper.class);
    private Configuration _properties;

    private String _inputFilePath;
    private String _outputPath;
    private String _tableName;
    private String _postfix;

    private Path _currentHdfsWorkDir;
    private String _currentDiskWorkDir;

    // Temporary HDFS path for local machine
    private String _localHdfsSegmentTarPath;

    private String _localDiskSegmentDirectory;
    private String _localDiskSegmentTarPath;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

      _currentHdfsWorkDir = FileOutputFormat.getWorkOutputPath(context);
      _currentDiskWorkDir = "pinot_hadoop_tmp";

      // Temporary HDFS path for local machine
      _localHdfsSegmentTarPath = _currentHdfsWorkDir + "/segmentTar";

      // Temporary DISK path for local machine
      _localDiskSegmentDirectory = _currentDiskWorkDir + "/segments/";
      _localDiskSegmentTarPath = _currentDiskWorkDir + "/segmentsTar/";
      new File(_localDiskSegmentTarPath).mkdirs();

      LOGGER.info("*********************************************************************");
      LOGGER.info("Configurations : {}", context.getConfiguration().toString());
      LOGGER.info("*********************************************************************");
      LOGGER.info("Current HDFS working dir : {}", _currentHdfsWorkDir);
      LOGGER.info("Current DISK working dir : {}", new File(_currentDiskWorkDir).getAbsolutePath());
      LOGGER.info("*********************************************************************");
      _properties = context.getConfiguration();

      _outputPath = _properties.get("path.to.output");
      _tableName = _properties.get("segment.table.name");
      _postfix = _properties.get("segment.name.postfix", null);
      if (_outputPath == null || _tableName == null) {
        throw new RuntimeException(
            "Missing configs: " +
                "\n\toutputPath: " +
                _properties.get("path.to.output") +
                "\n\ttableName: " +
                _properties.get("segment.table.name"));
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      FileUtils.deleteQuietly(new File(_currentDiskWorkDir));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      String line = value.toString();
      String[] lineSplits = line.split(" ");

      LOGGER.info("*********************************************************************");
      LOGGER.info("mapper input : {}", value);
      LOGGER.info("PATH_TO_OUTPUT : {}", _outputPath);
      LOGGER.info("TABLE_NAME : {}", _tableName);
      LOGGER.info("num lines : {}", lineSplits.length);

      for (String split : lineSplits) {
        LOGGER.info("Command line : {}", split);
      }
      LOGGER.info("*********************************************************************");

      if (lineSplits.length != 3) {
        throw new RuntimeException("Input to the mapper is malformed, please contact the pinot team");
      }
      _inputFilePath = lineSplits[1].trim();

      Schema schema = new Schema();
      try {
        schema = getSchema(context);
      } catch (Exception e) {
        LOGGER.info("Could not get schema");
      }

      LOGGER.info("*********************************************************************");
      LOGGER.info("input data file path : {}", _inputFilePath);
      LOGGER.info("local hdfs segment tar path: {}", _localHdfsSegmentTarPath);
      LOGGER.info("local disk segment path: {}", _localDiskSegmentDirectory);
      LOGGER.info("local disk segment tar path: {}", _localDiskSegmentTarPath);
      LOGGER.info("data schema: {}", _localDiskSegmentTarPath);
      LOGGER.info("*********************************************************************");

      try {
        createSegment(_inputFilePath, schema, Integer.parseInt(lineSplits[2]));
        LOGGER.info("finished segment creation job successfully");
      } catch (Exception e) {
        LOGGER.error("Got exceptions during creating segments!", e);
      }

      context.write(new LongWritable(Long.parseLong(lineSplits[2])),
          new Text(FileSystem.get(new Configuration()).listStatus(new Path(_localHdfsSegmentTarPath + "/"))[0].getPath().getName()));
      LOGGER.info("finished the job successfully");
    }

    private String createSegment(String dataFilePath, Schema schema, Integer seqId) throws Exception {
      final FileSystem fs = FileSystem.get(new Configuration());
      final Path hdfsDataPath = new Path(dataFilePath);
      final File dataPath = new File(_currentDiskWorkDir, "data");
      if (dataPath.exists()) {
        dataPath.delete();
      }
      dataPath.mkdir();
      final Path localAvroPath = new Path(dataPath + "/" + hdfsDataPath.getName());
      fs.copyToLocalFile(hdfsDataPath, localAvroPath);

      LOGGER.info("Data schema is : {}", schema);
      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
      segmentGeneratorConfig.setTableName(_tableName);

      segmentGeneratorConfig.setInputFilePath(new File(dataPath, hdfsDataPath.getName()).getAbsolutePath());

      FileFormat fileFormat = getFileFormat(dataFilePath);
      segmentGeneratorConfig.setFormat(fileFormat);

      if (null != _postfix) {
        segmentGeneratorConfig.setSegmentNamePostfix(String.format("%s-%s", _postfix, seqId));
      } else {
        segmentGeneratorConfig.setSequenceId(seqId);
      }
      segmentGeneratorConfig.setReaderConfig(getReaderConfig(fileFormat));

      segmentGeneratorConfig.setOutDir(_localDiskSegmentDirectory);

      // Add the current java package version to the segment metadata
      // properties file.
      Package objPackage = this.getClass().getPackage();
      if (null != objPackage) {
        String packageVersion = objPackage.getSpecificationVersion();
        if (null != packageVersion) {
          LOGGER.info("Pinot Hadoop Package version {}", packageVersion);
          segmentGeneratorConfig.setCreatorVersion(packageVersion);
        }
      }

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(segmentGeneratorConfig);
      driver.build();
      // Tar the segment directory into file.
      String segmentName = (new File(_localDiskSegmentDirectory).listFiles()[0]).getName();
      String localSegmentPath = new File(_localDiskSegmentDirectory, segmentName).getAbsolutePath();

      String localTarPath = _localDiskSegmentTarPath + "/" + segmentName + ".tar.gz";
      LOGGER.info("Trying to tar the segment to: {}", localTarPath);
      TarGzCompressionUtils.createTarGzOfDirectory(localSegmentPath, localTarPath);
      String hdfsTarPath = _localHdfsSegmentTarPath + "/" + segmentName + ".tar.gz";

      LOGGER.info("*********************************************************************");
      LOGGER.info("Copy from : {} to {}", localTarPath, hdfsTarPath);
      LOGGER.info("*********************************************************************");
      fs.copyFromLocalFile(true, true, new Path(localTarPath), new Path(hdfsTarPath));
      return segmentName;
    }

    private RecordReaderConfig getReaderConfig(FileFormat fileFormat) {
      RecordReaderConfig readerConfig = null;
      switch (fileFormat) {
        case CSV:
          readerConfig = new CSVRecordReaderConfig();
          break;
        case AVRO:
          break;
        case JSON:
          break;
        default:
          break;
      }
      return readerConfig;
    }

    protected Schema getSchema(Context context) throws Exception {
      return Schema.fromString(context.getConfiguration().get("data.schema"));
    }

    protected Schema deriveAvroSchema(String avroFilePath, String currentDiskWorkDir, String timeColumnName,
        String timeType, List<String> dimensionList, List<String> metricsList) throws Exception {

      final FileSystem fs = FileSystem.get(new Configuration());
      final Path hdfsAvroPath = new Path(avroFilePath);
      final File avroPath = new File(currentDiskWorkDir, "avro");

      if (!avroPath.exists()) {
        avroPath.mkdir();
      }
      if (!(new File(avroPath, hdfsAvroPath.getName()).exists())) {
        final Path localAvroPath = new Path(avroPath + "/" + hdfsAvroPath.getName());
        LOGGER.info("Copy from " + hdfsAvroPath + " to " + localAvroPath);
        fs.copyToLocalFile(hdfsAvroPath, localAvroPath);
      }
      DataFileStream<GenericRecord> avroStream =
          new DataFileStream<GenericRecord>(new FileInputStream(new File(avroPath, hdfsAvroPath.getName())), new GenericDatumReader<GenericRecord>());

      org.apache.avro.Schema avroSchema = avroStream.getSchema();

      avroStream.close();
      final List<org.apache.avro.Schema.Field> inputFieldList = avroSchema.getFields();
      final Set<String> inputFieldNames = new HashSet<String>(inputFieldList.size());
      for (org.apache.avro.Schema.Field inputField : inputFieldList) {
        inputFieldNames.add(inputField.name());
      }

      if (dimensionList.isEmpty() && metricsList.isEmpty()) {
          dimensionList.addAll(inputFieldNames);
          LOGGER.info("Dimension list not found: Added all avro fields as dimemsions:" + dimensionList);
      } else {
        // Metrics and/or dimensions are specified.
        Set<String> specifiedFieldNames = new HashSet<String>(dimensionList.size() + metricsList.size());
        for (String fieldName : dimensionList) {
          specifiedFieldNames.add(fieldName);
        }
        for (String fieldName : metricsList) {
          specifiedFieldNames.add(fieldName);
        }
        if (timeColumnName != null) {
          specifiedFieldNames.add(timeColumnName);
        }
        if (!specifiedFieldNames.equals(inputFieldNames)) {
          for (String fieldName : inputFieldNames) {
            if (!specifiedFieldNames.contains(fieldName)) {
              String msg = "Field " + fieldName + " in Avro, but not specified. V1 segments will differ from V2 segments";
              // If you see this exception, then add this field to one of metric or dimension list and rerun the job.
              throw new RuntimeException(msg);
            }
          }
        }
      }

      Schema schema = new Schema();
      for (String dimension : dimensionList) {
        try {
          if (dimension.equals(timeColumnName)) {
            // We will add the time column below after adding all the other dimensions and metrics
            continue;
          }
          org.apache.avro.Schema.Field field = avroSchema.getField(dimension);

          if (field == null) {
            LOGGER
                .error("Cannot find dimension field " + dimension + " in avro schema, does it exist in the source data?");
            throw new RuntimeException("Unknown Avro field " + dimension);
          }

          FieldSpec.DataType pinotDataType = AvroRecordReader.getColumnType(field);
          FieldSpec spec = new DimensionFieldSpec();
          spec.setName(dimension);
          spec.setDataType(pinotDataType);

          org.apache.avro.Schema fieldSchema = field.schema();
          if (fieldSchema.getType() == org.apache.avro.Schema.Type.UNION) {
            if ((fieldSchema).getType() == org.apache.avro.Schema.Type.UNION) {
              fieldSchema = ((org.apache.avro.Schema) CollectionUtils.find(fieldSchema.getTypes(), new Predicate() {
                @Override
                public boolean evaluate(Object object) {
                  return ((org.apache.avro.Schema) object).getType() != org.apache.avro.Schema.Type.NULL;
                }
              }));
            }
          }
          spec.setSingleValueField(!(fieldSchema.getType() == org.apache.avro.Schema.Type.ARRAY));
          schema.addField(spec);
        } catch (Exception e) {
          String errMsg = "Unable to add field '" + dimension + "' as a dimension field";
          LOGGER.error(errMsg);
          throw new RuntimeException(errMsg, e);
        }
      }

      for (String metric : metricsList) {
        try {
          FieldSpec spec = new MetricFieldSpec();
          org.apache.avro.Schema.Field field = avroSchema.getField(metric);

          if (field == null) {
            LOGGER.error("Cannot find metric field " + metric + " in avro schema, does it exist in the source data?");
            throw new RuntimeException("Unknown Avro field " + metric);
          }
          spec.setDataType(AvroRecordReader.getColumnType(field));
          spec.setName(metric);
          spec.setSingleValueField(true);
          schema.addField(spec);
        } catch (Exception e) {
          String errMsg = "Unable to add field '" + metric + "' as a metric field";
          LOGGER.error(errMsg);
          throw new RuntimeException(errMsg, e);
        }
      }

        TimeGranularitySpec incoming =
            new TimeGranularitySpec(AvroRecordReader.getColumnType(avroSchema.getField(timeColumnName)), TimeUnit
                .valueOf(timeType.toUpperCase()), timeColumnName);
        TimeGranularitySpec outgoing =
            new TimeGranularitySpec(AvroRecordReader.getColumnType(avroSchema.getField(timeColumnName)), TimeUnit.valueOf(timeType.toUpperCase()), timeColumnName);
        schema.addField(new TimeFieldSpec(incoming, outgoing));
      return schema;
    }

    private FileFormat getFileFormat(String dataFilePath) {
      if (dataFilePath.endsWith(".json")) {
        return FileFormat.JSON;
      }
      if (dataFilePath.endsWith(".csv")) {
        return FileFormat.CSV;
      }
      if (dataFilePath.endsWith(".avro")) {
        return FileFormat.AVRO;
      }
      throw new RuntimeException("Not support file format - " + dataFilePath);
    }
  }
}
