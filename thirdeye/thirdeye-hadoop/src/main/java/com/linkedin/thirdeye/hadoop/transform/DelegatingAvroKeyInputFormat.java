package com.linkedin.thirdeye.hadoop.transform;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyRecordReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DelegatingAvroKeyInputFormat<T> extends AvroKeyInputFormat<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DelegatingAvroKeyInputFormat.class);
  private static TypeReference MAP_STRING_STRING_TYPE = new TypeReference<Map<String, String>>() {
  };

  public org.apache.hadoop.mapreduce.RecordReader<org.apache.avro.mapred.AvroKey<T>, NullWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    LOGGER.info("DelegatingAvroKeyInputFormat.createRecordReader()  for split:{}", split);
    FileSplit fileSplit = (FileSplit) split;
    Configuration configuration = context.getConfiguration();
    String sourceName = getSourceNameFromPath(fileSplit, configuration);
    LOGGER.info("Source Name for path {} : {}", fileSplit.getPath(), sourceName);
    Map<String, String> schemaJSONMapping = new ObjectMapper()
        .readValue(configuration.get("schema.json.mapping"), MAP_STRING_STRING_TYPE);

    LOGGER.info("Schema JSON Mapping: {}", schemaJSONMapping);

    String sourceSchemaJSON = schemaJSONMapping.get(sourceName);

    Schema schema = new Schema.Parser().parse(sourceSchemaJSON);
    return new AvroKeyRecordReader<T>(schema);
  }

  public static String getSourceNameFromPath(FileSplit fileSplit, Configuration configuration)
      throws IOException, JsonParseException, JsonMappingException {
    String content = configuration.get("schema.path.mapping");
    Map<String, String> schemaPathMapping =
        new ObjectMapper().readValue(content, MAP_STRING_STRING_TYPE);
    LOGGER.info("Schema Path Mapping: {}", schemaPathMapping);

    String sourceName = null;
    for (String path : schemaPathMapping.keySet()) {
      if (fileSplit.getPath().toString().indexOf(path) > -1) {
        sourceName = schemaPathMapping.get(path);
        break;
      }
    }
    return sourceName;
  };
}
