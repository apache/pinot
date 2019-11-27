/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.druid.tools;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathParser;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.Parser;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.JsonUtils;


/**
 * The DruidToPinotSchemaConverter is a tool that takes a Druid segment and creates a Pinot schema with the
 * segment's information.
 */
public class DruidToPinotSchemaConverterCommand {
  // TODO: Change implementation to use the Command framework like the CreateSegmentCommand
  // TODO: Allow configuration for time stuff etc.
  // TODO: add "columnsToInclude" like in DumpSegment
  //  - for example if you don't want "count" in the schema, you can specify
  public static Schema createSchema(String schemaName, String druidSegmentPath) // ok should this take the path to the file or the actual file
      throws IOException {
    // TODO: Consider putting this druid setup in another file/class so you can keep reusing it as you do
    ColumnConfig config = new DruidProcessingConfig() {
      @Override
      public String getFormatString() {
        return "processing-%s";
      }

      @Override
      public int intermediateComputeSizeBytes() {
        return 100 * 1024 * 1024;
      }

      @Override
      public int getNumThreads() {
        return 1;
      }

      @Override
      public int columnCacheSizeBytes() {
        return 25 * 1024 * 1024;
      }
    };

    ObjectMapper mapper = new DefaultObjectMapper();
    final IndexIO indexIO = new IndexIO(mapper, config);
    File druidSegment = new File(druidSegmentPath);
    QueryableIndex index = indexIO.loadIndex(druidSegment);
    QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);

    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder(); // use addField() for every column
    schemaBuilder.setSchemaName(schemaName);

    List<String> columnNames = index.getColumnNames();
    Indexed<String> dimensions = adapter.getAvailableDimensions();
    Iterable<String> metrics = adapter.getAvailableMetrics();

    for (String dimension : dimensions) {
      ColumnCapabilities columnCapabilities = adapter.getColumnCapabilities(dimension);
      boolean isSingleValueField = !columnCapabilities.hasMultipleValues();
      try {
        FieldSpec.DataType type = getPinotDataType(columnCapabilities.getType());
        if (isSingleValueField) {
          schemaBuilder.addSingleValueDimension(dimension, type, null);
        } else {
          schemaBuilder.addMultiValueDimension(dimension, type, null);
        }

      } catch(UnsupportedOperationException e) {
        System.out.println(e.getMessage() + "; Skipping column " + dimension);
      }
    }

    for (String metric : metrics) {
      ColumnCapabilities columnCapabilities = adapter.getColumnCapabilities(metric);
      try {
        FieldSpec.DataType type = getPinotDataType(columnCapabilities.getType());
        schemaBuilder.addMetric(metric, type);
      } catch(UnsupportedOperationException e) {
        System.out.println(e.getMessage() + "; Skipping column " + metric);
      }
    }
    schemaBuilder.addTime(ColumnHolder.TIME_COLUMN_NAME, TimeUnit.MILLISECONDS, FieldSpec.DataType.LONG,null);
    return schemaBuilder.build();
  }

  public static Schema ingestionSpecToPinotSchema(String ingestionSpecPath)
      throws IOException {
    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder();
    File ingestionSpecFile = new File(ingestionSpecPath);

    JsonNode specJsonNode = JsonUtils.fileToJsonNode(ingestionSpecFile);
    JsonNode dataSchema = specJsonNode.get("dataSchema");
    JsonNode ioConfig = specJsonNode.get("ioConfig");
    JsonNode tuningConfig = specJsonNode.get("tuningConfig");

    return schemaBuilder.build();
  }

  public static void writeSchemaToFile(Schema schema, String outputDirectory)
      throws IOException {
    File outputFile = new File(outputDirectory + "/" + schema.getSchemaName() + ".json");
    String json = schema.toPrettyJsonString();
    try {
      outputFile.createNewFile();
      FileUtils.writeStringToFile(outputFile, json);
    } catch (IOException e) {
      System.out.println("Error writing to the json file: " + outputFile.getAbsolutePath());
      throw e;
    }
  }

  private static FieldSpec.DataType getPinotDataType(ValueType druidType) {
    switch (druidType) {
      case STRING:
        return FieldSpec.DataType.STRING;
      case LONG:
        return FieldSpec.DataType.LONG;
      case FLOAT:
        return FieldSpec.DataType.FLOAT;
      case DOUBLE:
        return FieldSpec.DataType.DOUBLE;
      case COMPLEX:
      default:
        throw new UnsupportedOperationException("Pinot does not support Druid ValueType " + druidType.name());
    }
  }

  public static void main(String[] args)
      throws IOException {
    args = new String[]{"1", "2", "3"}; // PLACEHOLDER TODO: PLS DELETE THIS
    if (args.length != 3) {
      System.out.println("Usage:");
      System.out.println("java -jar druid-to-pinot-schema-generator-jar-with-dependencies.jar <schema_name> <druid_segment_path> <output_directory>");
    } else {
      String pathToIngestionSpec = "/Users/dadapon/Desktop/test/all-types-test/all-types-data/all-types-ingestion-spec.json";

      //Schema schema = createSchema(args[0], args[1]);
      Schema schema = ingestionSpecToPinotSchema(pathToIngestionSpec);
      //writeSchemaToFile(schema, args[2]);
    }
  }
}
