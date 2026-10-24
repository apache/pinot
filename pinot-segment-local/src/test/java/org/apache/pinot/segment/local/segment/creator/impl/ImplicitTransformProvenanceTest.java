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
package org.apache.pinot.segment.local.segment.creator.impl;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.recordtransformer.TransformProvenanceUtils;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


/// Verifies transform provenance written by the offline segment-creation pipeline. Each test owns its temporary
/// directory and the class has no shared mutable state, so instances are safe to run in parallel.
public class ImplicitTransformProvenanceTest {
  private static final String MAP_KEYS = "attributes__KEYS";
  private static final String TIME_OUTPUT = "eventDays";
  private static final String MAP_DEPENDENT = "keyCount";
  private static final String TIME_DEPENDENT = "dayPlusOne";
  private static final String INDEPENDENT = "independent";
  private static final String MAP_DEPENDENT_TRANSFORM = "Groovy({attributes__KEYS.size()}, attributes__KEYS)";
  private static final String TIME_DEPENDENT_TRANSFORM = "plus(eventDays, 1)";
  private static final String INDEPENDENT_TRANSFORM = "plus(source, 1)";

  // Exercises the deprecated TimeFieldSpec conversion path that still runs for backward-compatible schemas.
  @SuppressWarnings("deprecation")
  @Test
  public void testImplicitTransformsAndDependentsDoNotClaimDependencyClosedProvenance()
      throws Exception {
    File outputDir = Files.createTempDirectory("implicit-transform-provenance").toFile();
    try {
      Schema schema = new Schema.SchemaBuilder()
          .setSchemaName("implicitTransformProvenance")
          .addSingleValueDimension("source", DataType.INT)
          .addMultiValueDimension(MAP_KEYS, DataType.STRING)
          .addSingleValueDimension(MAP_DEPENDENT, DataType.INT)
          .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "incomingHours"),
              new TimeGranularitySpec(DataType.LONG, TimeUnit.DAYS, TIME_OUTPUT))
          .addSingleValueDimension(TIME_DEPENDENT, DataType.LONG)
          .addSingleValueDimension(INDEPENDENT, DataType.INT)
          .build();
      IngestionConfig ingestionConfig = new IngestionConfig();
      ingestionConfig.setTransformConfigs(List.of(
          new TransformConfig(MAP_DEPENDENT, MAP_DEPENDENT_TRANSFORM),
          new TransformConfig(TIME_DEPENDENT, TIME_DEPENDENT_TRANSFORM),
          new TransformConfig(INDEPENDENT, INDEPENDENT_TRANSFORM)));
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
          .setTableName("implicitTransformProvenance")
          .setTimeColumnName(TIME_OUTPUT)
          .setIngestionConfig(ingestionConfig)
          .build();
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
      config.setOutDir(outputDir.getAbsolutePath());
      config.setSegmentName("implicitTransformProvenanceSegment");

      GenericRow row = new GenericRow();
      row.putValue("source", 1);
      row.putValue("attributes", Map.of("first", "one", "second", "two"));
      row.putValue("incomingHours", 440_496L);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, new GenericRowRecordReader(List.of(row)));
      driver.build();

      SegmentMetadataImpl metadata = new SegmentMetadataImpl(driver.getOutputDirectory());
      assertUnknownProvenance(metadata.getColumnMetadataFor(MAP_KEYS));
      assertUnknownProvenance(metadata.getColumnMetadataFor(TIME_OUTPUT));
      assertDirectOnlyProvenance(metadata.getColumnMetadataFor(MAP_DEPENDENT), MAP_DEPENDENT_TRANSFORM);
      assertDirectOnlyProvenance(metadata.getColumnMetadataFor(TIME_DEPENDENT), TIME_DEPENDENT_TRANSFORM);

      ColumnMetadata independent = metadata.getColumnMetadataFor(INDEPENDENT);
      assertEquals(independent.getTransformFunction(), INDEPENDENT_TRANSFORM);
      assertEquals(independent.getTransformFunctionProvenanceVersion(), TransformProvenanceUtils.CURRENT_VERSION);
      assertNotNull(independent.getTransformFunctionFingerprint());
    } finally {
      FileUtils.deleteDirectory(outputDir);
    }
  }

  private static void assertUnknownProvenance(ColumnMetadata columnMetadata) {
    assertNotNull(columnMetadata);
    assertNull(columnMetadata.getTransformFunction());
    assertEquals(columnMetadata.getTransformFunctionProvenanceVersion(), ColumnMetadata.UNAVAILABLE);
    assertNull(columnMetadata.getTransformFunctionFingerprint());
  }

  private static void assertDirectOnlyProvenance(ColumnMetadata columnMetadata, String transformFunction) {
    assertNotNull(columnMetadata);
    assertEquals(columnMetadata.getTransformFunction(), transformFunction);
    assertEquals(columnMetadata.getTransformFunctionProvenanceVersion(), 1);
    assertNull(columnMetadata.getTransformFunctionFingerprint());
  }
}
