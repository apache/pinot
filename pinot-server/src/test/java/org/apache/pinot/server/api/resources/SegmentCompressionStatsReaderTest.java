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
package org.apache.pinot.server.api.resources;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.ws.rs.WebApplicationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.restlet.resources.SegmentCompressionStatsContribution;
import org.apache.pinot.common.restlet.resources.ServerCompressionStatsResponse;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Tests server-side compression metadata coverage semantics.
public class SegmentCompressionStatsReaderTest {
  @Test
  public void testEmptySegmentHasCompleteZeroByteStats() {
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getName()).thenReturn("empty");
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(new TreeMap<>());

    assertCompleteZeroByteStats(SegmentCompressionStatsReader.read(segmentMetadata, true));
  }

  @Test
  public void testSegmentWithAllForwardIndexesDisabledHasCompleteZeroByteStats() {
    ColumnMetadata columnMetadata = mock(ColumnMetadata.class);
    when(columnMetadata.getIndexSizeFor(StandardIndexes.forward())).thenReturn(-1L);
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getName()).thenReturn("forwardDisabled");
    TreeMap<String, ColumnMetadata> columnMetadataMap = new TreeMap<>();
    columnMetadataMap.put("column", columnMetadata);
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(columnMetadataMap);

    assertCompleteZeroByteStats(SegmentCompressionStatsReader.read(segmentMetadata, false));
  }

  @Test
  public void testV1SegmentUsesSidecarIndexFileSizes()
      throws Exception {
    File outputDir = Files.createTempDirectory("SegmentCompressionStatsReaderTest").toFile();
    String segmentName = "trackedV1";
    try {
      Schema schema = new Schema.SchemaBuilder().setSchemaName("trackedV1")
          .addSingleValueDimension("value", DataType.STRING)
          .build();
      TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("trackedV1")
          .setNoDictionaryColumns(List.of("value"))
          .setFieldConfigList(List.of(new FieldConfig("value", FieldConfig.EncodingType.RAW, List.of(),
              FieldConfig.CompressionCodec.LZ4, null)))
          .setCompressionStatsEnabled(true)
          .build();
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
      config.setOutDir(outputDir.getAbsolutePath());
      config.setSegmentName(segmentName);
      config.setSegmentVersion(SegmentVersion.v1);
      List<GenericRow> rows = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        GenericRow row = new GenericRow();
        row.putValue("value", "value-" + i);
        rows.add(row);
      }
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, new GenericRowRecordReader(rows));
      driver.build();

      SegmentMetadata metadata = new SegmentMetadataImpl(new File(outputDir, segmentName));
      SegmentCompressionStatsContribution contribution = SegmentCompressionStatsReader.read(metadata, true);
      assertTrue(contribution.isComplete());
      assertTrue(contribution.getUncompressedValueSizeInBytes() > 0);
      assertTrue(contribution.getForwardIndexAndDictionaryStorageSizeInBytes() > 0);
      assertEquals(contribution.getColumnCompressionStats().get("value").getEncodingBreakdown().get(0)
          .getChunkCompressionType(), ChunkCompressionType.LZ4);
    } finally {
      FileUtils.deleteQuietly(outputDir);
    }
  }

  @Test
  public void testServerRejectsOversizedColumnContributionResponse() {
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getColumnMetadataMap()).thenReturn(new TreeMap<>(Map.of(
        "column", mock(ColumnMetadata.class))));
    ImmutableSegment segment = mock(ImmutableSegment.class);
    when(segment.getSegmentMetadata()).thenReturn(segmentMetadata);

    WebApplicationException exception = Assert.expectThrows(WebApplicationException.class,
        () -> TablesResource.addColumnContributions(
            ServerCompressionStatsResponse.MAX_COLUMN_CONTRIBUTIONS_PER_RESPONSE, segment, null));
    assertEquals(exception.getResponse().getStatus(), 413);
  }

  private static void assertCompleteZeroByteStats(SegmentCompressionStatsContribution contribution) {
    assertTrue(contribution.isComplete());
    assertEquals(contribution.getUncompressedValueSizeInBytes(), 0L);
    assertEquals(contribution.getForwardIndexAndDictionaryStorageSizeInBytes(), 0L);
    assertNull(contribution.getColumnCompressionStats());
  }
}
