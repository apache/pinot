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

package org.apache.pinot.segment.local.segment.index.forward;

import java.io.File;
import java.nio.file.Files;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/**
 * Tests {@link ForwardIndexCreatorFactory}'s forward-index encoding branch selection. Each test uses an isolated
 * temporary index directory and does not share mutable state.
 */
public class ForwardIndexCreatorFactoryTest {
  private static final String COLUMN_NAME = "testCol";

  @Test
  public void testRawEncodingBuildsRawForwardIndexEvenWithDictionary()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try (ForwardIndexCreator creator = ForwardIndexCreatorFactory.createIndexCreator(newContext(indexDir, true),
        new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW).build())) {
      assertFalse(creator.isDictionaryEncoded());
      creator.putInt(1);
      creator.seal();
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  @Test
  public void testDefaultEncodingBuildsDictionaryForwardIndex()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try (ForwardIndexCreator creator =
        ForwardIndexCreatorFactory.createIndexCreator(newContext(indexDir, true),
            ForwardIndexConfig.getDefault(FieldConfig.EncodingType.DICTIONARY))) {
      assertTrue(creator.isDictionaryEncoded());
      creator.putDictId(0);
      creator.seal();
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /// Regression: ZSTD with a non-default level (e.g. `ZSTD(5)`) needs the V7 codec-pipeline writer,
  /// which only supports SV INT/LONG. Configuring it on an MV column must throw with a clear message.
  @Test
  public void testCodecSpecZstdNonDefaultLevelOnMvColumnThrows()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      assertThrows(IllegalArgumentException.class, () ->
          ForwardIndexCreatorFactory.createIndexCreator(newMvIntContext(indexDir),
              new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
                  .withCodecSpec("ZSTD(5)")
                  .build()));
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /// Regression: a transform codecSpec on a non-INT/LONG column must throw with a clear message
  /// pointing at the V7 supported-type restriction.
  @Test
  public void testCodecSpecTransformOnStringColumnThrows()
      throws Exception {
    File indexDir = Files.createTempDirectory("ForwardIndexCreatorFactoryTest").toFile();
    try {
      assertThrows(IllegalArgumentException.class, () ->
          ForwardIndexCreatorFactory.createIndexCreator(newStringContext(indexDir),
              new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
                  .withCodecSpec("DELTA")
                  .build()));
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  private static IndexCreationContext newMvIntContext(File indexDir) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, DataType.INT, false); // multi-value
    ColumnMetadata metadata = Mockito.mock(ColumnMetadata.class);
    Mockito.when(metadata.getFieldSpec()).thenReturn(fieldSpec);
    Mockito.when(metadata.getCardinality()).thenReturn(2);
    Mockito.when(metadata.getTotalDocs()).thenReturn(1);
    Mockito.when(metadata.hasDictionary()).thenReturn(false);
    Mockito.when(metadata.isFixedLength()).thenReturn(true);
    Mockito.when(metadata.getMaxNumberOfMultiValues()).thenReturn(1);
    return new IndexCreationContext.Builder(indexDir, tableConfig, metadata).build();
  }

  private static IndexCreationContext newStringContext(File indexDir) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, DataType.STRING, true);
    ColumnMetadata metadata = Mockito.mock(ColumnMetadata.class);
    Mockito.when(metadata.getFieldSpec()).thenReturn(fieldSpec);
    Mockito.when(metadata.getCardinality()).thenReturn(2);
    Mockito.when(metadata.getTotalDocs()).thenReturn(1);
    Mockito.when(metadata.hasDictionary()).thenReturn(false);
    Mockito.when(metadata.getLengthOfShortestElement()).thenReturn(1);
    Mockito.when(metadata.getLengthOfLongestElement()).thenReturn(8);
    Mockito.when(metadata.isFixedLength()).thenReturn(false);
    return new IndexCreationContext.Builder(indexDir, tableConfig, metadata).build();
  }

  private static IndexCreationContext newContext(File indexDir, boolean hasDictionary) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, DataType.INT, true);
    ColumnMetadata metadata = Mockito.mock(ColumnMetadata.class);
    Mockito.when(metadata.getFieldSpec()).thenReturn(fieldSpec);
    Mockito.when(metadata.getCardinality()).thenReturn(2);
    Mockito.when(metadata.getTotalDocs()).thenReturn(1);
    Mockito.when(metadata.hasDictionary()).thenReturn(hasDictionary);
    Mockito.when(metadata.getLengthOfShortestElement()).thenReturn(Integer.BYTES);
    Mockito.when(metadata.getLengthOfLongestElement()).thenReturn(Integer.BYTES);
    Mockito.when(metadata.isFixedLength()).thenReturn(true);
    return new IndexCreationContext.Builder(indexDir, tableConfig, metadata).build();
  }
}
