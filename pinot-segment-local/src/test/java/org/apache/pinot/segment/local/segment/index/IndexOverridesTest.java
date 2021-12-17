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
package org.apache.pinot.segment.local.segment.index;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.UUID;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.index.IndexingOverrides;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.apache.commons.io.FileUtils.deleteQuietly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class IndexOverridesTest {

  private File _file;

  @BeforeTest
  public void before()
      throws IOException {
    _file = Files.createTempFile("IndexOverridesTest", UUID.randomUUID().toString()).toFile();
  }

  @AfterTest
  public void cleanup() {
    deleteQuietly(_file);
  }

  @Test
  public void testOverrideInvertedIndexCreation()
      throws IOException {
    DictionaryBasedInvertedIndexCreator highCardinalityInvertedIndex = mock(DictionaryBasedInvertedIndexCreator.class);
    InvertedIndexReader<?> highCardinalityInvertedIndexReader = mock(InvertedIndexReader.class);
    IndexCreatorProvider provider = new IndexingOverrides.Default() {
      @Override
      public DictionaryBasedInvertedIndexCreator newInvertedIndexCreator(IndexCreationContext.Inverted context)
          throws IOException {
        if (context.getCardinality() >= 10000) {
          return highCardinalityInvertedIndex;
        }
        return super.newInvertedIndexCreator(context);
      }

      @Override
      public InvertedIndexReader<?> newInvertedIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
          throws IOException {
        if (metadata.getCardinality() >= 10000) {
          return highCardinalityInvertedIndexReader;
        }
        return super.newInvertedIndexReader(dataBuffer, metadata);
      }
    };
    MockedStatic<IndexingOverrides> overrides = mockStatic(IndexingOverrides.class);
    overrides.when(IndexingOverrides::getIndexCreatorProvider).thenReturn(provider);
    IndexCreationContext.Inverted highCardinalityContext = newContext(Integer.MAX_VALUE);
    assertEquals(IndexingOverrides.getIndexCreatorProvider().newInvertedIndexCreator(highCardinalityContext),
        highCardinalityInvertedIndex);
    IndexCreationContext.Inverted lowCardinalityContext = newContext(1);
    assertTrue(IndexingOverrides.getIndexCreatorProvider()
        .newInvertedIndexCreator(lowCardinalityContext) instanceof OffHeapBitmapInvertedIndexCreator);
    overrides.when(IndexingOverrides::getIndexReaderProvider).thenReturn(provider);
    ColumnMetadata highCardinalityMetadata = mock(ColumnMetadata.class);
    when(highCardinalityMetadata.getCardinality()).thenReturn(100_000);
    assertEquals(IndexingOverrides.getIndexReaderProvider()
            .newInvertedIndexReader(mock(PinotDataBuffer.class), highCardinalityMetadata),
        highCardinalityInvertedIndexReader);
  }

  private IndexCreationContext.Inverted newContext(int cardinality) {
    FieldSpec fieldSpec = new DimensionFieldSpec("test", FieldSpec.DataType.INT, true);
    return IndexCreationContext.builder().withIndexDir(_file)
        .withColumnMetadata(ColumnMetadataImpl.builder().setFieldSpec(fieldSpec).setCardinality(cardinality).build())
        .build().forInvertedIndex();
  }
}
