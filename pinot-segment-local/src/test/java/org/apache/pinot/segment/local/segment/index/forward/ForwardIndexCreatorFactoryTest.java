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
import java.lang.reflect.Field;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.config.table.CompressionCodecSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class ForwardIndexCreatorFactoryTest {

  @DataProvider(name = "leveledCompressionSpecs")
  public Object[][] leveledCompressionSpecs() {
    return new Object[][] {
        {"zstd(3)", "ZstandardCompressor", 3},
        {"LZ4(12)", "LZ4WithLengthCompressor", 12},
    };
  }

  @Test(dataProvider = "leveledCompressionSpecs")
  public void testLeveledCompressionSpecIsPassedToChunkWriter(String rawSpec, String expectedCompressorName,
      int expectedLevel)
      throws Exception {
    File indexDir = new File(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
    FileUtils.forceMkdir(indexDir);
    try {
      IndexCreationContext context = mock(IndexCreationContext.class);
      FieldSpec fieldSpec = new DimensionFieldSpec("rawCol", FieldSpec.DataType.STRING, true);
      when(context.getIndexDir()).thenReturn(indexDir);
      when(context.getFieldSpec()).thenReturn(fieldSpec);
      when(context.getTotalDocs()).thenReturn(1);
      when(context.getLengthOfLongestEntry()).thenReturn(32);
      when(context.getMaxNumberOfMultiValueElements()).thenReturn(1);
      when(context.getMaxRowLengthInBytes()).thenReturn(32);
      when(context.hasDictionary()).thenReturn(false);
      when(context.getColumnStatistics()).thenReturn(null);

      ForwardIndexConfig forwardIndexConfig = new ForwardIndexConfig.Builder()
          .withCompressionCodecSpec(CompressionCodecSpec.fromString(rawSpec))
          .build();

      ForwardIndexCreator creator = ForwardIndexCreatorFactory.createIndexCreator(context, forwardIndexConfig);
      assertTrue(creator instanceof SingleValueVarByteRawIndexCreator);

      Object indexWriter = getField(creator, SingleValueVarByteRawIndexCreator.class, "_indexWriter");
      Object chunkCompressor = getField(indexWriter, VarByteChunkForwardIndexWriterV4.class, "_chunkCompressor");
      assertEquals(chunkCompressor.getClass().getSimpleName(), expectedCompressorName);
      assertCompressionLevelApplied(chunkCompressor, expectedLevel);
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  private static void assertCompressionLevelApplied(Object chunkCompressor, int expectedLevel)
      throws Exception {
    Class<?> compressorClass = chunkCompressor.getClass();
    try {
      assertEquals(getIntField(chunkCompressor, compressorClass, "_compressionLevel"), expectedLevel);
      return;
    } catch (NoSuchFieldException ignored) {
      // Some compressors, e.g. LZ4WithLengthCompressor, only expose the explicit-level path via a dedicated instance.
    }

    Field instanceField = compressorClass.getDeclaredField("INSTANCE");
    instanceField.setAccessible(true);
    Object defaultInstance = instanceField.get(null);
    assertTrue(chunkCompressor != defaultInstance,
        "Expected an explicit compression level to allocate a non-default compressor instance");
  }

  private static Object getField(Object target, Class<?> declaringClass, String fieldName)
      throws Exception {
    Field field = declaringClass.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.get(target);
  }

  private static int getIntField(Object target, Class<?> declaringClass, String fieldName)
      throws Exception {
    Field field = declaringClass.getDeclaredField(fieldName);
    field.setAccessible(true);
    return field.getInt(target);
  }
}
