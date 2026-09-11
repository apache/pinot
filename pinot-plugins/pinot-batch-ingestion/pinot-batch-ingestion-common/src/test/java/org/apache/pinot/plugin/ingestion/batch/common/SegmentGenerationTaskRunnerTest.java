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
package org.apache.pinot.plugin.ingestion.batch.common;

import java.io.File;
import java.io.ObjectStreamClass;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.ingestion.IngestionGroovyPolicy;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.RecordReaderSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationTaskSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class SegmentGenerationTaskRunnerTest {

  @Test
  public void testGroovyPolicySerializationCompatibility()
      throws ReflectiveOperationException {
    assertEquals(ObjectStreamClass.lookup(SegmentGenerationTaskRunner.class).getSerialVersionUID(),
        -1755560917714370885L);

    SegmentGenerationTaskRunner taskRunner =
        new SegmentGenerationTaskRunner(new SegmentGenerationTaskSpec(), IngestionGroovyPolicy.ENABLED);
    assertFalse(taskRunner.isIngestionGroovyDisabled());

    // A runner serialized before the policy field was introduced deserializes with a null value and must fail closed.
    Field ingestionGroovyPolicyField = SegmentGenerationTaskRunner.class.getDeclaredField("_ingestionGroovyPolicy");
    ingestionGroovyPolicyField.setAccessible(true);
    ingestionGroovyPolicyField.set(taskRunner, null);
    assertTrue(taskRunner.isIngestionGroovyDisabled());
  }

  @Test
  public void testRuntimeGroovyPolicy()
      throws Exception {
    File testDir = Files.createTempDirectory("segment-generation-groovy-policy").toFile();
    try {
      SegmentGenerationTaskSpec disabledTaskSpec = createTaskSpec(testDir);
      IllegalStateException error = expectThrows(IllegalStateException.class,
          () -> new SegmentGenerationTaskRunner(disabledTaskSpec, IngestionGroovyPolicy.DISABLED).run());
      assertTrue(error.getMessage().contains("controller.disable.ingestion.groovy=false"));

      SegmentGenerationTaskSpec enabledTaskSpec = createTaskSpec(testDir);
      String segmentName = new SegmentGenerationTaskRunner(enabledTaskSpec, IngestionGroovyPolicy.ENABLED).run();
      try (PinotSegmentRecordReader recordReader =
          new PinotSegmentRecordReader(new File(enabledTaskSpec.getOutputDirectoryPath(), segmentName))) {
        assertEquals(recordReader.next().getValue("derived"), "cba");
      }
    } finally {
      FileUtils.deleteQuietly(testDir);
    }
  }

  private static SegmentGenerationTaskSpec createTaskSpec(File testDir)
      throws Exception {
    File inputFile = new File(testDir, "input");
    FileUtils.touch(inputFile);
    File outputDir = new File(testDir, "output");
    FileUtils.forceMkdir(outputDir);

    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("source", FieldSpec.DataType.STRING)
        .addSingleValueDimension("derived", FieldSpec.DataType.STRING).build();
    schema.getFieldSpecFor("derived").setTransformFunction("Groovy({source.reverse()}, source)");

    RecordReaderSpec recordReaderSpec = new RecordReaderSpec();
    recordReaderSpec.setClassName(OneRowRecordReader.class.getName());
    SegmentNameGeneratorSpec segmentNameGeneratorSpec = new SegmentNameGeneratorSpec();
    segmentNameGeneratorSpec.setType(BatchConfigProperties.SegmentNameGeneratorType.FIXED);
    segmentNameGeneratorSpec.addConfig(SegmentGenerationTaskRunner.SEGMENT_NAME, "testSegment");

    SegmentGenerationTaskSpec taskSpec = new SegmentGenerationTaskSpec();
    taskSpec.setTableConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build());
    taskSpec.setSchema(schema);
    taskSpec.setRecordReaderSpec(recordReaderSpec);
    taskSpec.setSegmentNameGeneratorSpec(segmentNameGeneratorSpec);
    taskSpec.setInputFilePath(inputFile.getAbsolutePath());
    taskSpec.setOutputDirectoryPath(outputDir.getAbsolutePath());
    return taskSpec;
  }

  public static final class OneRowRecordReader implements RecordReader {
    private boolean _read;

    @Override
    public void init(File dataFile, Set<String> fieldsToRead, @Nullable RecordReaderConfig recordReaderConfig) {
    }

    @Override
    public boolean hasNext() {
      return !_read;
    }

    @Override
    public GenericRow next(GenericRow reuse) {
      _read = true;
      reuse.putValue("source", "abc");
      return reuse;
    }

    @Override
    public void rewind() {
      _read = false;
    }

    @Override
    public void close() {
    }
  }
}
