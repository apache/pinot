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
package org.apache.pinot.segment.local.segment.creator;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.RecordProcessingException;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;


/// Tests that segment creation wraps record failures in [RecordProcessingException].
public class RecordProcessingExceptionTest {
  private static final File OUT_DIR = new File(FileUtils.getTempDirectory(), "RecordProcessingExceptionTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String COLUMN = "name";

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(OUT_DIR);
  }

  @Test
  public void testRecordThatCannotBeTransformedWhileGatheringStats()
      throws Exception {
    GenericRow validRow = new GenericRow();
    validRow.putValue(COLUMN, "kylo");
    GenericRow invalidRow = new GenericRow();
    invalidRow.putValue(COLUMN, new Object[]{"cooper", "max"});
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(getSegmentGeneratorConfig(), new GenericRowRecordReader(List.of(validRow, invalidRow)));

    RecordProcessingException e = expectThrows(RecordProcessingException.class, driver::build);

    assertEquals(e.getMessage(), "Caught exception while reading data");
    assertEquals(ExceptionUtils.getRootCause(e).getMessage(),
        "Cannot read single-value from Object[]: [cooper, max] for column: " + COLUMN);
  }

  @Test
  public void testRecordThatCannotBeReadWhileIndexing()
      throws Exception {
    GenericRow row = new GenericRow();
    row.putValue(COLUMN, "kylo");
    IOException readFailure = new IOException("Failed to read record");
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(getSegmentGeneratorConfig(), new FailsAfterFirstPassRecordReader(List.of(row), readFailure));

    RecordProcessingException e = expectThrows(RecordProcessingException.class, driver::build);

    assertEquals(e.getMessage(), "Error occurred while reading row during indexing");
    assertSame(e.getCause(), readFailure);
  }

  private static SegmentGeneratorConfig getSegmentGeneratorConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(COLUMN, DataType.STRING)
        .build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(OUT_DIR.getAbsolutePath());
    config.setSegmentName("testSegment");
    return config;
  }

  private static class FailsAfterFirstPassRecordReader implements RecordReader {
    private final GenericRowRecordReader _delegate;
    private final IOException _failure;
    private boolean _firstPassDone;

    FailsAfterFirstPassRecordReader(List<GenericRow> rows, IOException failure) {
      _delegate = new GenericRowRecordReader(rows);
      _failure = failure;
    }

    @Override
    public void init(File dataFile, @Nullable Set<String> fieldsToRead,
        @Nullable RecordReaderConfig recordReaderConfig) {
    }

    @Override
    public boolean hasNext() {
      boolean hasNext = _delegate.hasNext();
      if (!hasNext) {
        _firstPassDone = true;
      }
      return hasNext;
    }

    @Override
    public GenericRow next(GenericRow reuse)
        throws IOException {
      if (_firstPassDone) {
        throw _failure;
      }
      return _delegate.next(reuse);
    }

    @Override
    public void rewind() {
      _delegate.rewind();
    }

    @Override
    public void close() {
    }
  }
}
