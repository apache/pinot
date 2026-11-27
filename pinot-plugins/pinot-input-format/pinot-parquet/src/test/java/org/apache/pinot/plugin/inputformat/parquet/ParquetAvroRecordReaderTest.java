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
package org.apache.pinot.plugin.inputformat.parquet;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordFetchException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class ParquetAvroRecordReaderTest {

  private static final Schema SIMPLE_SCHEMA = SchemaBuilder.record("TestRecord")
      .fields()
      .requiredString("name")
      .endRecord();

  @FunctionalInterface
  private interface RecordSupplier {
    GenericRecord read()
        throws IOException;
  }

  @Test
  public void testUncheckedPrefetchFailureWrappedAsRecordFetchException()
      throws IOException {
    File dataFile = createSimpleParquetFile();
    try {
      GenericRecord initialRecord = createRecord("alice");
      ParquetDecodingException decodingException = new ParquetDecodingException("Corrupt Parquet page");
      AtomicInteger reads = new AtomicInteger();

      try (ParquetAvroRecordReader reader = readerWithStubbedRead(() -> {
        if (reads.getAndIncrement() == 0) {
          return initialRecord;
        }
        throw decodingException;
      })) {
        reader.init(dataFile, null, null);
        assertTrue(reader.hasNext());

        RecordFetchException exception =
            expectThrows(RecordFetchException.class, () -> reader.next(new GenericRow()));
        assertSame(exception.getCause(), decodingException);
        // Prefetch failed before advancing, so the reader remains on the unadvanced record.
        assertTrue(reader.hasNext());
      }
    } finally {
      FileUtils.deleteQuietly(dataFile);
    }
  }

  @Test
  public void testCheckedIoExceptionPrefetchFailureWrappedAsRecordFetchException()
      throws IOException {
    File dataFile = createSimpleParquetFile();
    try {
      GenericRecord initialRecord = createRecord("alice");
      IOException ioException = new IOException("Underlying I/O failure");
      AtomicInteger reads = new AtomicInteger();

      try (ParquetAvroRecordReader reader = readerWithStubbedRead(() -> {
        if (reads.getAndIncrement() == 0) {
          return initialRecord;
        }
        throw ioException;
      })) {
        reader.init(dataFile, null, null);
        assertTrue(reader.hasNext());

        RecordFetchException exception =
            expectThrows(RecordFetchException.class, () -> reader.next(new GenericRow()));
        assertSame(exception.getCause(), ioException);
        assertTrue(reader.hasNext());
      }
    } finally {
      FileUtils.deleteQuietly(dataFile);
    }
  }

  @Test
  public void testRepeatedUncheckedPrefetchFailuresTerminateAtConfiguredThreshold()
      throws IOException {
    File dataFile = createSimpleParquetFile();
    try {
      GenericRecord initialRecord = createRecord("alice");
      ParquetDecodingException decodingException = new ParquetDecodingException("Persistent decode error");
      AtomicInteger reads = new AtomicInteger();

      int maxConsecutiveFetchFailuresAllowed = 3;
      int consecutiveFetchFailures = 0;
      int parseFailures = 0;
      int attempts = 0;

      try (ParquetAvroRecordReader reader = readerWithStubbedRead(() -> {
        if (reads.getAndIncrement() == 0) {
          return initialRecord;
        }
        throw decodingException;
      })) {
        reader.init(dataFile, null, null);
        GenericRow reuse = new GenericRow();
        // Mirrors SegmentMapper's continueOnError + maxConsecutiveRecordFetchFailuresAllowed loop.
        while (reader.hasNext() && attempts < 10) {
          attempts++;
          try {
            reader.next(reuse);
            consecutiveFetchFailures = 0;
          } catch (Exception e) {
            if (e instanceof RecordFetchException) {
              assertSame(e.getCause(), decodingException);
              consecutiveFetchFailures++;
              if (consecutiveFetchFailures >= maxConsecutiveFetchFailuresAllowed) {
                break;
              }
            } else {
              parseFailures++;
            }
          }
          reuse.clear();
        }
      }

      assertEquals(attempts, maxConsecutiveFetchFailuresAllowed);
      assertEquals(consecutiveFetchFailures, maxConsecutiveFetchFailuresAllowed);
      assertEquals(parseFailures, 0);
      // 1 initial read in init() + 3 failed prefetch reads in next().
      assertEquals(reads.get(), 1 + maxConsecutiveFetchFailuresAllowed);
    } finally {
      FileUtils.deleteQuietly(dataFile);
    }
  }

  @Test
  public void testExtractionFailurePreservedAsParseErrorAndAdvancesReader()
      throws IOException {
    File dataFile = createSimpleParquetFile();
    try {
      IllegalArgumentException extractException = new IllegalArgumentException("Simulated parse failure");
      GenericRecord badRecord = createFailingRecord(extractException);
      GenericRecord goodRecord = createRecord("bob");
      GenericRecord[] sequence = new GenericRecord[]{badRecord, goodRecord, null};
      AtomicInteger reads = new AtomicInteger();

      try (ParquetAvroRecordReader reader = readerWithStubbedRead(() -> sequence[reads.getAndIncrement()])) {
        reader.init(dataFile, null, null);
        assertTrue(reader.hasNext());

        IllegalArgumentException thrown =
            expectThrows(IllegalArgumentException.class, () -> reader.next(new GenericRow()));
        assertSame(thrown, extractException);

        assertTrue(reader.hasNext());
        GenericRow row = reader.next(new GenericRow());
        assertEquals(row.getValue("name"), "bob");
        assertFalse(reader.hasNext());
      }
    } finally {
      FileUtils.deleteQuietly(dataFile);
    }
  }

  @Test
  public void testDualExtractionAndPrefetchFailureThrowsRecordFetchExceptionWithSuppressedCause()
      throws IOException {
    File dataFile = createSimpleParquetFile();
    try {
      IllegalArgumentException extractException = new IllegalArgumentException("Simulated parse failure");
      GenericRecord badRecord = createFailingRecord(extractException);
      ParquetDecodingException decodingException = new ParquetDecodingException("Prefetch decode failure");
      AtomicInteger reads = new AtomicInteger();

      try (ParquetAvroRecordReader reader = readerWithStubbedRead(() -> {
        if (reads.getAndIncrement() == 0) {
          return badRecord;
        }
        throw decodingException;
      })) {
        reader.init(dataFile, null, null);
        assertTrue(reader.hasNext());

        RecordFetchException exception =
            expectThrows(RecordFetchException.class, () -> reader.next(new GenericRow()));
        assertSame(exception.getCause(), decodingException);
        assertEquals(exception.getSuppressed().length, 1);
        assertSame(exception.getSuppressed()[0], extractException);
      }
    } finally {
      FileUtils.deleteQuietly(dataFile);
    }
  }

  @Test
  public void testRejectedRowsAndRewindBehaviorPreserved()
      throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType("message m { required int96 value; }");
    File dataFile = new File(FileUtils.getTempDirectory(), "avro-rejected-int96-" + System.nanoTime() + ".parquet");
    try {
      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new Path(dataFile.getAbsolutePath()))
          .withType(schema).build()) {
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        // Year 1600 overflows signed Long epoch nanos; valid middle row is followed by another rejection.
        writer.write(groups.newGroup().append("value", new NanoTime(2305448, 0L)));
        writer.write(groups.newGroup().append("value", new NanoTime(2440588, 7L)));
        writer.write(groups.newGroup().append("value", new NanoTime(2305448, 0L)));
      }

      ParquetRecordReaderConfig config = new ParquetRecordReaderConfig();
      config.setExtractRawTimeValues(true);
      try (ParquetAvroRecordReader reader = new ParquetAvroRecordReader()) {
        reader.init(dataFile, null, config);
        for (int pass = 0; pass < 2; pass++) {
          if (pass > 0) {
            reader.rewind();
          }
          assertTrue(reader.hasNext());
          expectThrows(IllegalArgumentException.class, () -> reader.next(new GenericRow()));
          assertTrue(reader.hasNext());
          assertEquals(reader.next(new GenericRow()).getValue("value"), 7L);
          assertTrue(reader.hasNext());
          expectThrows(IllegalArgumentException.class, () -> reader.next(new GenericRow()));
          assertFalse(reader.hasNext());
        }
      }
    } finally {
      FileUtils.deleteQuietly(dataFile);
    }
  }

  private static ParquetAvroRecordReader readerWithStubbedRead(RecordSupplier supplier) {
    return new ParquetAvroRecordReader() {
      @Override
      GenericRecord readNextRecord()
          throws IOException {
        return supplier.read();
      }
    };
  }

  private static GenericRecord createRecord(String name) {
    GenericRecord record = new GenericData.Record(SIMPLE_SCHEMA);
    record.put("name", name);
    return record;
  }

  private static GenericRecord createFailingRecord(RuntimeException failure) {
    return new GenericData.Record(SIMPLE_SCHEMA) {
      @Override
      public Object get(String key) {
        throw failure;
      }

      @Override
      public Object get(int i) {
        throw failure;
      }
    };
  }

  private static File createSimpleParquetFile()
      throws IOException {
    File dataFile = new File(FileUtils.getTempDirectory(), "avro-prefetch-test-" + System.nanoTime() + ".parquet");
    try (ParquetWriter<GenericRecord> writer =
        ParquetTestUtils.getParquetAvroWriter(new Path(dataFile.getAbsolutePath()), SIMPLE_SCHEMA)) {
      writer.write(createRecord("seed"));
    }
    return dataFile;
  }
}
