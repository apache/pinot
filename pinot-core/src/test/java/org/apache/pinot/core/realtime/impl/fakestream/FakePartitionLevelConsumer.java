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
package org.apache.pinot.core.realtime.impl.fakestream;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.realtime.stream.MessageBatch;
import org.apache.pinot.core.realtime.stream.PartitionLevelConsumer;
import org.apache.pinot.core.util.AvroUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils.*;


/**
 * Implementation of {@link PartitionLevelConsumer} for fake stream
 * Unpacks tar files in /resources/data/On_Time_Performance_2014_partition_<partition>.tar.gz as source of messages
 */
public class FakePartitionLevelConsumer implements PartitionLevelConsumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(FakePartitionLevelConsumer.class.getName());

  private List<Integer> messageOffsets = new ArrayList<>();
  private List<byte[]> messageBytes = new ArrayList<>();

  FakePartitionLevelConsumer(int partition) {

    String avroTarFileName = FakeStreamConfigUtils.getAvroTarFileName(partition);
    File tempDir = new File(FileUtils.getTempDirectory(), getClass().getSimpleName());
    File outputDir = new File(tempDir, String.valueOf(partition));

    int offset = 0;

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(65536)) {
      List<File> avroFiles = unpackAvroTarFile(avroTarFileName, outputDir);

      for (File avroFile : avroFiles) {
        try (DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(avroFile)) {
          BinaryEncoder binaryEncoder = new EncoderFactory().directBinaryEncoder(outputStream, null);
          GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(reader.getSchema());

          for (GenericRecord genericRecord : reader) {
            outputStream.reset();

            datumWriter.write(genericRecord, binaryEncoder);
            binaryEncoder.flush();

            byte[] bytes = outputStream.toByteArray();
            // contiguous offsets
            messageOffsets.add(offset++);
            messageBytes.add(bytes);
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Could not create {}", FakePartitionLevelConsumer.class.getName(), e);
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  @Override
  public MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis) throws TimeoutException {
    if (startOffset >= FakeStreamConfigUtils.getLargestOffset()) {
      return new FakeStreamMessageBatch(Collections.emptyList(), Collections.emptyList());
    }
    if (startOffset < FakeStreamConfigUtils.getSmallestOffset()) {
      startOffset = FakeStreamConfigUtils.getSmallestOffset();
    }
    if (endOffset > FakeStreamConfigUtils.getLargestOffset()) {
      endOffset = FakeStreamConfigUtils.getLargestOffset();
    }
    return new FakeStreamMessageBatch(messageOffsets.subList((int) startOffset, (int) endOffset),
        messageBytes.subList((int) startOffset, (int) endOffset));
  }

  @Override
  public void close() throws IOException {
  }
}
