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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkForwardIndexWriterV4;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkWriter;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkForwardIndexReaderV4;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.MapUtils;
import org.apache.pinot.spi.utils.MapUtils.PreparedMapKey;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class VarByteChunkV4Test implements PinotBuffersAfterClassCheckRule {

  protected File[] _dirs;

  @DataProvider(parallel = true)
  public Object[][] params() {
    Object[][] params = new Object[][]{
        {null, ChunkCompressionType.LZ4, 20, 1024},
        {null, ChunkCompressionType.LZ4_LENGTH_PREFIXED, 20, 1024},
        {null, ChunkCompressionType.PASS_THROUGH, 20, 1024},
        {null, ChunkCompressionType.SNAPPY, 20, 1024},
        {null, ChunkCompressionType.ZSTANDARD, 20, 1024},
        {null, ChunkCompressionType.LZ4, 2048, 1024},
        {null, ChunkCompressionType.LZ4_LENGTH_PREFIXED, 2048, 1024},
        {null, ChunkCompressionType.PASS_THROUGH, 2048, 1024},
        {null, ChunkCompressionType.SNAPPY, 2048, 1024},
        {null, ChunkCompressionType.ZSTANDARD, 2048, 1024}
    };

    for (int i = 0; i < _dirs.length; i++) {
      params[i][0] = _dirs[i];
    }

    return params;
  }

  protected String getTestDirName() {
    return "VarByteChunkV4Test";
  }

  @BeforeClass
  public void forceMkDirs()
      throws IOException {
    _dirs = new File[10];
    for (int i = 0; i < _dirs.length; i++) {
      _dirs[i] = new File(new File(FileUtils.getTempDirectory(), UUID.randomUUID().toString()), getTestDirName());
      FileUtils.forceMkdir(_dirs[i]);
    }
  }

  @AfterClass
  public void deleteDirs() {
    for (File dir : _dirs) {
      FileUtils.deleteQuietly(dir);
    }
  }

  @Test(dataProvider = "params")
  public void testStringSV(File file, ChunkCompressionType compressionType, int longestEntry, int chunkSize)
      throws IOException {
    File stringSVFile = new File(file, "testStringSV");
    testWriteRead(stringSVFile, compressionType, longestEntry, chunkSize, FieldSpec.DataType.STRING, x -> x,
        VarByteChunkWriter::putString, (reader, context, docId) -> reader.getString(docId, context));
    FileUtils.deleteQuietly(stringSVFile);
  }

  @Test(dataProvider = "params")
  public void testBytesSV(File file, ChunkCompressionType compressionType, int longestEntry, int chunkSize)
      throws IOException {
    File bytesSVFile = new File(file, "testBytesSV");
    testWriteRead(bytesSVFile, compressionType, longestEntry, chunkSize, FieldSpec.DataType.BYTES,
        x -> x.getBytes(StandardCharsets.UTF_8), VarByteChunkWriter::putBytes,
        (reader, context, docId) -> reader.getBytes(docId, context));
    FileUtils.deleteQuietly(bytesSVFile);
  }

  @Test(dataProvider = "params")
  public void testStringMV(File file, ChunkCompressionType compressionType, int longestEntry, int chunkSize)
      throws IOException {
    File stringMVFile = new File(file, "testStringMV");
    testWriteRead(stringMVFile, compressionType, longestEntry, chunkSize, FieldSpec.DataType.STRING,
        new StringSplitterMV(), VarByteChunkWriter::putStringMV,
        (reader, context, docId) -> reader.getStringMV(docId, context));
    FileUtils.deleteQuietly(stringMVFile);
  }

  @Test(dataProvider = "params")
  public void testBytesMV(File file, ChunkCompressionType compressionType, int longestEntry, int chunkSize)
      throws IOException {
    File bytesMVFile = new File(file, "testBytesMV");
    testWriteRead(bytesMVFile, compressionType, longestEntry, chunkSize, FieldSpec.DataType.BYTES, new ByteSplitterMV(),
        VarByteChunkWriter::putBytesMV, (reader, context, docId) -> reader.getBytesMV(docId, context));
    FileUtils.deleteQuietly(bytesMVFile);
  }

  /// A sealed MAP column is a chunked raw index over serialized frames, and a projected `attributes['key']` resolves
  /// to a single-key read against it. Pins that the selective readers agree with deserializing the whole frame, for
  /// every compression type and - through the subclasses - every reader version.
  @Test(dataProvider = "params")
  public void testMapSV(File file, ChunkCompressionType compressionType, int longestEntry, int chunkSize)
      throws IOException {
    File mapSVFile = new File(file, "testMapSV");
    int numDocs = 1000;
    List<Map<String, Object>> maps = new ArrayList<>(numDocs);
    try (VarByteChunkWriter writer = createWriter(mapSVFile, compressionType, chunkSize)) {
      for (int i = 0; i < numDocs; i++) {
        // Dotted OpenTelemetry-style keys, the first two of equal length so the scan has to compare their bytes
        // rather than skip on a length mismatch.
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("k8s.workload.name", "workload-" + i);
        map.put("k8s.workload.kind", "Deployment");
        map.put("k8s.namespace.name", "namespace-" + i % 7);
        map.put("host_logical_cpus", i);
        maps.add(map);
        writer.putBytes(MapUtils.serializeMap(map));
      }
    }

    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(mapSVFile);
        VarByteChunkForwardIndexReaderV4 reader = createReader(buffer, FieldSpec.DataType.MAP, true);
        VarByteChunkForwardIndexReaderV4.ReaderContext context = reader.createContext()) {
      PreparedMapKey workloadName = new PreparedMapKey("k8s.workload.name");
      PreparedMapKey cpus = new PreparedMapKey("host_logical_cpus");
      PreparedMapKey absent = new PreparedMapKey("k8s.workload.namf");
      PreparedMapKey[] allKeys = {workloadName, absent, cpus, new PreparedMapKey("k8s.namespace.name")};
      String[] allValues = new String[allKeys.length];
      for (int i = 0; i < numDocs; i++) {
        Map<String, Object> expected = maps.get(i);
        assertEquals(reader.getMap(i, context), expected);
        assertEquals(reader.getMapAsJsonString(i, context), MapUtils.toString(expected));
        assertEquals(reader.getMapEntryValue(i, context, workloadName), expected.get("k8s.workload.name"));
        assertEquals(reader.getMapEntryValueAsString(i, context, workloadName), expected.get("k8s.workload.name"));
        assertEquals(reader.getMapEntryValue(i, context, cpus), expected.get("host_logical_cpus"));
        assertEquals(reader.getMapEntryValueAsString(i, context, cpus), String.valueOf(i));
        assertNull(reader.getMapEntryValue(i, context, absent));
        assertNull(reader.getMapEntryValueAsString(i, context, absent));

        // Reading every key in one walk of the frame has to agree with reading them one at a time.
        reader.getMapEntryValuesAsString(i, context, allKeys, allValues);
        for (int k = 0; k < allKeys.length; k++) {
          assertEquals(allValues[k], reader.getMapEntryValueAsString(i, context, allKeys[k]));
        }
      }
    }
    FileUtils.deleteQuietly(mapSVFile);
  }

  static class StringSplitterMV implements Function<String, String[]> {
    @Override
    public String[] apply(String input) {
      List<String> res = new ArrayList<>();
      for (int i = 0; i < input.length(); i += 3) {
        int endIndex = Math.min(i + 3, input.length());
        res.add(input.substring(i, endIndex));
      }
      return res.toArray(new String[0]);
    }
  }

  static class ByteSplitterMV implements Function<String, byte[][]> {
    @Override
    public byte[][] apply(String input) {
      List<byte[]> res = new ArrayList<>();
      for (int i = 0; i < input.length(); i += 3) {
        int endIndex = Math.min(i + 3, input.length());
        res.add(input.substring(i, endIndex).getBytes());
      }
      return res.toArray(new byte[0][]);
    }
  }

  protected VarByteChunkWriter createWriter(File file, ChunkCompressionType compressionType, int chunkSize)
      throws IOException {
    return new VarByteChunkForwardIndexWriterV4(file, compressionType, chunkSize);
  }

  protected VarByteChunkForwardIndexReaderV4 createReader(PinotDataBuffer buffer, FieldSpec.DataType dataType,
      boolean isSingleValue) {
    return new VarByteChunkForwardIndexReaderV4(buffer, dataType, isSingleValue);
  }

  protected <T> void testWriteRead(File file, ChunkCompressionType compressionType, int longestEntry, int chunkSize,
      FieldSpec.DataType dataType, Function<String, T> forwardMapper,
      BiConsumer<VarByteChunkWriter, T> write,
      Read<T> read)
      throws IOException {
    List<T> values = randomStrings(1000, longestEntry).map(forwardMapper).collect(Collectors.toList());
    try (VarByteChunkWriter writer = createWriter(file, compressionType, chunkSize)) {
      for (T value : values) {
        write.accept(writer, value);
      }
    }
    try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(file);
        VarByteChunkForwardIndexReaderV4 reader = createReader(buffer, dataType, true);
        VarByteChunkForwardIndexReaderV4.ReaderContext context = reader.createContext()) {
      for (int i = 0; i < values.size(); i++) {
        assertEquals(read.read(reader, context, i), values.get(i));
      }
      for (int i = 0; i < values.size(); i += 2) {
        assertEquals(read.read(reader, context, i), values.get(i));
      }
      for (int i = 1; i < values.size(); i += 2) {
        assertEquals(read.read(reader, context, i), values.get(i));
      }
      for (int i = 1; i < values.size(); i += 100) {
        assertEquals(read.read(reader, context, i), values.get(i));
      }
      for (int i = values.size() - 1; i >= 0; i--) {
        assertEquals(read.read(reader, context, i), values.get(i));
      }
      for (int i = values.size() - 1; i >= 0; i -= 2) {
        assertEquals(read.read(reader, context, i), values.get(i));
      }
      for (int i = values.size() - 2; i >= 0; i -= 2) {
        assertEquals(read.read(reader, context, i), values.get(i));
      }
      for (int i = values.size() - 1; i >= 0; i -= 100) {
        assertEquals(read.read(reader, context, i), values.get(i));
      }
    }
  }

  protected Stream<String> randomStrings(int count, int lengthOfLongestEntry) {
    return IntStream.range(0, count)
        .mapToObj(i -> {
          int length = ThreadLocalRandom.current().nextInt(lengthOfLongestEntry);
          byte[] bytes = new byte[length];
          if (length != 0) {
            bytes[bytes.length - 1] = 'c';
            if (length > 2) {
              Arrays.fill(bytes, 1, bytes.length - 1, (byte) 'b');
            }
            bytes[0] = 'a';
          }
          return new String(bytes, StandardCharsets.UTF_8);
        });
  }

  @FunctionalInterface
  interface Read<T> {
    T read(VarByteChunkForwardIndexReaderV4 reader, VarByteChunkForwardIndexReaderV4.ReaderContext context,
        int docId);
  }
}
