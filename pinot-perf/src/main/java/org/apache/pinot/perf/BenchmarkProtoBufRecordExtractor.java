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
package org.apache.pinot.perf;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.plugin.inputformat.protobuf.ProtoBufMessageDecoder;
import org.apache.pinot.plugin.inputformat.protobuf.ProtoBufRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * JMH Benchmark for ProtoBufRecordExtractor field descriptor caching optimization.
 *
 * <p>This benchmark measures the performance improvement from:
 * <ul>
 *   <li>Field descriptor caching (eliminates repeated findFieldByName() calls)</li>
 *   <li>Reusable ProtoBufFieldInfo object (reduces GC pressure)</li>
 * </ul>
 *
 * <p>Run with:
 * <pre>
 * java -jar pinot-perf/target/benchmarks.jar BenchmarkProtoBufRecordExtractor
 * </pre>
 *
 * <p>Or via generated script:
 * <pre>
 * ./pinot-perf/target/pinot-perf-pkg/bin/pinot-BenchmarkProtoBufRecordExtractor.sh
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@State(Scope.Benchmark)
public class BenchmarkProtoBufRecordExtractor {

  private static final String DESCRIPTOR_FILE = "complex_types.desc";
  private static final String PROTO_CLASS_NAME = "TestMessage";

  // Field name constants
  private static final String STRING_FIELD = "string_field";
  private static final String INT_FIELD = "int_field";
  private static final String LONG_FIELD = "long_field";
  private static final String DOUBLE_FIELD = "double_field";
  private static final String FLOAT_FIELD = "float_field";
  private static final String BOOL_FIELD = "bool_field";
  private static final String BYTES_FIELD = "bytes_field";
  private static final String REPEATED_STRINGS = "repeated_strings";
  private static final String NESTED_MESSAGE = "nested_message";
  private static final String NESTED_INT_FIELD = "nested_int_field";
  private static final String NESTED_STRING_FIELD = "nested_string_field";
  private static final String REPEATED_NESTED_MESSAGES = "repeated_nested_messages";
  private static final String COMPLEX_MAP = "complex_map";
  private static final String SIMPLE_MAP = "simple_map";
  private static final String ENUM_FIELD = "enum_field";

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkProtoBufRecordExtractor.class.getSimpleName()).shouldDoGC(true);
    new Runner(opt.build()).run();
  }

  @Param({"all_fields", "subset_5_fields", "single_field"})
  private String _extractionMode;

  @Param({"1000"})
  private int _numMessages;

  private List<byte[]> _messagePayloads;
  private List<Message> _parsedMessages;
  private int _currentIndex = 0;

  private ProtoBufMessageDecoder _decoder;
  private ProtoBufRecordExtractor _extractor;
  private GenericRow _reusableRow;
  private Set<String> _fieldsToExtract;

  @Setup(Level.Trial)
  public void setUp()
      throws Exception {
    // Determine fields to extract based on mode
    switch (_extractionMode) {
      case "all_fields":
        _fieldsToExtract = null; // null means extract all
        break;
      case "subset_5_fields":
        _fieldsToExtract = new HashSet<>(Arrays.asList(STRING_FIELD, INT_FIELD, LONG_FIELD, DOUBLE_FIELD, FLOAT_FIELD));
        break;
      case "single_field":
        _fieldsToExtract = new HashSet<>(Arrays.asList(STRING_FIELD));
        break;
      default:
        _fieldsToExtract = null;
        break;
    }

    // Load descriptor file from classpath and copy to temp file
    // (ProtoBufMessageDecoder requires a file path, not a classpath resource)
    InputStream descriptorStream = getClass().getClassLoader().getResourceAsStream(DESCRIPTOR_FILE);
    if (descriptorStream == null) {
      throw new RuntimeException("Could not find descriptor file: " + DESCRIPTOR_FILE);
    }

    File tempDescriptorFile = File.createTempFile("benchmark_", ".desc");
    tempDescriptorFile.deleteOnExit();
    try (OutputStream out = new FileOutputStream(tempDescriptorFile)) {
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = descriptorStream.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
      }
    }
    descriptorStream.close();

    // Initialize decoder with temp file path
    _decoder = new ProtoBufMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("descriptorFile", tempDescriptorFile.getAbsolutePath());
    props.put("protoClassName", PROTO_CLASS_NAME);
    _decoder.init(props, _fieldsToExtract, "testTopic");

    // Initialize extractor
    _extractor = new ProtoBufRecordExtractor();
    _extractor.init(_fieldsToExtract, null);

    // Parse schema for message generation
    DynamicSchema dynamicSchema =
        DynamicSchema.parseFrom(getClass().getClassLoader().getResourceAsStream(DESCRIPTOR_FILE));
    Descriptors.Descriptor descriptor = dynamicSchema.getMessageDescriptor(PROTO_CLASS_NAME);

    // Generate test messages
    _messagePayloads = new ArrayList<>(_numMessages);
    _parsedMessages = new ArrayList<>(_numMessages);
    Random random = new Random(42);

    for (int i = 0; i < _numMessages; i++) {
      Message message = generateTestMessage(descriptor, random, i);
      _messagePayloads.add(message.toByteArray());
      _parsedMessages.add(message);
    }

    _reusableRow = new GenericRow();
  }

  private Message generateTestMessage(Descriptors.Descriptor descriptor, Random random, int index) {
    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);

    setField(builder, descriptor, STRING_FIELD, "test_" + index + "_" + randomString(random, 20));
    setField(builder, descriptor, INT_FIELD, random.nextInt(1000000));
    setField(builder, descriptor, LONG_FIELD, random.nextLong());
    setField(builder, descriptor, DOUBLE_FIELD, random.nextDouble() * 1000);
    setField(builder, descriptor, FLOAT_FIELD, random.nextFloat() * 100);
    setField(builder, descriptor, BOOL_FIELD, random.nextBoolean());
    setField(builder, descriptor, BYTES_FIELD, ByteString.copyFromUtf8(randomString(random, 50)));

    // Set enum field
    Descriptors.FieldDescriptor enumField = descriptor.findFieldByName(ENUM_FIELD);
    if (enumField != null) {
      Descriptors.EnumDescriptor enumDesc = enumField.getEnumType();
      builder.setField(enumField, enumDesc.getValues().get(random.nextInt(enumDesc.getValues().size())));
    }

    // Set repeated strings
    Descriptors.FieldDescriptor repeatedField = descriptor.findFieldByName(REPEATED_STRINGS);
    if (repeatedField != null) {
      for (int i = 0; i < 5; i++) {
        builder.addRepeatedField(repeatedField, "item_" + i + "_" + randomString(random, 10));
      }
    }

    // Set nested message
    Descriptors.FieldDescriptor nestedField = descriptor.findFieldByName(NESTED_MESSAGE);
    if (nestedField != null) {
      DynamicMessage.Builder nestedBuilder = DynamicMessage.newBuilder(nestedField.getMessageType());
      setField(nestedBuilder, nestedField.getMessageType(), NESTED_INT_FIELD, random.nextInt(100));
      setField(nestedBuilder, nestedField.getMessageType(), NESTED_STRING_FIELD, "nested_" + randomString(random, 15));
      builder.setField(nestedField, nestedBuilder.build());
    }

    // Set simple map
    Descriptors.FieldDescriptor simpleMapField = descriptor.findFieldByName(SIMPLE_MAP);
    if (simpleMapField != null) {
      Descriptors.Descriptor mapEntryDesc = simpleMapField.getMessageType();
      for (int i = 0; i < 3; i++) {
        DynamicMessage.Builder entryBuilder = DynamicMessage.newBuilder(mapEntryDesc);
        entryBuilder.setField(mapEntryDesc.findFieldByName("key"), "key_" + i);
        entryBuilder.setField(mapEntryDesc.findFieldByName("value"), random.nextInt(100));
        builder.addRepeatedField(simpleMapField, entryBuilder.build());
      }
    }

    return builder.build();
  }

  private void setField(DynamicMessage.Builder builder, Descriptors.Descriptor descriptor, String fieldName,
      Object value) {
    Descriptors.FieldDescriptor field = descriptor.findFieldByName(fieldName);
    if (field != null) {
      builder.setField(field, value);
    }
  }

  private String randomString(Random random, int length) {
    StringBuilder sb = new StringBuilder(length);
    String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (int i = 0; i < length; i++) {
      sb.append(chars.charAt(random.nextInt(chars.length())));
    }
    return sb.toString();
  }

  private byte[] getNextPayload() {
    byte[] payload = _messagePayloads.get(_currentIndex);
    _currentIndex = (_currentIndex + 1) % _numMessages;
    return payload;
  }

  private Message getNextParsedMessage() {
    Message message = _parsedMessages.get(_currentIndex);
    _currentIndex = (_currentIndex + 1) % _numMessages;
    return message;
  }

  /**
   * Benchmark: Full decode path (parse + extract)
   * Measures complete ProtoBufMessageDecoder.decode() including protobuf parsing and field extraction.
   */
  @Benchmark
  public GenericRow decodeFullPath(Blackhole blackhole) {
    byte[] payload = getNextPayload();
    _reusableRow.clear();

    GenericRow result = _decoder.decode(payload, _reusableRow);

    blackhole.consume(result);
    return result;
  }

  /**
   * Benchmark: Extraction only (pre-parsed messages)
   * Isolates ProtoBufRecordExtractor.extract() performance - this is where caching helps most.
   */
  @Benchmark
  public GenericRow extractOnly(Blackhole blackhole) {
    Message message = getNextParsedMessage();
    _reusableRow.clear();

    GenericRow result = _extractor.extract(message, _reusableRow);

    blackhole.consume(result);
    return result;
  }

  /**
   * Benchmark: Extraction without caching (simulates old behavior)
   * Creates a new extractor for each extraction to show the benefit of caching.
   */
  @Benchmark
  public GenericRow extractWithoutCaching(Blackhole blackhole) {
    Message message = getNextParsedMessage();
    _reusableRow.clear();

    // Create new extractor each time to simulate no caching
    ProtoBufRecordExtractor freshExtractor = new ProtoBufRecordExtractor();
    freshExtractor.init(_fieldsToExtract, null);

    GenericRow result = freshExtractor.extract(message, _reusableRow);

    blackhole.consume(result);
    return result;
  }
}
