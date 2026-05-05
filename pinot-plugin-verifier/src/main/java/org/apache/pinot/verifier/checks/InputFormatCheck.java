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
package org.apache.pinot.verifier.checks;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.verifier.PluginVerifier.CheckContext;


/// Loads each known {@link RecordReader} implementation through {@code PluginManager}. We
/// don't drive {@code init(...)} against a real file because each reader expects a different
/// on-disk format; the discovery + no-arg constructor path is what the realm work actually
/// changes. Real file-format coverage lives in pinot-integration-tests.
public final class InputFormatCheck implements Check {
  /// Map of pluginName → RecordReader FQCN. Plugin name matches the plugin directory layout
  /// under {@code plugins/pinot-input-format/<name>/} so {@code --strict-realm} can pin the
  /// lookup.
  private static final Map<String, String> READERS = new LinkedHashMap<>() {{
    put("pinot-avro", "org.apache.pinot.plugin.inputformat.avro.AvroRecordReader");
    put("pinot-csv", "org.apache.pinot.plugin.inputformat.csv.CSVRecordReader");
    put("pinot-json", "org.apache.pinot.plugin.inputformat.json.JSONRecordReader");
    put("pinot-orc", "org.apache.pinot.plugin.inputformat.orc.ORCRecordReader");
    put("pinot-parquet", "org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader");
    put("pinot-protobuf", "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufRecordReader");
    put("pinot-thrift", "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReader");
    put("pinot-arrow", "org.apache.pinot.plugin.inputformat.arrow.ArrowRecordReader");
    put("pinot-confluent-avro", "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder");
    put("pinot-confluent-json", "org.apache.pinot.plugin.inputformat.json.confluent.KafkaConfluentSchemaRegistryJsonMessageDecoder");
    put("pinot-confluent-protobuf", "org.apache.pinot.plugin.inputformat.protobuf.confluent.KafkaConfluentSchemaRegistryProtoBufMessageDecoder");
  }};

  @Override
  public String name() {
    return "Input format plugins (RecordReader / MessageDecoder)";
  }

  @Override
  public Outcome run(CheckContext context) {
    int pass = 0;
    int fail = 0;
    for (Map.Entry<String, String> e : READERS.entrySet()) {
      String pluginName = e.getKey();
      String fqcn = e.getValue();
      if (!context.targets(fqcn)) {
        continue;
      }
      try {
        Object instance = context.createInstance(pluginName, fqcn);
        if (!(instance instanceof RecordReader || instance.getClass().getName().contains("MessageDecoder"))) {
          // The Confluent classes implement StreamMessageDecoder rather than RecordReader.
          // We accept both — the check is about whether they instantiate at all.
        }
        System.out.println("  PASS  " + fqcn);
        if (context.verbose()) {
          for (String line : context.describeCodeSource(instance.getClass())) {
            System.out.println("        " + line);
          }
        }
        pass++;
      } catch (Throwable t) {
        System.out.println("  FAIL  " + fqcn + " — " + t.getClass().getSimpleName() + ": " + t.getMessage());
        if (context.verbose()) {
          t.printStackTrace(System.out);
        }
        fail++;
      }
    }
    return new Outcome(pass, fail);
  }
}
