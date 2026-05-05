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
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.verifier.PluginVerifier.CheckContext;


/// Each {@link StreamConsumerFactory} is normally instantiated by
/// {@code StreamConsumerFactoryProvider.create(StreamConfig)} after parsing a table's stream
/// config. We don't synthesise a {@code StreamConfig} (it has many required fields per
/// stream type) — we just instantiate the factory class via {@code PluginManager} the same
/// way the provider does internally. That covers the realm-loading concern; talking to a real
/// Kafka / Kinesis / Pulsar broker is integration-test territory.
public final class StreamConsumerCheck implements Check {
  /// Map of pluginName → StreamConsumerFactory FQCN. Plugin name matches
  /// {@code plugins/pinot-stream-ingestion/<name>}.
  private static final Map<String, String> CONSUMERS = new LinkedHashMap<>() {{
    put("pinot-kafka-3.0", "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory");
    put("pinot-kinesis", "org.apache.pinot.plugin.stream.kinesis.KinesisConsumerFactory");
    put("pinot-pulsar", "org.apache.pinot.plugin.stream.pulsar.PulsarConsumerFactory");
  }};

  @Override
  public String name() {
    return "Stream consumer plugins (StreamConsumerFactory)";
  }

  @Override
  public Outcome run(CheckContext context) {
    int pass = 0;
    int fail = 0;
    for (Map.Entry<String, String> e : CONSUMERS.entrySet()) {
      String pluginName = e.getKey();
      String fqcn = e.getValue();
      if (!context.targets(fqcn)) {
        continue;
      }
      try {
        Object instance = context.createInstance(pluginName, fqcn);
        if (!(instance instanceof StreamConsumerFactory)) {
          throw new IllegalStateException(fqcn + " is not a StreamConsumerFactory");
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
