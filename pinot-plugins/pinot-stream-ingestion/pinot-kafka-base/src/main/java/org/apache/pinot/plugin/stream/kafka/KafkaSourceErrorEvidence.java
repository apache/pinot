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
package org.apache.pinot.plugin.stream.kafka;

import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorEvidence;
import org.apache.pinot.spi.ingestion.error.IngestionSourceErrorVerdict;


/// The evidence the Kafka plugins attach to the exceptions they create for a source failure. These are templates:
/// [IngestionSourceErrorEvidence#attach] copies them onto each failure.
public final class KafkaSourceErrorEvidence {
  /// The broker's metadata lacks the topic, which the synthesized "Topic does not exist" doesn't say.
  public static final IngestionSourceErrorEvidence TOPIC_MISSING = new IngestionSourceErrorEvidence()
      .setTarget(IngestionSourceErrorVerdict.TARGET_BROKER)
      .setEvidence(0, IngestionSourceErrorEvidence.UNKNOWN_TOPIC_OR_PARTITION);
  /// The broker never answered in time.
  public static final IngestionSourceErrorEvidence BROKER_UNREACHABLE = new IngestionSourceErrorEvidence()
      .setTarget(IngestionSourceErrorVerdict.TARGET_BROKER)
      .setEvidence(0, IngestionSourceErrorEvidence.UNREACHABLE);

  private KafkaSourceErrorEvidence() {
  }
}
