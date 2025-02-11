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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageMetadata;

// TODO: Make it a util class
public class KafkaStreamMessageMetadata extends StreamMessageMetadata {
  public static final String METADATA_OFFSET_KEY = "offset";
  public static final String RECORD_TIMESTAMP_KEY = "recordTimestamp";
  public static final String METADATA_PARTITION_KEY = "partition";

  @Deprecated
  public KafkaStreamMessageMetadata(long recordIngestionTimeMs, @Nullable GenericRow headers,
      Map<String, String> metadata) {
    super(recordIngestionTimeMs, headers, metadata);
  }
}
