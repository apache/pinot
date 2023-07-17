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
package org.apache.pinot.plugin.stream.kafka20;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.RowMetadata;


@FunctionalInterface
public interface KafkaMetadataExtractor {
  static KafkaMetadataExtractor build(boolean populateMetadata) {
    return record -> {
      long recordTimestamp = record.timestamp();
      Map<String, String> metadataMap = new HashMap<>();
      metadataMap.put(KafkaStreamMessageMetadata.METADATA_OFFSET_KEY, String.valueOf(record.offset()));
      metadataMap.put(KafkaStreamMessageMetadata.RECORD_TIMESTAMP_KEY, String.valueOf(recordTimestamp));

      if (!populateMetadata) {
        return new KafkaStreamMessageMetadata(recordTimestamp, RowMetadata.EMPTY_ROW, metadataMap);
      }
      GenericRow headerGenericRow = new GenericRow();
      Headers headers = record.headers();
      if (headers != null) {
        Header[] headersArray = headers.toArray();
        for (Header header : headersArray) {
          headerGenericRow.putValue(header.key(), header.value());
        }
      }
      return new KafkaStreamMessageMetadata(record.timestamp(), headerGenericRow, metadataMap);
    };
  }

  RowMetadata extract(ConsumerRecord<?, ?> consumerRecord);
}
