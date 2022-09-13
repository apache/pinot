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
package org.apache.pinot.plugin.stream.kinesis;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.stream.RowMetadata;
import software.amazon.awssdk.services.kinesis.model.Record;

// TODO: Make this an interface/api in the stream spis st StreamMessageMetadata extract(T streamTypeRecord)

public interface KinesisMetadataExtractor {
  static KinesisMetadataExtractor build(boolean populateMetadata) {
    return record -> {
      long recordTimestamp = record.approximateArrivalTimestamp().toEpochMilli();
      Map<String, String> metadataMap = new HashMap<>();
      metadataMap.put(KinesisStreamMessageMetadata.APPRX_ARRIVAL_TIMESTAMP_KEY, String.valueOf(recordTimestamp));
      if (!populateMetadata) {
        return new KinesisStreamMessageMetadata(recordTimestamp, null, metadataMap);
      }

      String seqNumber = record.sequenceNumber();
      metadataMap.put(KinesisStreamMessageMetadata.SEQUENCE_NUMBER_KEY, seqNumber);
      return new KinesisStreamMessageMetadata(recordTimestamp, null, metadataMap);
    };
  }
  RowMetadata extract(Record record);
}
