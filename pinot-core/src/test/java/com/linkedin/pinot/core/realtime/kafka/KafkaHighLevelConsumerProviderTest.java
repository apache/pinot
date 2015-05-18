/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.kafka;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import com.linkedin.pinot.core.realtime.RealtimeFileBasedReaderTest;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelConsumerStreamProvider;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelStreamProviderConfig;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


public class KafkaHighLevelConsumerProviderTest {
  private static final String AVRO_DATA = "data/mirror-mv.avro";
  private static Schema schema;
  private static String filePath;
  private static Map<String, FieldType> fieldTypeMap;
  private static StreamProviderConfig config;

  public static void setup() throws Exception {
    filePath = RealtimeFileBasedReaderTest.class.getClassLoader().getResource(AVRO_DATA).getFile();
    fieldTypeMap = new HashMap<String, FieldSpec.FieldType>();
    fieldTypeMap.put("viewerId", FieldType.DIMENSION);
    fieldTypeMap.put("vieweeId", FieldType.DIMENSION);
    fieldTypeMap.put("viewerPrivacySetting", FieldType.DIMENSION);
    fieldTypeMap.put("vieweePrivacySetting", FieldType.DIMENSION);
    fieldTypeMap.put("viewerObfuscationType", FieldType.DIMENSION);
    fieldTypeMap.put("viewerCompanies", FieldType.DIMENSION);
    fieldTypeMap.put("viewerOccupations", FieldType.DIMENSION);
    fieldTypeMap.put("viewerRegionCode", FieldType.DIMENSION);
    fieldTypeMap.put("viewerIndustry", FieldType.DIMENSION);
    fieldTypeMap.put("viewerSchool", FieldType.DIMENSION);
    fieldTypeMap.put("weeksSinceEpochSunday", FieldType.DIMENSION);
    fieldTypeMap.put("daysSinceEpoch", FieldType.DIMENSION);
    fieldTypeMap.put("minutesSinceEpoch", FieldType.TIME);
    fieldTypeMap.put("count", FieldType.METRIC);
    schema = SegmentTestUtils.extractSchemaFromAvro(new File(filePath), fieldTypeMap, TimeUnit.MINUTES);

    Map<String, String> properties = new HashMap<String, String>();
    properties.put(Helix.DataSource.Realtime.Kafka.HighLevelConsumer.GROUP_ID,
        "PinotTestNewHighLevelConsumerMirror_dpatel_local");
    properties.put(Helix.DataSource.Realtime.Kafka.HighLevelConsumer.ZK_CONNECTION_STRING,
        "zk-eat1-kafka.corp.linkedin.com:12913/kafka-aggregate-tracking");
    properties.put(Helix.DataSource.Realtime.Kafka.TOPIC_NAME, "MirrorDecoratedProfileViewEvent");
    properties.put(Helix.DataSource.Realtime.Kafka.DECODER_CLASS,
        "com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder");
    properties.put(
        Helix.DataSource.Realtime.Kafka.getDecoderPropertyKeyFor(KafkaAvroMessageDecoder.SCHEMA_REGISTRY_REST_URL),
        "http://eat1-ei2-schema-vip-z.stg.linkedin.com:10252/schemaRegistry/schemas");

    config = new KafkaHighLevelStreamProviderConfig();
    config.init(properties, schema);
  }

  public static void main(String[] args) throws Exception {
    setup();
    // start the kafka provider
    KafkaHighLevelConsumerStreamProvider streamProvider = new KafkaHighLevelConsumerStreamProvider();
    streamProvider.init(config);
    streamProvider.start();

    // initialize a realtime segment
    final RealtimeSegmentImpl realtimeSegment = new RealtimeSegmentImpl(schema, 10000000);

    Timer timer = new Timer(true);
    timer.schedule(new TimerTask() {

      @Override
      public void run() {
        System.out.println("numDocsIndexed : " + realtimeSegment.getRawDocumentCount() + " docIds so far : "
            + realtimeSegment.getAggregateDocumentCount());
      }
    }, 5000, 5000);

    int counter = 0;
    long start = System.currentTimeMillis();
    while (counter <= 1000) {
      realtimeSegment.index(streamProvider.next());
      counter++;
    }

    System.out.println("time taken : " + (System.currentTimeMillis() - start));
    streamProvider.shutdown();
    timer.cancel();
  }
}
