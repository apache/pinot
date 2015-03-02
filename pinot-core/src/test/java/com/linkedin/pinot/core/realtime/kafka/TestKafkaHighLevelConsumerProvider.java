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
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import com.linkedin.pinot.core.realtime.TestRealtimeFileBasedReader;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelConsumerStreamProvider;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaHighLevelStreamProviderConfig;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaProperties;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


public class TestKafkaHighLevelConsumerProvider {
  private static final String AVRO_DATA = "data/mirror-mv.avro";
  private static Schema schema;
  private static String filePath;
  private static Map<String, FieldType> fieldTypeMap;
  private static StreamProviderConfig config;

  public static void setup() throws Exception {
    filePath = TestRealtimeFileBasedReader.class.getClassLoader().getResource(AVRO_DATA).getFile();
    fieldTypeMap = new HashMap<String, FieldSpec.FieldType>();
    fieldTypeMap.put("viewerId", FieldType.dimension);
    fieldTypeMap.put("vieweeId", FieldType.dimension);
    fieldTypeMap.put("viewerPrivacySetting", FieldType.dimension);
    fieldTypeMap.put("vieweePrivacySetting", FieldType.dimension);
    fieldTypeMap.put("viewerObfuscationType", FieldType.dimension);
    fieldTypeMap.put("viewerCompanies", FieldType.dimension);
    fieldTypeMap.put("viewerOccupations", FieldType.dimension);
    fieldTypeMap.put("viewerRegionCode", FieldType.dimension);
    fieldTypeMap.put("viewerIndustry", FieldType.dimension);
    fieldTypeMap.put("viewerSchool", FieldType.dimension);
    fieldTypeMap.put("weeksSinceEpochSunday", FieldType.dimension);
    fieldTypeMap.put("daysSinceEpoch", FieldType.dimension);
    fieldTypeMap.put("minutesSinceEpoch", FieldType.time);
    fieldTypeMap.put("count", FieldType.metric);
    schema = SegmentTestUtils.extractSchemaFromAvro(new File(filePath), fieldTypeMap, TimeUnit.MINUTES);

    Map<String, String> properties = new HashMap<String, String>();
    properties.put(KafkaProperties.HighLevelConsumer.GROUP_ID, "PinotTestNewHighLevelConsumerMirror_dpatel_local");
    properties.put(KafkaProperties.HighLevelConsumer.ZK_CONNECTION_STRING,
        "zk-eat1-kafka.corp.linkedin.com:12913/kafka-aggregate-tracking");
    properties.put(KafkaProperties.TOPIC_NAME, "MirrorDecoratedProfileViewEvent");
    properties
        .put(KafkaProperties.DECODER_CLASS, "com.linkedin.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder");
    properties.put(KafkaProperties.getDecoderPropertyKeyFor(KafkaAvroMessageDecoder.SCHEMA_REGISTRY_REST_URL),
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
    final RealtimeSegmentImpl realtimeSegment = new RealtimeSegmentImpl(schema);

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
