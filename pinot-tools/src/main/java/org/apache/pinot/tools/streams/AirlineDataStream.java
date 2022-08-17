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
package org.apache.pinot.tools.streams;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.QuickStartBase;
import org.apache.pinot.tools.Quickstart;
import org.apache.pinot.tools.utils.KafkaStarterUtils;


/**
 * This is used in Hybrid Quickstart.
 */
public class AirlineDataStream {
  private static final String KAFKA_TOPIC_NAME = "flights-realtime";
  Schema _pinotSchema;
  String _timeColumnName;
  File _avroFile;
  final Integer _startTime = 16102;
  private StreamDataProducer _producer;
  private PinotRealtimeSource _pinotStream;

  public AirlineDataStream(Schema pinotSchema, TableConfig tableConfig, File avroFile, StreamDataProducer producer)
      throws IOException {
    _pinotSchema = pinotSchema;
    _timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    _avroFile = avroFile;
    _producer = producer;
    AvroFileSourceGenerator generator = new AvroFileSourceGenerator(pinotSchema, avroFile, 1, _timeColumnName,
        (rowNumber) -> (_startTime + rowNumber / 60));
    _pinotStream =
        PinotRealtimeSource.builder().setProducer(_producer).setGenerator(generator).setTopic(KAFKA_TOPIC_NAME)
            .setMaxMessagePerSecond(1).build();
    QuickStartBase.printStatus(Quickstart.Color.YELLOW,
        "***** Offine data has max time as 16101, realtime will start consuming from time 16102 and increment time "
            + "every 60 events (which is approximately 60 seconds) *****");
  }

  public AirlineDataStream(File baseDir)
      throws Exception {
    this(Schema.fromFile(new File(baseDir, "airlineStats_schema.json")),
        JsonUtils.fileToObject(new File(baseDir, "airlineStats_realtime_table_config.json"), TableConfig.class),
        new File(baseDir, "rawdata/airlineStats_data.avro"), getDefaultKafkaProducer());
  }

  public static StreamDataProducer getDefaultKafkaProducer()
      throws Exception {
    Properties properties = new Properties();
    properties.put("metadata.broker.list", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");

    return StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);
  }

  public void run() {
    _pinotStream.run();
  }

  public void shutdown()
      throws Exception {
    _pinotStream.close();
    _producer.close();
    _producer = null;
  }
}
