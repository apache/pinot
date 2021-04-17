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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.tools.Quickstart;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is used in Hybrid Quickstart.
 */
public class AirlineDataStream {
  private static final Logger logger = LoggerFactory.getLogger(AirlineDataStream.class);

  Schema pinotSchema;
  String timeColumnName;
  File avroFile;
  DataFileStream<GenericRecord> avroDataStream;
  Integer currentTimeValue = 16102;
  boolean keepIndexing = true;
  ExecutorService service;
  int counter = 0;
  private StreamDataProducer producer;

  public AirlineDataStream(Schema pinotSchema, TableConfig tableConfig, File avroFile) throws Exception {
    this.pinotSchema = pinotSchema;
    this.timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
    this.avroFile = avroFile;
    createStream();
    Properties properties = new Properties();
    properties.put("metadata.broker.list", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");

    producer = StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);

    service = Executors.newFixedThreadPool(1);
    Quickstart.printStatus(Quickstart.Color.YELLOW,
        "***** Offine data has max time as 16101, realtime will start consuming from time 16102 and increment time every 60 events (which is approximately 60 seconds) *****");
  }

  public void shutdown() {
    keepIndexing = false;
    avroDataStream = null;
    producer.close();
    producer = null;
    service.shutdown();
  }

  private void createStream() throws IOException {
    if (keepIndexing) {
      avroDataStream = new DataFileStream<>(new FileInputStream(avroFile), new GenericDatumReader<>());
      return;
    }
    avroDataStream = null;
  }

  private void publish(GenericRecord message) throws IOException {
    if (!keepIndexing) {
      avroDataStream.close();
      avroDataStream = null;
      return;
    }
    producer.produce("flights-realtime", message.toString().getBytes("UTF-8"));
  }

  public void run() {

    service.submit(new Runnable() {

      @Override
      public void run() {
        while (true) {
          while (avroDataStream.hasNext()) {
            if (!keepIndexing) {
              return;
            }

            GenericRecord record = avroDataStream.next();

            GenericRecord message = new GenericData.Record(AvroUtils.getAvroSchemaFromPinotSchema(pinotSchema));

            for (FieldSpec spec : pinotSchema.getDimensionFieldSpecs()) {
              message.put(spec.getName(), record.get(spec.getName()));
            }

            for (FieldSpec spec : pinotSchema.getMetricFieldSpecs()) {
              message.put(spec.getName(), record.get(spec.getName()));
            }

            message.put(timeColumnName, currentTimeValue);

            try {
              publish(message);
              counter++;
              if (counter % 60 == 0) {
                currentTimeValue = currentTimeValue + 1;
              }
              Thread.sleep(1000);
            } catch (Exception e) {
              logger.error(e.getMessage());
            }
          }

          try {
            avroDataStream.close();
          } catch (IOException e) {
            logger.error(e.getMessage());
          }

          try {
            createStream();
          } catch (IOException e) {
            logger.error(e.getMessage());
          }
        }
      }
    });
  }
}
