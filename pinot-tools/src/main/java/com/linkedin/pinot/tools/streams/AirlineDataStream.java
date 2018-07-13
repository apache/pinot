/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.tools.streams;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.tools.Quickstart;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AirlineDataStream {
  private static final Logger logger = LoggerFactory.getLogger(AirlineDataStream.class);

  Schema pinotSchema;
  File avroFile;
  DataFileStream<GenericRecord> avroDataStream;
  Integer currentTimeValue = 16102;
  boolean keepIndexing = true;
  private Producer<String, byte[]> producer;
  ExecutorService service;
  int counter = 0;

  public AirlineDataStream(Schema pinotSchema, File avroFile) throws FileNotFoundException, IOException {
    this.pinotSchema = pinotSchema;
    this.avroFile = avroFile;
    createStream();
    Properties properties = new Properties();
    properties.put("metadata.broker.list", KafkaStarterUtils.DEFAULT_KAFKA_BROKER);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");

    ProducerConfig producerConfig = new ProducerConfig(properties);
    producer = new Producer<String, byte[]>(producerConfig);
    service = Executors.newFixedThreadPool(1);
    Quickstart.printStatus(Quickstart.Color.YELLOW,
        "***** Offine data has max time as 16101, realtime will start consuming from time 16102 and increment time every 3000 events *****");
  }

  public void shutdown() {
    keepIndexing = false;
    avroDataStream = null;
    producer.close();
    producer = null;
    service.shutdown();
  }

  private void createStream() throws FileNotFoundException, IOException {
    if (keepIndexing) {
      avroDataStream =
          new DataFileStream<GenericRecord>(new FileInputStream(avroFile), new GenericDatumReader<GenericRecord>());
      return;
    }
    avroDataStream = null;
  }

  private void publish(JSONObject message) throws JSONException, IOException {
    if (!keepIndexing) {
      avroDataStream.close();
      avroDataStream = null;
      return;
    }
    KeyedMessage<String, byte[]> data =
        new KeyedMessage<String, byte[]>("airlineStatsEvents", message.toString().getBytes("UTF-8"));
    producer.send(data);
  }

  public void run() throws IOException, JSONException {

    service.submit(new Runnable() {

      @Override
      public void run() {
        while (true) {
          while (avroDataStream.hasNext()) {
            if (keepIndexing == false) {
              return;
            }

            GenericRecord record = avroDataStream.next();
            JSONObject message = new JSONObject();

            for (FieldSpec spec : pinotSchema.getDimensionFieldSpecs()) {
              try {
                message.put(spec.getName(), record.get(spec.getName()));
              } catch (JSONException e) {
                logger.error(e.getMessage());
              }
            }

            for (FieldSpec spec : pinotSchema.getDimensionFieldSpecs()) {
              try {
                message.put(spec.getName(), record.get(spec.getName()));
              } catch (JSONException e) {
                logger.error(e.getMessage());
              }
            }

            TimeFieldSpec spec = pinotSchema.getTimeFieldSpec();

            String timeColumn = spec.getIncomingTimeColumnName();
            try {
              message.put(timeColumn, currentTimeValue);
            } catch (JSONException e) {
              logger.error(e.getMessage());
            }
            try {
              publish(message);
              counter++;
              if (counter % 3000 == 0) {
                currentTimeValue = currentTimeValue + 1;
              }
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
