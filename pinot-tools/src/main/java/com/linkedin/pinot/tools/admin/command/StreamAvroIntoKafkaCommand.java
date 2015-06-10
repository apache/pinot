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
package com.linkedin.pinot.tools.admin.command;

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


/**
 * Class for command to stream Avro data into Kafka.
 */
public class StreamAvroIntoKafkaCommand extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamAvroIntoKafkaCommand.class);

  @Option(name="-avroFile", required=true, metaVar="<String>", usage="Avro file to stream.")
  private String _avroFile = null;

  @Option(name="-help", required=false, help=true, aliases={"-h", "--h", "--help"}, usage="Print this message.")
  private boolean _help = false;

  @Option(name="-kafkaBrokerList", required=false, metaVar="<String>", usage="Kafka broker list.")
  private String _kafkaBrokerList = KafkaStarterUtils.DEFAULT_KAFKA_BROKER;

  @Option(name="-kafkaTopic", required=true, metaVar="<String>", usage="Kafka topic to stream into.")
  private String _kafkaTopic = null;

  @Option(name="-zkAddress", required=false, metaVar="<string>", usage="Address of Zookeeper.")
  private String _zkAddress = "localhost:2181";

  @Override
  public boolean getHelp() {
    return _help;
  }

  @Override
  public String getName() {
    return "StreamAvroToKafka";
  }

  @Override
  public String toString() {
    return "StreamAvroInfoKafka -avroFile " + _avroFile + " -kafkaBrokerList " + _kafkaBrokerList + " -kafkaTopic " +
        _kafkaTopic;
  }

  @Override
  public String description() {
    return "Stream the specified Avro file into a Kafka topic, which can be read by Pinot\n" +
        "by using com.linkedin.pinot.core.realtime.impl.kafka.KafkaJSONMessageDecoder as the\n" +
        "message decoder class name (stream.kafka.decoder.class.name).";
  }

  @Override
  public boolean execute() throws IOException {
    // Create Kafka producer
    Properties properties = new Properties();
    properties.put("metadata.broker.list", _kafkaBrokerList);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");

    ProducerConfig producerConfig = new ProducerConfig(properties);
    Producer<String, byte[]> producer = new Producer<String, byte[]>(producerConfig);
    try {
      // Open the Avro file
      DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(new File(_avroFile));

      // Iterate over every record
      for (GenericRecord genericRecord : reader) {
        // Write the message to Kafka
        String recordJson = genericRecord.toString();
        byte[] bytes = recordJson.getBytes("utf-8");
        KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(_kafkaTopic, bytes);

        producer.send(data);

        // Sleep for one second
        Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);
      }

      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }

    savePID(System.getProperty("java.io.tmpdir") + File.separator + ".streamAvro.pid");
    return true;
  }
}
