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
package com.linkedin.pinot.tools.admin.command;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.utils.HashUtil;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import com.linkedin.pinot.core.util.AvroUtils;
import com.linkedin.pinot.tools.Command;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class for command to stream Avro data into Kafka.
 */
public class StreamAvroIntoKafkaCommand extends AbstractBaseAdminCommand implements Command {
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

  @Option(name="-millisBetweenMessages", required=false, metaVar="<int>", usage="Delay in milliseconds between messages (default 1000 ms)")
  private String _millisBetweenMessages = "1000";

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
        _kafkaTopic + " -millisBetweenMessages " + _millisBetweenMessages;
  }

  @Override
  public String description() {
    return "Stream the specified Avro file into a Kafka topic, which can be read by Pinot\n" +
        "by using com.linkedin.pinot.core.realtime.impl.kafka.KafkaJSONMessageDecoder as the\n" +
        "message decoder class name (stream.kafka.decoder.class.name).";
  }

  @Override
  public boolean execute() throws IOException {
    int messageDelayMillis = Integer.parseInt(_millisBetweenMessages);
    final boolean sleepRequired = 0 < messageDelayMillis;

    if (sleepRequired) {
      LOGGER.info("Streaming Avro file into Kafka topic {} with {} ms between messages", _kafkaTopic,
          _millisBetweenMessages);
    } else {
      LOGGER.info("Streaming Avro file into Kafka topic {} with no delay between messages", _kafkaTopic);
    }

    // Create Kafka producer
    Properties properties = new Properties();
    properties.put("metadata.broker.list", _kafkaBrokerList);
    properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
    properties.put("request.required.acks", "1");

    ProducerConfig producerConfig = new ProducerConfig(properties);
    Producer<byte[], byte[]> producer = new Producer<byte[], byte[]>(producerConfig);
    try {
      // Open the Avro file
      DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(new File(_avroFile));

      // Iterate over every record
      for (GenericRecord genericRecord : reader) {
        // Write the message to Kafka
        String recordJson = genericRecord.toString();
        byte[] bytes = recordJson.getBytes("utf-8");
        KeyedMessage<byte[], byte[]> data = new KeyedMessage<byte[], byte[]>(_kafkaTopic,
            Longs.toByteArray(HashUtil.hash64(bytes, bytes.length)), bytes);

        producer.send(data);

        // Sleep between messages
        if (sleepRequired) {
          Uninterruptibles.sleepUninterruptibly(messageDelayMillis, TimeUnit.MILLISECONDS);
        }
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
