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
package org.apache.pinot.tools.admin.command;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.stream.StreamDataProducer;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.utils.KafkaStarterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;


/**
 * Class for command to stream Avro data into Kafka.
 */
@CommandLine.Command(name = "StreamAvroIntoKafka", description =
    "Stream the specified Avro file into a Kafka topic, which can be read by Pinot\n"
    + "by using org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder as the\n"
    + "message decoder class name (stream.kafka.decoder.class.name).", mixinStandardHelpOptions = true)
public class StreamAvroIntoKafkaCommand extends AbstractBaseAdminCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamAvroIntoKafkaCommand.class);
  @CommandLine.Option(names = {"-avroFile"}, required = true, description = "Avro file to stream.")
  private String _avroFile = null;

  @CommandLine.Option(names = {"-kafkaBrokerList"}, required = false, description = "Kafka broker list.")
  private String _kafkaBrokerList = KafkaStarterUtils.DEFAULT_KAFKA_BROKER;

  @CommandLine.Option(names = {"-kafkaTopic"}, required = true, description = "Kafka topic to stream into.")
  private String _kafkaTopic = null;

  @CommandLine.Option(names = {"-zkAddress"}, required = false, description = "Address of Zookeeper.")
  private String _zkAddress = "localhost:2181";

  @CommandLine.Option(names = {"-outputFormat"}, required = false,
      description = "Data format to produce to Kafka, supported: json(default) and avro")
  private String _outputFormat = "json";

  @CommandLine.Option(names = {"-millisBetweenMessages"}, required = false,
      description = "Delay in milliseconds between messages (default 1000 ms)")
  private String _millisBetweenMessages = "1000";

  @Override
  public String getName() {
    return "StreamAvroToKafka";
  }

  @Override
  public String toString() {
    return "StreamAvroInfoKafka -avroFile " + _avroFile + " -kafkaBrokerList " + _kafkaBrokerList + " -kafkaTopic "
        + _kafkaTopic + "-outputFormat" + _outputFormat + " -millisBetweenMessages " + _millisBetweenMessages;
  }

  @Override
  public boolean execute()
      throws IOException {
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

    StreamDataProducer streamDataProducer;
    try {
      streamDataProducer =
          StreamDataProvider.getStreamDataProducer(KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME, properties);
    } catch (Exception e) {
      throw new RuntimeException("Failed to get StreamDataProducer - " + KafkaStarterUtils.KAFKA_PRODUCER_CLASS_NAME,
          e);
    }
    try {
      // Open the Avro file
      DataFileStream<GenericRecord> reader = AvroUtils.getAvroReader(new File(_avroFile));
      DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(reader.getSchema());
      // Iterate over every record
      for (GenericRecord genericRecord : reader) {
        byte[] bytes;
        switch (_outputFormat) {
          case "avro":
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            datumWriter.write(genericRecord, encoder);
            encoder.flush();
            bytes = outputStream.toByteArray();
            break;
          default:
            String recordJson = genericRecord.toString();
            bytes = recordJson.getBytes("utf-8");
            break;
        }
        // Write the message to Kafka
        streamDataProducer.produce(_kafkaTopic, Longs.toByteArray(HashUtil.hash64(bytes, bytes.length)), bytes);

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

  public StreamAvroIntoKafkaCommand setAvroFile(String avroFile) {
    _avroFile = avroFile;
    return this;
  }

  public StreamAvroIntoKafkaCommand setKafkaBrokerList(String kafkaBrokerList) {
    _kafkaBrokerList = kafkaBrokerList;
    return this;
  }

  public StreamAvroIntoKafkaCommand setKafkaTopic(String kafkaTopic) {
    _kafkaTopic = kafkaTopic;
    return this;
  }

  public StreamAvroIntoKafkaCommand setZkAddress(String zkAddress) {
    _zkAddress = zkAddress;
    return this;
  }

  public StreamAvroIntoKafkaCommand setOutputFormat(String outputFormat) {
    _outputFormat = outputFormat;
    return this;
  }
}
