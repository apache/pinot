package com.linkedin.thirdeye.tools;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class KafkaLoadTool {
  private final String brokerList;
  private final String topic;
  private final File avroFile;

  public KafkaLoadTool(String brokerList, String topic, File avroFile) {
    this.brokerList = brokerList;
    this.topic = topic;
    this.avroFile = avroFile;
  }

  private static class ByteArraySerializer implements Serializer<byte[]> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      // Nop
    }

    @Override
    public byte[] serialize(String topic, byte[] data) {
      return data;
    }

    @Override
    public void close() {
      // Nop
    }
  }

  public void run() throws Exception {
    // Create kafka producer
    Map<String, Object> config = new HashMap<String, Object>();
    config.put("bootstrap.servers", brokerList);
    config.put("acks", "1");
    KafkaProducer<byte[], byte[]> kafkaProducer = new KafkaProducer<byte[], byte[]>(config,
        new ByteArraySerializer(), new ByteArraySerializer());

    // Decoder
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> fileReader =
        new DataFileReader<GenericRecord>(avroFile, datumReader);

    // Encoder
    DatumWriter<GenericRecord> datumWriter =
        new GenericDatumWriter<GenericRecord>(fileReader.getSchema());
    BinaryEncoder encoder = null;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    // Send records
    int recordId = 0;
    GenericRecord record = new GenericData.Record(fileReader.getSchema());
    while (fileReader.hasNext()) {
      // Read next
      fileReader.next(record);

      // Encode
      baos.reset();
      encoder = new BinaryEncoder(baos);
      datumWriter.write(record, encoder);
      encoder.flush();
      byte[] bytes = baos.toByteArray();

      if (recordId % 1000 == 0) {
        System.out.println("Producing record " + recordId);
      }

      // Write
      kafkaProducer.send(
          new ProducerRecord<byte[], byte[]>(topic, String.valueOf(recordId).getBytes(), bytes));

      recordId++;
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("usage: brokerList topic avroFile");
      System.exit(1);
    }

    new KafkaLoadTool(args[0], args[1], new File(args[2])).run();
  }
}
