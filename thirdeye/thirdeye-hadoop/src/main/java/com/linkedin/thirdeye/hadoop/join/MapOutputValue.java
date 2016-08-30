package com.linkedin.thirdeye.hadoop.join;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

public class MapOutputValue {

  private static BinaryDecoder binaryDecoder;
  private String schemaName;
  private GenericRecord record;
  private GenericDatumWriter<GenericRecord> WRITER;
  private EncoderFactory factory = EncoderFactory.get();

  private BinaryEncoder binaryEncoder;

  public MapOutputValue(String schemaName, GenericRecord record) {
    this.schemaName = schemaName;
    this.record = record;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public GenericRecord getRecord() {
    return record;
  }

  public byte[] toBytes() throws IOException {
    ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
    Schema schema = record.getSchema();
    if (WRITER == null) {
      WRITER = new GenericDatumWriter<GenericRecord>(schema);
    }
    binaryEncoder = factory.directBinaryEncoder(dataStream, binaryEncoder);
    WRITER.write(record, binaryEncoder);

    // serialize to bytes, we also need to know the schema name when we
    // process this record on the reducer since reducer gets the record from
    // multiple mappers. So we first write the schema/source name and then
    // write the serialized bytes
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(out);
    dos.writeInt(schema.getName().getBytes().length);
    dos.write(schema.getName().getBytes());
    byte[] dataBytes = dataStream.toByteArray();

    dos.writeInt(dataBytes.length);
    dos.write(dataBytes);
    return out.toByteArray();
  }

  public static MapOutputValue fromBytes(byte[] bytes, Map<String, Schema> schemaMap)
      throws IOException {
    DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
    int length = dataInputStream.readInt();
    byte[] sourceNameBytes = new byte[length];
    dataInputStream.read(sourceNameBytes);
    String schemaName = new String(sourceNameBytes);

    int recordDataLength = dataInputStream.readInt();

    byte[] recordBytes = new byte[recordDataLength];
    dataInputStream.read(recordBytes);
    Schema schema = schemaMap.get(schemaName);
    GenericRecord record = new GenericData.Record(schema);
    binaryDecoder = DecoderFactory.get().binaryDecoder(recordBytes, binaryDecoder);
    GenericDatumReader<GenericRecord> gdr = new GenericDatumReader<GenericRecord>(schema);
    gdr.read(record, binaryDecoder);
    return new MapOutputValue(schemaName, record);
  }

}
