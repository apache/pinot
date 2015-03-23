package com.linkedin.thirdeye.realtime;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;

public class ThirdEyeKafkaDecoderAvroImpl implements ThirdEyeKafkaDecoder
{
  public static final String PROP_RECORD_OFFSET = "record.offset";
  public static final String PROP_SCHEMA_URI = "schema.uri";

  public static final int DEFAULT_RECORD_OFFSET = 0;

  private int recordOffset = DEFAULT_RECORD_OFFSET;
  private StarTreeConfig starTreeConfig;
  private ThreadLocal<BinaryDecoder> decoderThreadLocal;
  private DatumReader<GenericRecord> datumReader;

  @Override
  public void init(StarTreeConfig starTreeConfig, ThirdEyeKafkaConfig kafkaConfig) throws Exception
  {
    this.starTreeConfig = starTreeConfig;
    this.decoderThreadLocal = new ThreadLocal<BinaryDecoder>();

    // Get schema
    String schemaUri = kafkaConfig.getDecoderConfig().getProperty(PROP_SCHEMA_URI);
    if (schemaUri == null)
    {
      throw new IllegalStateException("Must provide " + PROP_SCHEMA_URI);
    }
    InputStream schemaInputStream = URI.create(schemaUri).toURL().openStream();
    Schema schema = new Schema.Parser().parse(schemaInputStream);
    schemaInputStream.close();

    // Set decoder
    this.datumReader = new GenericDatumReader<GenericRecord>(schema);

    // Set any record offset
    String recordOffsetProp = kafkaConfig.getDecoderConfig().getProperty(PROP_RECORD_OFFSET);
    if (recordOffsetProp != null)
    {
      this.recordOffset = Integer.valueOf(recordOffsetProp);
    }
  }

  @Override
  public StarTreeRecord decode(byte[] bytes) throws IOException
  {
    if (recordOffset > 0)
    {
      bytes = Arrays.copyOfRange(bytes, recordOffset, bytes.length);
    }
    decoderThreadLocal.set(DecoderFactory.get().binaryDecoder(bytes, decoderThreadLocal.get()));
    GenericRecord record = datumReader.read(null, decoderThreadLocal.get());
    return ThirdEyeAvroUtils.convert(starTreeConfig, record);
  }
}
