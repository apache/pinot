package com.linkedin.thirdeye.realtime;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecord;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ThirdEyeKafkaDecoderSchemaRegistryAvroImpl implements ThirdEyeKafkaDecoder
{
  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeKafkaDecoderSchemaRegistryAvroImpl.class);

  private static final String PROP_SCHEMA_REGISTRY_URI = "schema.registry.uri";
  private static final int MD5_LENGTH_BYTES = 16;
  private static final int MAGIC_BYTE_LENGTH = 1;

  private final ConcurrentMap<String, DatumReader<GenericRecord>> readers
          = new ConcurrentHashMap<String, DatumReader<GenericRecord>>();

  private final ThreadLocal<BinaryDecoder> decoderThreadLocal = new ThreadLocal<BinaryDecoder>();

  private ThirdEyeKafkaConfig kafkaConfig;
  private StarTreeConfig starTreeConfig;

  private String schemaRegistryUri;

  @Override
  public void init(StarTreeConfig starTreeConfig, ThirdEyeKafkaConfig kafkaConfig) throws Exception
  {
    this.starTreeConfig = starTreeConfig;
    this.kafkaConfig = kafkaConfig;
    this.schemaRegistryUri = kafkaConfig.getDecoderConfig().getProperty(PROP_SCHEMA_REGISTRY_URI);

    if (schemaRegistryUri == null)
    {
      throw new IllegalArgumentException("Must provide " + PROP_SCHEMA_REGISTRY_URI);
    }
  }

  @Override
  public StarTreeRecord decode(byte[] bytes) throws IOException
  {
    String md5 = hex(Arrays.copyOfRange(bytes, MAGIC_BYTE_LENGTH, MD5_LENGTH_BYTES + MAGIC_BYTE_LENGTH));  // n.b. first byte is assumed magic byte

    // Get reader
    DatumReader<GenericRecord> reader = readers.get(md5);
    if (reader == null)
    {
      InputStream inputStream = URI.create(schemaRegistryUri + "/id=" + md5).toURL().openStream();
      Schema schema = Schema.parse(inputStream);
      inputStream.close();
      reader = new GenericDatumReader<GenericRecord>(schema);
      readers.put(md5, reader);
    }

    // Decode record
    int offset = MAGIC_BYTE_LENGTH + MD5_LENGTH_BYTES;
    int length = bytes.length - offset;
    decoderThreadLocal.set(DecoderFactory.defaultFactory().createBinaryDecoder(bytes, offset, length, decoderThreadLocal.get()));
    GenericRecord record = reader.read(null, decoderThreadLocal.get());

    if (record.getSchema().getName().equals(kafkaConfig.getTopicName()))
    {
      return ThirdEyeAvroUtils.convert(starTreeConfig, record);
    }
    else
    {
      LOGGER.warn("Received event with record name {} not topic name {}, skipping",
               record.getSchema().getName(), kafkaConfig.getTopicName());
      return null;
    }
  }

  public static String hex(byte[] bytes)
  {
    StringBuilder builder = new StringBuilder(2 * bytes.length);
    for (int i = 0; i < bytes.length; i++)
    {
      String hexString = Integer.toHexString(0xFF & bytes[i]);
      if (hexString.length() < 2)
      {
        hexString = "0" + hexString;
      }
      builder.append(hexString);
    }
    return builder.toString();
  }
}
