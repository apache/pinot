package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeRecord;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Arrays;

public class TestStarTreeRecordStreamAvroFileImpl
{
  private File avroFile;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    avroFile = new File(System.getProperty("java.io.tmpdir") + File.separator + TestStarTreeRecordStreamAvroFileImpl.class.getSimpleName() + ".avro");
    Schema schema = new Schema.Parser().parse(ClassLoader.getSystemResourceAsStream("MyRecord.avsc"));
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));
    dataFileWriter.create(schema, avroFile);

    // Write some records to the file
    for (int i = 0; i < 100; i++)
    {
      GenericRecord record = new GenericData.Record(schema);
      record.put("A", "A" + (i % 8));
      record.put("B", "B" + (i % 2));
      record.put("C", "C" + (i & 4));
      record.put("M", 1);
      record.put("hoursSinceEpoch", 0L);
      dataFileWriter.append(record);
    }

    dataFileWriter.flush();
    dataFileWriter.close();
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    FileUtils.forceDelete(avroFile);
  }

  @Test
  public void testAvroFileStream() throws Exception
  {
    Number metricSum = 0;

    for (StarTreeRecord record : new StarTreeRecordStreamAvroFileImpl(
            avroFile,
            Arrays.asList(new DimensionSpec("A"), new DimensionSpec("B"), new DimensionSpec("C")),
            Arrays.asList(new MetricSpec("M", MetricType.INT)), "hoursSinceEpoch"))
    {
      metricSum = NumberUtils.sum(metricSum, record.getMetricValues().get("M"), MetricType.INT);
    }

    Assert.assertEquals(metricSum, 100);
  }
}
