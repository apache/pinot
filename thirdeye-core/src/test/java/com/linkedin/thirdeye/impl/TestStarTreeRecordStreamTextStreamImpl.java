package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeRecord;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.Arrays;

public class TestStarTreeRecordStreamTextStreamImpl
{
  private File recordFile;

  @BeforeClass
  public void beforeClass() throws Exception
  {
    recordFile = new File(System.getProperty("java.io.tmpdir"), TestStarTreeRecordStreamTextStreamImpl.class.getSimpleName() + ".tsv");

    BufferedWriter writer = new BufferedWriter(new FileWriter(recordFile));
    for (int i = 0; i < 100; i++)
    {
      StringBuilder sb = new StringBuilder();
      sb.append("A").append(i).append("\t")
        .append("B").append(i).append("\t")
        .append("C").append(i).append("\t")
        .append("1").append("\t")
        .append("0");
      writer.write(sb.toString());
      writer.newLine();
    }
    writer.flush();
  }

  @AfterClass
  public void afterClass() throws Exception
  {
    FileUtils.forceDelete(recordFile);
  }

  @Test
  public void testFileStream() throws Exception
  {
    StarTreeRecordStreamTextStreamImpl starTreeRecords
            = new StarTreeRecordStreamTextStreamImpl(new FileInputStream(recordFile), Arrays.asList("A", "B", "C"), Arrays.asList("M"), "\t");

    int idx = 0;
    for (StarTreeRecord starTreeRecord : starTreeRecords)
    {
      Assert.assertEquals(starTreeRecord.getDimensionValues().get("A"), "A" + idx);
      Assert.assertEquals(starTreeRecord.getDimensionValues().get("B"), "B" + idx);
      Assert.assertEquals(starTreeRecord.getDimensionValues().get("C"), "C" + idx);
      Assert.assertEquals(starTreeRecord.getMetricValues().get("M").intValue(), 1);
      Assert.assertEquals(starTreeRecord.getTime().intValue(), 0);
      idx++;
    }
  }
}
