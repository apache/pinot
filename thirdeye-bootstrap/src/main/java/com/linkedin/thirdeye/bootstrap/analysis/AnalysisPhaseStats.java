package com.linkedin.thirdeye.bootstrap.analysis;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class AnalysisPhaseStats
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private Long minTime;
  private Long maxTime;
  private String inputPath;

  public AnalysisPhaseStats() { }

  public long getMinTime()
  {
    return minTime;
  }

  public void setMinTime(long minTime)
  {
    this.minTime = minTime;
  }

  public long getMaxTime()
  {
    return maxTime;
  }

  public void setMaxTime(long maxTime)
  {
    this.maxTime = maxTime;
  }

  public String getInputPath()
  {
    return inputPath;
  }

  public void setInputPath(String inputPath)
  {
    this.inputPath = inputPath;
  }

  public void update(AnalysisPhaseStats stats)
  {
    if (minTime == null || stats.getMinTime() < minTime)
    {
      minTime = stats.getMinTime();
    }

    if (maxTime == null || stats.getMaxTime() > maxTime)
    {
      maxTime = stats.getMaxTime();
    }
  }

  public byte[] toBytes() throws IOException
  {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(this);
  }

  public static AnalysisPhaseStats fromBytes(byte[] bytes) throws IOException
  {
    return OBJECT_MAPPER.readValue(bytes, AnalysisPhaseStats.class);
  }
}
