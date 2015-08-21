package com.linkedin.thirdeye.tools;

import java.io.File;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.linkedin.thirdeye.api.StarTreeConfig;

public class DataGeneratorConfig {

   String minTime;
   String maxTime;
   long startTime;
   long endTime;

   String cardinality;
   long numRecords;
   int numFiles;
   long numRecordsPerFile;
   int numRecordsPerTimeUnit;
   int[] dimensionCardinality;

   StarTreeConfig starTreeConfig;
   File schemaFile;
   File outputDataDirectory;

  public void setOptions() {
    // Default to a year
    if (minTime == null || maxTime == null) {
      DateTime d = new DateTime();
      endTime = d.getMillis()  ;
      startTime = d.minusYears(1).getMillis();
    } else {
       DateTimeFormatter parser = ISODateTimeFormat.dateHourMinuteSecond();
       startTime = parser.parseDateTime(minTime).getMillis();
       endTime = parser.parseDateTime(maxTime).getMillis();
    }
    TimeUnit inputTimeUnit = starTreeConfig.getTime().getBucket().getUnit();
    int inputTimeBucketSize = starTreeConfig.getTime().getBucket().getSize();
    startTime = inputTimeUnit.convert(startTime, TimeUnit.MILLISECONDS) / inputTimeBucketSize;
    endTime = inputTimeUnit.convert(endTime, TimeUnit.MILLISECONDS) / inputTimeBucketSize;

    // Default to random cardinalities between 2-50
    dimensionCardinality = new int[starTreeConfig.getDimensions().size()];
    Random r = new Random();
    if (cardinality == null) {
      for (int i = 0; i < starTreeConfig.getDimensions().size(); i++)
        dimensionCardinality[i] = r.nextInt(50) + 2;
    } else {
      String[] tokens = cardinality.split(",");
      for (int i = 0; i < tokens.length; i++)
        dimensionCardinality[i] = Integer.parseInt(tokens[i]);
      for (int i = tokens.length; i < starTreeConfig.getDimensions().size(); i++)
        dimensionCardinality[i] = r.nextInt(50) + 2;
    }

    // Default to 100 records per time unit
    if (numRecords == 0) {
      numRecordsPerTimeUnit = 100;
      numRecords = (endTime - startTime + 1)*numRecordsPerTimeUnit;
    } else {
      if (numRecords > (endTime - startTime + 1))
        numRecordsPerTimeUnit = (int) (numRecords / (endTime - startTime + 1)) ;
      else
        numRecordsPerTimeUnit = 1;
    }

    // Default to 1 output avro file
    if (numFiles == 0) {
      numFiles = 1;
    }
    numRecordsPerFile = numRecords / numFiles;
  }

  public String getMinTime() {
    return minTime;
  }

  public void setMinTime(String minTime) {
    this.minTime = minTime;
  }

  public String getMaxTime() {
    return maxTime;
  }

  public void setMaxTime(String maxTime) {
    this.maxTime = maxTime;
  }

  public String getCardinality() {
    return cardinality;
  }

  public void setCardinality(String cardinality) {
    this.cardinality = cardinality;
  }

  public long getNumRecords() {
    return numRecords;
  }

  public void setNumRecords(long numRecords) {
    this.numRecords = numRecords;
  }

  public int getNumFiles() {
    return numFiles;
  }

  public void setNumFiles(int numFiles) {
    this.numFiles = numFiles;
  }

  public StarTreeConfig getStarTreeConfig() {
    return starTreeConfig;
  }

  public void setStarTreeConfig(StarTreeConfig starTreeConfig) {
    this.starTreeConfig = starTreeConfig;
  }

  public File getSchemaFile() {
    return schemaFile;
  }

  public void setSchemaFile(File schemaFile) {
    this.schemaFile = schemaFile;
  }

  public File getOutputDataDirectory() {
    return outputDataDirectory;
  }

  public void setOutputDataDirectory(File outputDataDirectory) {
    this.outputDataDirectory = outputDataDirectory;
  }




}
