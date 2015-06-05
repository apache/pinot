package com.linkedin.thirdeye.tools;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

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

   File starTreeConfigFile;
   File schemaFile;
   File outputDataDirectory;

   List<String> dimensions;
   List<String> metrics;
   String timeColumn;
   String timeSize;
   String timeUnit;
   int[] dimensionCardinality;

  public DataGeneratorConfig() {
    dimensions = new ArrayList<String>();
    metrics = new ArrayList<String>();
  }

  public void init() throws Exception {
    readConfig();
    setOptions();
  }

  private void readConfig() throws Exception
  {
    Scanner configScanner = new Scanner(starTreeConfigFile);
    String line;

    int expectedTokens = 3;
    if (configScanner.hasNext()) {
        line = configScanner.nextLine();
    } else {
      configScanner.close();
      throw new Exception();
    }

    while (expectedTokens != 0) {
      if (line.startsWith("dimensions:")) {
        while (!(line = configScanner.nextLine()).isEmpty()) {
          if (line.trim().startsWith("-"))
              dimensions.add(line.split(":")[1].trim());
        }
        expectedTokens--;
      } else if (line.startsWith("metrics:")) {
        while (!(line = configScanner.nextLine()).isEmpty()) {
          if (line.trim().startsWith("-"))
            metrics.add(line.split(":")[1].trim());
        }
        expectedTokens--;
      } else if (line.startsWith("time:")) {
        while (!(line = configScanner.nextLine().trim()).startsWith("columnName:")) {;}
        timeColumn = (line.split(":")[1]).trim();
        while (!(line = configScanner.nextLine().trim()).startsWith("input:")) {;}
        while (!(line = configScanner.nextLine().trim()).startsWith("size:")) {;}
        timeSize = (line.split(":")[1]).trim();
        while (!(line = configScanner.nextLine().trim()).startsWith("unit:")) {;}
        timeUnit = (line.split(":")[1]).trim();
        expectedTokens--;
      } else {
        line = configScanner.nextLine();
      }
    }
    configScanner.close();
  }

  private void setOptions()
  {
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
    TimeUnit inputTimeUnit = TimeUnit.valueOf(timeUnit);
        startTime = inputTimeUnit.convert(startTime, TimeUnit.MILLISECONDS) / Integer.parseInt(timeSize);
        endTime = inputTimeUnit.convert(endTime, TimeUnit.MILLISECONDS) / Integer.parseInt(timeSize);

    // Default to random cardinalities between 2-50
    dimensionCardinality = new int[dimensions.size()];
    Random r = new Random();
    if (cardinality == null) {
      for (int i = 0; i < dimensions.size(); i++)
        dimensionCardinality[i] = r.nextInt(50) + 2;
    } else {
      String[] tokens = cardinality.split(",");
      for (int i = 0; i < tokens.length; i++)
        dimensionCardinality[i] = Integer.parseInt(tokens[i]);
      for (int i = tokens.length; i < dimensions.size(); i++)
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

  public void setMinTime(String minTime) {
    this.minTime = minTime;
  }

  public void setMaxTime(String maxTime) {
    this.maxTime = maxTime;
  }

  public void setCardinality(String cardinality) {
    this.cardinality = cardinality;
  }

  public void setNumRecords(long numRecords) {
    this.numRecords = numRecords;
  }

  public void setNumFiles(int numFiles) {
    this.numFiles = numFiles;
  }

  public void setStarTreeConfigFile(File starTreeConfigFile) {
    this.starTreeConfigFile = starTreeConfigFile;
  }

  public void setSchemaFile(File schemaFile) {
    this.schemaFile = schemaFile;
  }

  public void setOutputDataDirectory(File outputDataDirectory) {
    this.outputDataDirectory = outputDataDirectory;
  }
}
