package com.linkedin.thirdeye.tools;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.lang.RandomStringUtils;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;

public class DataGeneratorWriter {

  DataGeneratorConfig config;
  HashMap<String, ArrayList<String>> dimensionValues;

  private static final Joiner PATH_JOINER = Joiner.on(File.separator);

  public DataGeneratorWriter(DataGeneratorConfig config) {
    this.config = config;
  }

  public void init() {
    createDimensionValuesPool();
    //createMetricValuesPool();
  }

  // Generate a pool of random values according to cardinality for each dimension
  private void createDimensionValuesPool() {
    dimensionValues = new HashMap<String, ArrayList<String>>();
    String str;
    for (int dimensionIdx = 0; dimensionIdx < config.starTreeConfig.getDimensions().size(); dimensionIdx++) {
      Set<String> uniqueValues = new HashSet<String>();
      for (int cardinalityCount = 0; cardinalityCount < config.dimensionCardinality[dimensionIdx]; cardinalityCount++) {
        while (uniqueValues.contains((str = RandomStringUtils.randomAlphabetic(dimensionIdx + 1)))) { ; }
        uniqueValues.add("d"+dimensionIdx+"_"+str);
      }
      dimensionValues.put(config.starTreeConfig.getDimensions().get(dimensionIdx).getName(), new ArrayList<String>(uniqueValues));
    }
  }

  public void generate() throws IOException {

      Schema schemaJson = Schema.parse(config.schemaFile);
      final GenericDatumWriter<GenericData.Record> datum = new GenericDatumWriter<GenericData.Record>(schemaJson);
      DataFileWriter<GenericData.Record> recordWriter = new DataFileWriter<GenericData.Record>(datum);

      Map<String, Double> minMap = new HashMap<String, Double>();
      Map<String, Double> maxMap = new HashMap<String, Double>();
      for (DimensionSpec dimension : config.starTreeConfig.getDimensions()) {
        minMap.put(dimension.getName(), (double) 0);
        maxMap.put(dimension.getName(), (double) (dimensionValues.get(dimension.getName()).size() - 1));
      }

      int recordNo = 0;
      int fileNo = 0;
      for (long time = config.startTime; time <= config.endTime; time++) {
        for (long record = 0; record < config.numRecordsPerTimeUnit; record++) {

          if (recordNo % config.numRecordsPerFile == 0) {
            File avroFile =  new File(PATH_JOINER.join(config.outputDataDirectory, "part-"+fileNo+".avro"));
            if (avroFile.exists())
              avroFile.delete();
            if (fileNo > 0)
              recordWriter.close();
            recordWriter.create(schemaJson, avroFile);
            fileNo ++;
          }

          recordNo++;
          GenericData.Record outRecord = new GenericData.Record(schemaJson);

          // Write time
          outRecord.put(config.starTreeConfig.getTime().getColumnName(), time);

          Random rand = new Random();
          // Write dimensions
          for (DimensionSpec dimensionSpec : config.starTreeConfig.getDimensions()) {
            String dimension = dimensionSpec.getName();
            int cardinality = dimensionValues.get(dimension).size();
            double mean = (cardinality)/2;
            double variance = cardinality/2;
            double gaussianPick = mean + rand.nextGaussian() * variance;

            if (gaussianPick < minMap.get(dimension))
              minMap.put(dimension, gaussianPick);
            else if (gaussianPick > maxMap.get(dimension))
              maxMap.put(dimension, gaussianPick);

            int normalizedValue = (int) ((cardinality - 1)/(maxMap.get(dimension) - minMap.get(dimension))
                *(gaussianPick - maxMap.get(dimension)) + (cardinality - 1));

            outRecord.put(dimension, dimensionValues.get(dimension).get(normalizedValue));
          }

          // Write metrics
          for (MetricSpec metricSpec : config.starTreeConfig.getMetrics()) {
            String metric = metricSpec.getName();
            long metricValue = rand.nextInt(100);
            outRecord.put(metric, metricValue);
          }

          recordWriter.append(outRecord);
        }
      }
      recordWriter.close();
  }
}