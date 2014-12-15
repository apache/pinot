package com.linkedin.pinot.integration.tests.helpers;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.json.JSONException;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.FieldSpec.FieldType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.FileFormat;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 12, 2014
 */

public class DataGenerator {
  private static final Logger logger = Logger.getLogger(DataGenerator.class);
  private File outDir;

  DataGeneratorSpec genSpec;

  private final Map<String, Generator> generators;

  public DataGenerator() {
    generators = new HashMap<String, Generator>();
  }

  public void init(DataGeneratorSpec spec) throws IOException {
    genSpec = spec;
    outDir = new File(genSpec.getOutputDir());
    if (outDir.exists() && !genSpec.isOverrideOutDir()) {
      logger.error("output directory already exists, and override is set to false");
      throw new RuntimeException("output directory exists");
    }

    if (outDir.exists()) {
      FileUtils.deleteDirectory(outDir);
    }

    outDir.mkdir();

    for (final String column : genSpec.getColumns()) {
      if (!genSpec.getCardinalityMap().containsKey(column)) {
        logger.error("cardinality for this column does not exist : " + column);
        throw new RuntimeException("cardinality for this column does not exist");
      }

      generators.put(column,
          GeneratorFactory.getGeneratorFor(genSpec.getDataTypesMap().get(column), genSpec.getCardinalityMap().get(column)));
      generators.get(column).init();
    }
  }

  public void generate(long totalDocs, int numFiles) throws IOException, JSONException {
    final int numPerFiles = (int) (totalDocs / numFiles);
    for (int i = 0; i < numFiles; i++) {
      final AvroWriter writer = new AvroWriter(outDir, i, generators, fetchSchema());

      for (int j = 0; j < numPerFiles; j++) {
        writer.writeNext();
      }
      writer.seal();
    }
  }

  public Schema fetchSchema() {
    final Schema schema = new Schema();
    for (final String column : genSpec.getColumns()) {
      final FieldSpec spec = new FieldSpec();
      spec.setDataType(genSpec.getDataTypesMap().get(column));
      spec.setFieldType(FieldType.dimension);
      spec.setName(column);
      spec.setSingleValueField(true);
      schema.addSchema(column, spec);
    }
    return schema;
  }

  public static void main(String[] args) throws IOException, JSONException {
    final String[] columns = { "column1", "column2", "column3", "column4", "column5" };
    final Map<String, DataType> dataTypes = new HashMap<String, FieldSpec.DataType>();
    final Map<String, Integer> cardinality = new HashMap<String, Integer>();
    for (final String col : columns) {
      dataTypes.put(col, DataType.INT);
      cardinality.put(col, 1000);
    }
    final DataGeneratorSpec spec = new DataGeneratorSpec(Arrays.asList(columns), cardinality, dataTypes, FileFormat.avro, "/tmp/out", true);
    final DataGenerator gen = new DataGenerator();
    gen.init(spec);
    gen.generate(1000000L, 2);
  }
}
