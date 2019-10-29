package org.apache.pinot.druid.data.readers;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;


public class TestDruidSegmentRecordReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestDruidSegmentRecordReader.class);

  // TODO: Validate/Test for complex metric column
  // TODO: Validate/Test for multi/single-value mismatchd
  // TODO: Test - If the column does not exist in the segment file, skip it.

  @Test
  public void testDruidSegmentRecordReader() {
    // TODO: make actual tests that follow the conventions of other tests lol
    // Check in with Seunghyun about this lol
    LOGGER.info("hi"); // wait the logger still doesn't work???

    DruidSegmentRecordReader testReader = new DruidSegmentRecordReader();

    String allTypes = "/Users/dadapon/workspace/apache-druid-0.15.1-incubating/var/druid/segment-cache/allTypesData/2011-01-10T00:00:00.000Z_2011-01-17T00:00:00.000Z/2019-10-22T21:47:12.766Z/0";
    String allTypesSchemaExtraColumn = "/Users/dadapon/Desktop/test/all-types-test/all-types-config/all-types-schema-extra-column.json";

    String indexPath = allTypes;
    String schemaPath = allTypesSchemaExtraColumn;
    Schema schema;
    try {
      schema = Schema.fromFile(new File(schemaPath));
    } catch (Exception e) {
      System.out.println("SCHEMA.FROMFILE() FAILED");
      LOGGER.error("Could not make schema from given file: ", e);
      return;
    }
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig();
    segmentGeneratorConfig.setInputFilePath(indexPath);
    segmentGeneratorConfig.setSchema(schema);

    try {
      testReader.init(segmentGeneratorConfig);
    } catch (Exception e) {
      System.out.println("INIT RECORDREADER FAILED");
      LOGGER.error("Failed to initialize DruidSegmentRecordReader: ", e);
      return;
    }

    List<GenericRow> rows = new ArrayList<>();

    try {
      while (testReader.hasNext()) {
        rows.add(testReader.next());
      }
    } catch (Exception e) {
      System.out.println("FAILED CREATING AND ADDING ROW");
      LOGGER.error("Failed creating and adding row", e);
      return;
    }

    for (int i = 0; i < rows.size(); i++) {
      System.out.println(rows.get(i).toString());
    }

  }
}
