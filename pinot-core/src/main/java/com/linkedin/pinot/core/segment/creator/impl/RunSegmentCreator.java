package com.linkedin.pinot.core.segment.creator.impl;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import java.io.File;


/**
 * FIXME Document me!
 *
 * @author jfim
 */
public class RunSegmentCreator {
  public static void main(String[] args) throws Exception {
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    Schema schema = AvroUtils.extractSchemaFromAvro(new File("/Users/jfim/test/input/part-00005.avro"));

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setInputFileFormat(FileFormat.AVRO);
    config.setTableName("blah");
    config.setInputFilePath("/Users/jfim/test/input/part-00005.avro");
    config.setIndexOutputDir("/Users/jfim/test/output");
    driver.init(config);
    driver.build();
  }
}