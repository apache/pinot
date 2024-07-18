package org.apache.pinot.segment.local.segment.creator;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class CLPDictionariesTest {

  private static final String AVRO_DATA = "data/test_sample_data.avro";
  private static final File INDEX_DIR = new File(DictionariesTest.class.toString());
  private static final Map<String, Set<Object>> UNIQUE_ENTRIES = new HashMap<>();

  private static File _segmentDirectory;

  private static TableConfig _tableConfig;

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public static void before()
      throws Exception {
    final String filePath =
        TestUtils.getFileFromResourceUrl(DictionariesTest.class.getClassLoader().getResource(AVRO_DATA));
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "time_day",
            TimeUnit.DAYS, "test");
    _tableConfig = config.getTableConfig();
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    _segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
    final Schema schema = AvroUtils.getPinotSchemaFromAvroDataFile(new File(filePath));

    final DataFileStream<GenericRecord> avroReader = AvroUtils.getAvroReader(new File(filePath));
    final org.apache.avro.Schema avroSchema = avroReader.getSchema();
    final String[] columns = new String[avroSchema.getFields().size()];
    int i = 0;
    for (final org.apache.avro.Schema.Field f : avroSchema.getFields()) {
      columns[i] = f.name();
      i++;
    }

    for (final String column : columns) {
      UNIQUE_ENTRIES.put(column, new HashSet<>());
    }

    while (avroReader.hasNext()) {
      final GenericRecord rec = avroReader.next();
      for (final String column : columns) {
        Object val = rec.get(column);
        if (val instanceof Utf8) {
          val = ((Utf8) val).toString();
        }
        UNIQUE_ENTRIES.get(column).add(val);
      }
    }
  }

  @Test
  public void clpStatsCollectorTest() {
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec("column1", FieldSpec.DataType.STRING, true));
    List<FieldConfig> fieldConfigList = new ArrayList<>();
    fieldConfigList.add(new FieldConfig("column1", FieldConfig.EncodingType.RAW, Collections.EMPTY_LIST,
        FieldConfig.CompressionCodec.CLP, Collections.EMPTY_MAP));
    _tableConfig.setFieldConfigList(fieldConfigList);
    StatsCollectorConfig statsCollectorConfig = new StatsCollectorConfig(_tableConfig, schema, null);
    StringColumnPreIndexStatsCollector statsCollector =
        new StringColumnPreIndexStatsCollector("column1", statsCollectorConfig);

    List<String> logLines = new ArrayList<>();
    logLines.add(
        "2023/10/26 00:03:10.168 INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32c_DEFAULT)] "
            + "Event pinot::DEFAULT::4a02a32c_DEFAULT : Refreshed 35 property LiveInstance took 5 ms. Selective: true");
    logLines.add(
        "2023/10/26 00:03:10.169 INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32d_DEFAULT)] "
            + "Event pinot::DEFAULT::4a02a32d_DEFAULT : Refreshed 81 property LiveInstance took 4 ms. Selective: true");

    for (String logLine : logLines) {
      statsCollector.collect(logLine);
    }
    statsCollector.seal();

    Assert.assertNotNull(statsCollector.getCLPStats());

    // Same log line format
    Assert.assertEquals(statsCollector.getCLPStats().getSortedLogTypeValues().length, 1);
  }
}
