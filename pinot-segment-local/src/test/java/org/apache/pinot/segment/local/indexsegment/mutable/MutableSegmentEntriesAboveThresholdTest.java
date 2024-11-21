package org.apache.pinot.segment.local.indexsegment.mutable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class MutableSegmentEntriesAboveThresholdTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), MutableSegmentEntriesAboveThresholdTest.class.getSimpleName());
  private static final String AVRO_FILE = "data/test_data-mv.avro";
  private Schema _schema;

  @Test
  public void testIfLimitBreached()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    MutableSegmentImpl _mutableSegmentImpl;

    URL resourceUrl = MutableSegmentImplTest.class.getClassLoader().getResource(AVRO_FILE);
    Assert.assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());

    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(avroFile, TEMP_DIR, "testTable");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    _schema = config.getSchema();
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(_schema, "testSegment");
    _mutableSegmentImpl = MutableSegmentImplTestUtils
        .createMutableSegmentImpl(_schema, Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
            Collections.emptyMap(),
            false, false, null, null, null, null, null, null, Collections.emptyList(), true);
    var _lastIngestionTimeMs = System.currentTimeMillis();
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(_lastIngestionTimeMs, new GenericRow());

    try (RecordReader recordReader = RecordReaderFactory
        .getRecordReader(FileFormat.AVRO, avroFile, _schema.getColumnNames(), null)) {
      GenericRow reuse = new GenericRow();
      while (recordReader.hasNext()) {
        _mutableSegmentImpl.index(recordReader.next(reuse), defaultMetadata);
      }
    }

    assert !_mutableSegmentImpl.isNumOfColValuesAboveThreshold();

  }
}
