package org.apache.pinot.segment.local.indexsegment.mutable;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexPlugin;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.mutable.MutableForwardIndex;
import org.apache.pinot.segment.spi.index.mutable.MutableIndex;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MutableSegmentEntriesAboveThresholdTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), MutableSegmentEntriesAboveThresholdTest.class.getSimpleName());
  private static final String AVRO_FILE = "data/test_data-mv.avro";
  private Schema _schema;

  private static class FakeMutableForwardIndex implements MutableForwardIndex {

    private final MutableForwardIndex _mutableForwardIndex;
    private static final int threshold = 2;
    private int _numValues;

    FakeMutableForwardIndex(MutableForwardIndex mutableForwardIndex) {
      this._mutableForwardIndex = mutableForwardIndex;
      this._numValues = 0;
    }

    @Override
    public boolean canAddMore() {
      return _numValues < threshold;
    }

    @Override
    public void setDictIdMV(int docId, int[] dictIds) {
      _numValues += dictIds.length;
      this._mutableForwardIndex.setDictIdMV(docId, dictIds);
    }

    @Override
    public int getLengthOfShortestElement() {
      return _mutableForwardIndex.getLengthOfShortestElement();
    }

    @Override
    public int getLengthOfLongestElement() {
      return _mutableForwardIndex.getLengthOfLongestElement();
    }

    @Override
    public void setDictId(int docId, int dictId) {
      this._mutableForwardIndex.setDictId(docId, dictId);
    }

    @Override
    public boolean isDictionaryEncoded() {
      return _mutableForwardIndex.isDictionaryEncoded();
    }

    @Override
    public boolean isSingleValue() {
      return _mutableForwardIndex.isSingleValue();
    }

    @Override
    public FieldSpec.DataType getStoredType() {
      return _mutableForwardIndex.getStoredType();
    }

    @Override
    public void close()
        throws IOException {
      _mutableForwardIndex.close();
    }
  }

  private File getAvroFile() {
    URL resourceUrl = MutableSegmentImplTest.class.getClassLoader().getResource(AVRO_FILE);
    Assert.assertNotNull(resourceUrl);
    return new File(resourceUrl.getFile());
  }

  private MutableSegmentImpl getMutableSegment(File avroFile)
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    MutableSegmentImpl _mutableSegmentImpl;

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
    return _mutableSegmentImpl;
  }

  @Test
  public void testNoLimitBreached()
      throws Exception {
    File avroFile = getAvroFile();
    MutableSegmentImpl mutableSegment = getMutableSegment(avroFile);
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(System.currentTimeMillis(), new GenericRow());
    try (RecordReader recordReader = RecordReaderFactory
        .getRecordReader(FileFormat.AVRO, avroFile, _schema.getColumnNames(), null)) {
      GenericRow reuse = new GenericRow();
      while (recordReader.hasNext()) {
        mutableSegment.index(recordReader.next(reuse), defaultMetadata);
      }
    }
    assert !mutableSegment.isIndexCapacityThresholdBreached();
  }

  @Test
  public void testLimitBreached()
      throws Exception {
    File avroFile = getAvroFile();
    MutableSegmentImpl mutableSegment = getMutableSegment(avroFile);

    Field indexContainerMapField = MutableSegmentImpl.class.getDeclaredField("_indexContainerMap");
    indexContainerMapField.setAccessible(true);
    Map<String, Object> colVsIndexContainer = (Map<String, Object>) indexContainerMapField.get(mutableSegment);

    for (Map.Entry<String, Object> entry : colVsIndexContainer.entrySet()) {
      Object indexContainer = entry.getValue();
      Field mutableIndexesField = indexContainer.getClass().getDeclaredField("_mutableIndexes");
      mutableIndexesField.setAccessible(true);
      Map<IndexType, MutableIndex> indexTypeVsMutableIndex =
          (Map<IndexType, MutableIndex>) mutableIndexesField.get(indexContainer);

      MutableForwardIndex mutableForwardIndex = null;
      for (IndexType indexType : indexTypeVsMutableIndex.keySet()) {
        if (indexType.getId().equals(StandardIndexes.FORWARD_ID)) {
          mutableForwardIndex = (MutableForwardIndex) indexTypeVsMutableIndex.get(indexType);
        }
      }

      assert mutableForwardIndex != null;

      indexTypeVsMutableIndex.put(new ForwardIndexPlugin().getIndexType(),
          new FakeMutableForwardIndex(mutableForwardIndex));
    }
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(System.currentTimeMillis(), new GenericRow());
    try (RecordReader recordReader = RecordReaderFactory
        .getRecordReader(FileFormat.AVRO, avroFile, _schema.getColumnNames(), null)) {
      GenericRow reuse = new GenericRow();
      while (recordReader.hasNext()) {
        mutableSegment.index(recordReader.next(reuse), defaultMetadata);
      }
    }

    assert mutableSegment.isIndexCapacityThresholdBreached();
  }
}
