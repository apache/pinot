package com.linkedin.pinot.core.chunk.creator.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.chunk.creator.ChunkIndexCreationDriver;
import com.linkedin.pinot.core.chunk.index.ChunkColumnMetadata;
import com.linkedin.pinot.core.chunk.index.ColumnarChunkMetadata;
import com.linkedin.pinot.core.chunk.index.data.source.ChunkColumnarDataSource;
import com.linkedin.pinot.core.chunk.index.loader.Loaders;
import com.linkedin.pinot.core.chunk.index.readers.DictionaryReader;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.Predicate.Type;
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.BitmapInvertedIndex;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.time.SegmentTimeUnit;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 7, 2014
 */

public class TestChunkIndexCreationDriverImpl {

  @Test
  public void test1() throws Exception {
    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(
            "/home/dpatel/experiments/github/pinot/pinot-core/src/test/resources/data/mirror-mv.avro"), new File("/tmp/mirrorTwoDotO"),
            "daysSinceEpoch", SegmentTimeUnit.days, "mirror", "mirror");
    config.setSegmentNamePostfix("1");
    final ChunkIndexCreationDriver driver = new ChunkIndexCreationDriverImpl();
    driver.init(config);

    driver.build();
  }

  @Test
  public void test2() throws ConfigurationException, InterruptedException {
    final ColumnarChunkMetadata metadata =
        new ColumnarChunkMetadata(new File("/tmp/mirrorTwoDotO/mirror_mirror_16381_16381_1", V1Constants.MetadataKeys.METADATA_FILE_NAME));

    final Map<String, DictionaryReader> dictionaryReaders = new HashMap<String, DictionaryReader>();
    final Map<String, ChunkColumnMetadata> metadataMap = new HashMap<String, ChunkColumnMetadata>();
    final Map<String, BitmapInvertedIndex> invertedIndexMap = new HashMap<String, BitmapInvertedIndex>();

    final Map<String, DataFileReader> fwdIndexReadersMap = new HashMap<String, DataFileReader>();
    for (final String column : metadata.getAllColumns()) {
      metadataMap.put(column, metadata.getColumnMetadataFor(column));
    }
    int total = 0;
    try {
      for (final String column : metadataMap.keySet()) {
        //Thread.sleep(4000);
        total += metadataMap.get(column).getCardinality();
        System.out.println(total);
        dictionaryReaders.put(
            column,
            Loaders.Dictionary.load(metadataMap.get(column), new File("/tmp/mirrorTwoDotO/mirror_mirror_16381_16381_1", column
                + V1Constants.Dict.FILE_EXTENTION), ReadMode.mmap));
        fwdIndexReadersMap.put(column,
            Loaders.ForwardIndex.loadFwdIndexForColumn(metadataMap.get(column), new File("/tmp/mirrorTwoDotO/mirror_mirror_16381_16381_1",
                column + V1Constants.Indexes.UN_SORTED_FWD_IDX_FILE_EXTENTION), ReadMode.mmap));

        invertedIndexMap.put(
            column,
            Loaders.InvertedIndex.load(metadataMap.get(column), new File("/tmp/mirrorTwoDotO/mirror_mirror_16381_16381_1", column
                + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION), ReadMode.mmap));
      }
    } catch (final Exception e) {
      e.printStackTrace();
      while (true) {

      }
    }

    for (final String column : dictionaryReaders.keySet()) {
      if (metadataMap.get(column).isSingleValue() && column.equals("viewerId") && !column.equals("vieweeId")) {
        final BitmapInvertedIndex invertedIndex = invertedIndexMap.get(column);
        final DictionaryReader r = dictionaryReaders.get(column);

        for (int i = 0; i < r.length(); i++) {
          System.out.println(r.get(i) + ":" + Arrays.toString(invertedIndex.getImmutable(i).toArray()));
        }
      }
    }
  }

  @Test
  public void test4() throws Exception {
    final IndexSegment segment = Loaders.IndexSegment.load(new File("/tmp/mirrorTwoDotO/mirror_mirror_16381_16381_1"), ReadMode.mmap);
    // 382912660
    final List<String> rhs = new ArrayList<String>();
    rhs.add("382912660");
    final Predicate p = new Predicate("viewerId", Type.LT, rhs);
    //[59943, 59944, 59945, 59946, 59947, 59948, 59949, 59950, 59951, 59952, 59953, 59954]
    final ChunkColumnarDataSource ds = (ChunkColumnarDataSource) segment.getDataSource("viewerId", p);
    System.out.println(Arrays.toString(ds.getFilteredBitmap().toArray()));
  }
}
