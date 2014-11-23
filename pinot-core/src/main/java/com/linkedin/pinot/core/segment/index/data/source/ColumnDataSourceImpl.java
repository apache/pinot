package com.linkedin.pinot.core.segment.index.data.source;

import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.operator.DataSource;
import com.linkedin.pinot.core.segment.index.BitmapInvertedIndex;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.data.source.mv.block.MultiValueBlock;
import com.linkedin.pinot.core.segment.index.data.source.sv.block.SingleValueBlock;
import com.linkedin.pinot.core.segment.index.readers.DictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedMVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedSVForwardIndexReader;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 15, 2014
 *
 */

public class ColumnDataSourceImpl implements DataSource {
  private static final Logger logger = Logger.getLogger(ColumnDataSourceImpl.class);

  private final DictionaryReader dictionary;
  private final DataFileReader reader;
  private final BitmapInvertedIndex invertedIndex;
  private final SegmentMetadataImpl columnMetadata;
  private Predicate predicate;
  private ImmutableRoaringBitmap filteredBitmap = null;

  int blockNextCallCount = 0;

  public ColumnDataSourceImpl(DictionaryReader dictionary, DataFileReader reader, BitmapInvertedIndex invertedIndex,
      SegmentMetadataImpl columnMetadata) {
    this.dictionary = dictionary;
    this.reader = reader;
    this.invertedIndex = invertedIndex;
    this.columnMetadata = columnMetadata;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public Block nextBlock() {
    if (blockNextCallCount > 0) {
      if (columnMetadata.isSingleValue()) {
        return new SingleValueBlock(new BlockId(0), (FixedBitCompressedSVForwardIndexReader) reader, filteredBitmap, dictionary,
            columnMetadata);
      } else {
        return new MultiValueBlock(new BlockId(0), (FixedBitCompressedMVForwardIndexReader) reader, filteredBitmap, dictionary,
            columnMetadata);
      }
    }

    blockNextCallCount++;
    return null;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    if (blockNextCallCount > 0) {
      logger.info("sending back a new Block for a column with isSingleValue set to : " + columnMetadata.isSingleValue());
    }

    blockNextCallCount++;
    return null;
  }

  @Override
  public boolean close() {
    return true;
  }

  public ImmutableRoaringBitmap getFilteredBitmap() {
    return filteredBitmap;
  }

  @Override
  public boolean setPredicate(Predicate p) {
    predicate = p;

    switch (predicate.getType()) {
      case EQ:
        final int valueToLookUP = dictionary.indexOf(predicate.getRhs().get(0));
        filteredBitmap = invertedIndex.getImmutable(valueToLookUP);
        break;
      case GT:
        int startGT = dictionary.indexOf(predicate.getRhs().get(0));

        if (startGT < 0) {
          startGT = -(startGT + 1);
        }

        if (startGT >= dictionary.length()) {
          filteredBitmap = null;
        } else {
          final MutableRoaringBitmap holderGT = invertedIndex.getMutable(startGT);

          for (int i = startGT + 1; i < dictionary.length(); i++) {
            holderGT.or(invertedIndex.getImmutable(i));
          }
          filteredBitmap = holderGT;
        }

        break;
      case LT:
        int startLT = dictionary.indexOf(predicate.getRhs().get(0));

        if (startLT < 0) {
          startLT = -(startLT + 1);
        }

        if (startLT > dictionary.length()) {
          startLT = dictionary.length() - 1;
        }

        if (startLT == 0) {
          filteredBitmap = null;
        } else {
          final MutableRoaringBitmap holderLT = invertedIndex.getMutable(startLT);

          for (int i = 0; i < startLT; i++) {
            holderLT.or(invertedIndex.getImmutable(i));
          }

          filteredBitmap = holderLT;
        }
        break;
      case GT_EQ:
        int startGTE = dictionary.indexOf(predicate.getRhs().get(0));

        if (startGTE < 0) {
          startGTE = -(startGTE + 1);
        }

        if (startGTE >= dictionary.length()) {
          filteredBitmap = null;
        } else {
          final MutableRoaringBitmap holderGTE = invertedIndex.getMutable(startGTE);

          for (int i = startGTE; i < dictionary.length(); i++) {
            holderGTE.or(invertedIndex.getImmutable(i));
          }
          filteredBitmap = holderGTE;
        }
        break;
      case LT_EQ:
        int startLTE = dictionary.indexOf(predicate.getRhs().get(0));

        if (startLTE < 0) {
          startLTE = -(startLTE + 1);
        }

        if (startLTE > dictionary.length()) {
          startLTE = dictionary.length() - 1;
        }

        if (startLTE == 0) {
          filteredBitmap = null;
        } else {
          final MutableRoaringBitmap holderLTE = invertedIndex.getMutable(startLTE - 1);

          for (int i = 0; i <= startLTE; i++) {
            holderLTE.or(invertedIndex.getImmutable(i));
          }

          filteredBitmap = holderLTE;
        }
        break;
      case NEQ:
        int neq = dictionary.indexOf(predicate.getRhs().get(0));
        if (neq < 0) {
          neq = 0;
        }

        final MutableRoaringBitmap holderNEQ = invertedIndex.getMutable(neq);

        for (int i = 0; i < dictionary.length(); i++) {
          holderNEQ.or(invertedIndex.getImmutable(i));
        }

        filteredBitmap = holderNEQ;
        break;
      case RANGE:

        int rangeStartIndex = 0;
        final int rangeEndIndex = 0;

        final String rangeString = predicate.getRhs().get(0);
        boolean incLower = true, incUpper = false;


        if(rangeString.trim().startsWith("(")) {
          incLower = false;
        }

        if(rangeString.trim().endsWith(")")) {
          incUpper = false;
        }

        final String lower,upper;
        lower = rangeString.split(",")[0].substring(1, rangeString.split(",")[0].length());
        upper = rangeString.split(",")[1].substring(0, rangeString.split(",")[1].length() - 1);

        if (lower.equals("*")) {
          rangeStartIndex = 0;
        }

        if (upper.equals("*")) {

        }
        throw new UnsupportedOperationException("unsupported type : " + columnMetadata.getDataType().toString()
            + " for filter type : range");
      case REGEX:
        throw new UnsupportedOperationException("unsupported type : " + columnMetadata.getDataType().toString()
            + " for filter type : regex");
    }
    return true;
  }

}
