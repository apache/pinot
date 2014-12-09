package com.linkedin.pinot.core.segment.index.data.source;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.operator.DataSource;
import com.linkedin.pinot.core.segment.index.BitmapInvertedIndex;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
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
  private final ColumnMetadata columnMetadata;
  private Predicate predicate;
  private ImmutableRoaringBitmap filteredBitmap = null;
  private int blockNextCallCount = 0;

  public ColumnDataSourceImpl(DictionaryReader dictionary, DataFileReader reader, BitmapInvertedIndex invertedIndex,
      ColumnMetadata columnMetadata) {
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
    blockNextCallCount++;
    if (blockNextCallCount <= 1) {
      if (columnMetadata.isSingleValue()) {
        return new SingleValueBlock(new BlockId(0), (FixedBitCompressedSVForwardIndexReader) reader, filteredBitmap, dictionary,
            columnMetadata);
      } else {
        return new MultiValueBlock(new BlockId(0), (FixedBitCompressedMVForwardIndexReader) reader, filteredBitmap, dictionary,
            columnMetadata);
      }
    }
    return null;
  }

  @Override
  public Block nextBlock(BlockId blockId) {
    if (columnMetadata.isSingleValue()) {
      return new SingleValueBlock(blockId, (FixedBitCompressedSVForwardIndexReader) reader, filteredBitmap, dictionary, columnMetadata);
    } else {
      return new MultiValueBlock(blockId, (FixedBitCompressedMVForwardIndexReader) reader, filteredBitmap, dictionary, columnMetadata);
    }
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
        if (valueToLookUP < 0) {
          filteredBitmap = new MutableRoaringBitmap();
        } else {
          filteredBitmap = invertedIndex.getImmutable(valueToLookUP);
        }
        break;
      case NEQ:
        // will change this later
        final int neq = dictionary.indexOf(predicate.getRhs().get(0));

        final MutableRoaringBitmap holderNEQ = new MutableRoaringBitmap();

        for (int i = 0; i < dictionary.length(); i++) {
          if (i != neq) {
            holderNEQ.or(invertedIndex.getImmutable(i));
          }
        }

        filteredBitmap = holderNEQ;
        break;
      case IN:
        final String[] inValues = predicate.getRhs().get(0).split(",");
        final MutableRoaringBitmap inHolder = new MutableRoaringBitmap();

        for (final String value : inValues) {
          final int index = dictionary.indexOf(value);
          if (index >= 0) {
            inHolder.or(invertedIndex.getImmutable(index));
          }
        }
        filteredBitmap = inHolder;
        break;
      case NOT_IN:
        final String[] notInValues = predicate.getRhs().get(0).split(",");
        final List<Integer> notInIds = new ArrayList<Integer>();
        for (final String notInValue : notInValues) {
          notInIds.add(new Integer(dictionary.indexOf(notInValue)));
        }

        final MutableRoaringBitmap notINHolder = new MutableRoaringBitmap();

        for (int i = 0; i < dictionary.length(); i++) {
          if (!notInIds.contains(new Integer(i))) {
            notINHolder.or(invertedIndex.getImmutable(i));
          }
        }

        filteredBitmap = notINHolder;
        break;
      case RANGE:

        int rangeStartIndex = 0;
        int rangeEndIndex = 0;

        final String rangeString = predicate.getRhs().get(0);
        boolean incLower = true,
            incUpper = false;

        if (rangeString.trim().startsWith("(")) {
          incLower = false;
        }

        if (rangeString.trim().endsWith(")")) {
          incUpper = false;
        }

        final String lower,
        upper;
        lower = rangeString.split(",")[0].substring(1, rangeString.split(",")[0].length());
        upper = rangeString.split(",")[1].substring(0, rangeString.split(",")[1].length() - 1);

        if (lower.equals("*")) {
          rangeStartIndex = 0;
        } else {
          rangeStartIndex = dictionary.indexOf(lower);
        }

        if (upper.equals("*")) {
          rangeEndIndex = dictionary.length() - 1;
        } else {
          rangeEndIndex = dictionary.indexOf(upper);
        }

        if (rangeStartIndex < 0) {
          rangeStartIndex = -(rangeStartIndex + 1);
        } else if (!incLower && !lower.equals("*")) {
          rangeStartIndex += 1;
        }


        if (rangeEndIndex < 0) {
          rangeEndIndex = -(rangeEndIndex + 1);
          rangeEndIndex = Math.max(0, rangeEndIndex - 1);
        } else if (!incUpper && !upper.equals("*")) {
          rangeEndIndex -= 1;
        }

        final MutableRoaringBitmap rangeBitmapHolder = invertedIndex.getMutable(rangeStartIndex);
        for (int i = (rangeStartIndex + 1); i <= rangeEndIndex; i++) {
          rangeBitmapHolder.or(invertedIndex.getImmutable(i));
        }
        filteredBitmap = rangeBitmapHolder;
        break;
      case REGEX:
        throw new UnsupportedOperationException("unsupported type : " + columnMetadata.getDataType().toString()
            + " for filter type : regex");
    }
    return true;
  }
}
