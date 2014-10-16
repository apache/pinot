package com.linkedin.pinot.core.block.intarray;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.indexsegment.columnar.BitmapInvertedIndex;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnMetadata;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.utils.IntArray;
import com.linkedin.pinot.core.operator.DataSource;


/**
 * Jul 15, 2014
 * @author Dhaval Patel <dpatel@linkedin.com>
 *
 */
public class CompressedIntArrayDataSource implements DataSource {

  private final int numBlocks;
  private final int blockSize;
  private Predicate p;
  private int blockIndex = 0;

  private final IntArray intArray;
  private final Dictionary<?> dictionary;
  private final ColumnMetadata metadata;
  private final BitmapInvertedIndex invertedIdx;
  private final DataType type;

  public CompressedIntArrayDataSource(IntArray forwardIndex, Dictionary<?> dictionary, ColumnMetadata metadata,
      BitmapInvertedIndex invertedIndex, DataType type) {
    intArray = forwardIndex;
    this.dictionary = dictionary;
    blockSize = metadata.getTotalDocs();
    numBlocks = 1;
    this.metadata = metadata;
    invertedIdx = invertedIndex;
    this.type = type;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public Block nextBlock() {
    if (blockIndex == (numBlocks)) {
      return null;
    }
    final Block b = nextBlock(new BlockId(blockIndex));
    blockIndex++;
    return b;
  }

  private int[] getBlockOffsetsforIndex(int index) {
    final int[] ret = { -1, -1 };
    final int total = metadata.getTotalDocs();
    final int start = index * blockSize;
    if (start >= total) {
      return ret;
    }
    final int end = start + blockSize;
    ret[0] = start;
    ret[1] = end;
    return ret;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    final int[] blockOffsets = getBlockOffsetsforIndex(blockIndex);
    System.out.println("creating block with start : " + blockOffsets[0] + " and end: " + blockOffsets[1]
        + " blockId : " + blockIndex);
    final Block b =
        new CompressedIntArrayBlock(intArray, dictionary, blockOffsets[0], blockOffsets[1], blockIndex, invertedIdx, metadata.getDataType());
    if (p != null) {
      b.applyPredicate(p);
    }
    return b;
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public boolean setPredicate(Predicate predicate) {
    if (p != null) {
      return false;
    }
    p = predicate;
    return true;
  }
}
