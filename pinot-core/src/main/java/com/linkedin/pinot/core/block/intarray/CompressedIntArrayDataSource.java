package com.linkedin.pinot.core.block.intarray;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Predicate;
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

  private int numBlocks;
  private int blockSize;
  private Predicate p;
  private int blockIndex = 0;

  private IntArray intArray;
  private Dictionary<?> dictionary;
  private ColumnMetadata metadata;

  public CompressedIntArrayDataSource(IntArray forwardIndex, Dictionary<?> dictionary, ColumnMetadata metadata) {
    intArray = forwardIndex;
    this.dictionary = dictionary;
    blockSize = metadata.getTotalDocs();
    numBlocks = 1;
    this.metadata = metadata;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public Block nextBlock() {
    if (blockIndex == (numBlocks))
      return null;
    Block b = nextBlock(new BlockId(blockIndex));
    blockIndex++;
    return b;
  }

  private int[] getBlockOffsetsforIndex(int index) {
    int[] ret = { -1, -1 };
    int total = metadata.getTotalDocs();
    int start = index * blockSize;
    if (start >= total)
      return ret;
    int end = start + blockSize;
    ret[0] = start;
    ret[1] = end;
    return ret;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    int[] blockOffsets = getBlockOffsetsforIndex(blockIndex);
    System.out.println("creating block with start : " + blockOffsets[0] + " and end: " + blockOffsets[1]
        + " blockId : " + blockIndex);
    Block b = new CompressedIntArrayBlock(intArray, dictionary, blockOffsets[0], blockOffsets[1], blockIndex);
    if (p != null)
      b.applyPredicate(p);
    return b;
  }

  @Override
  public boolean close() {
    return true;
  }

  @Override
  public boolean setPredicate(Predicate predicate) {
    if (this.p != null)
      return false;
    this.p = predicate;
    return true;
  }
}
