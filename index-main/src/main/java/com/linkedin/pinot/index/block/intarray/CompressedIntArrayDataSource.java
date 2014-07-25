package com.linkedin.pinot.index.block.intarray;

import java.util.HashMap;
import java.util.Map;

import com.linkedin.pinot.index.common.Block;
import com.linkedin.pinot.index.common.BlockId;
import com.linkedin.pinot.index.common.Predicate;
import com.linkedin.pinot.index.operator.DataSource;
import com.linkedin.pinot.segments.v1.segment.dictionary.Dictionary;
import com.linkedin.pinot.segments.v1.segment.utils.HeapCompressedIntArray;
import com.linkedin.pinot.segments.v1.segment.utils.IntArray;


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

  public CompressedIntArrayDataSource(IntArray forwardIndex, Dictionary<?> dictionary) {
    intArray = forwardIndex;
    this.dictionary = dictionary;
    blockSize = intArray.size();
    numBlocks = 1;
  }

  @Override
  public boolean open() {
    return true;
  }

  @Override
  public Block nextBlock() {
    // TODO Auto-generated method stub
    if (blockIndex == (numBlocks))
      return null;

    Block b = nextBlock(new BlockId(blockIndex));
    blockIndex++;
    return b;
  }

  private int[] getBlockOffsetsforIndex(int index) {
    int[] ret = { -1, -1 };
    int total = intArray.size();
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
    // TODO Auto-generated method stub
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
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean setPredicate(Predicate predicate) {
    if (this.p != null)
      return false;
    this.p = predicate;
    return true;
  }
}
