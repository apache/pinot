package com.linkedin.pinot.core.chunk.index.data.source;

import org.apache.log4j.Logger;

import com.linkedin.pinot.core.chunk.index.readers.AbstractDictionaryReader;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.datasource.ChunkColumnMetadata;
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.indexsegment.columnar.BitmapInvertedIndex;
import com.linkedin.pinot.core.operator.DataSource;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 15, 2014
 *
 */

public class ChunkColumnarDataSource implements DataSource {
  private static final Logger logger = Logger.getLogger(ChunkColumnarDataSource.class);

  private final AbstractDictionaryReader<?> dictionary;
  private final DataFileReader reader;
  private final BitmapInvertedIndex invertedIndex;
  private final ChunkColumnMetadata columnMetadata;
  private Predicate predicate;

  int blockNextCallCount = 0;

  public ChunkColumnarDataSource(AbstractDictionaryReader<?> dictionary, DataFileReader reader, BitmapInvertedIndex invertedIndex,
      ChunkColumnMetadata columnMetadata) {
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
      logger.info("sending back a new Block for a column with isSingleValue set to : " + columnMetadata.isSingleValue());
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

  @Override
  public boolean setPredicate(Predicate predicate) {
    this.predicate = predicate;
    return true;
  }

}
