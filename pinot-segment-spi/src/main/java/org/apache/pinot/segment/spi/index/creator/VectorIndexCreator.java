package org.apache.pinot.segment.spi.index.creator;

import java.io.IOException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.spi.data.readers.Vector;


public interface VectorIndexCreator extends IndexCreator {
  @Override
  default void add(@Nonnull Object value, int dictId)
      throws IOException {
    add((Vector) value);
  }

  @Override
  default void add(@Nonnull Object[] values, @Nullable int[] dictIds) {
  }

  void add(Vector document);

  void seal()
      throws IOException;

  @Override
  default void close()
      throws IOException {

  }
}
