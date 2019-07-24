package org.apache.pinot.tools.tuner.meta.manager.metadata.collector;

import io.vavr.Tuple2;
import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.pinot.tools.tuner.query.src.QuerySrc;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CompressedFilePathIter implements QuerySrc {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompressedFilePathIter.class);
  Iterable<Tuple2<String, File>> _iterable;

  private String _directory;

  private CompressedFilePathIter(Builder builder) {
    _directory = builder._directory;
  }

  private CompressedFilePathIter openDirectory() {
    File dir = new File(_directory);
    if (!dir.exists() || dir.isFile()) {
      LOGGER.error("Wrong input directory!");
      System.exit(1);
    }

    ArrayList<Tuple2<String, File>> validTableNameWithoutTypeSegmentFile = new ArrayList<>();

    Arrays.stream(Objects.requireNonNull(dir.listFiles()))
        .filter(tableDir -> (!tableDir.getName().startsWith(".") && tableDir.isDirectory())).forEach(
        tableDir -> Arrays.asList(Objects.requireNonNull(tableDir.listFiles()))
            .forEach(file -> validTableNameWithoutTypeSegmentFile.add(new Tuple2<>(tableDir.getName(), file))));

    _iterable = validTableNameWithoutTypeSegmentFile;
    return this;
  }

  /**
   *
   * @return If the input has next stats obj
   */
  @Override
  public boolean hasNext() {
    return _iterable.iterator().hasNext();
  }

  /**
   *
   * @return The next obj parsed from input
   * @throws NoSuchElementException
   */
  @Override
  public AbstractQueryStats next()
      throws NoSuchElementException {
    Tuple2<String, File> nextTuple = _iterable.iterator().next();
    return new PathWrapper.Builder().setTableNameWithoutType(nextTuple._1()).setPath(nextTuple._2().getAbsolutePath())
        .build();
  }

  public static final class Builder {
    private String _directory;

    public Builder() {
    }

    @Nonnull
    public Builder set_directory(@Nonnull String val) {
      _directory = val;
      return this;
    }

    @Nonnull
    public CompressedFilePathIter build() {
      return new CompressedFilePathIter(this).openDirectory();
    }
  }
}
