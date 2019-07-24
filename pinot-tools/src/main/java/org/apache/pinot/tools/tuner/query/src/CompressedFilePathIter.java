package org.apache.pinot.tools.tuner.query.src;

import java.io.File;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import javax.annotation.Nonnull;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.IndexSuggestQueryStatsImpl;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.PathWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CompressedFilePathIter implements QuerySrc {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompressedFilePathIter.class);
  Iterable<File> _iterable;

  private String _directory;

  private CompressedFilePathIter(Builder builder) {
    _directory = builder._directory;
  }

  public CompressedFilePathIter openDirectory() {
    File dir = new File(_directory);
    if (!dir.exists() || dir.isFile()) {
      LOGGER.error("Wrong input directory!");
      System.exit(1);
    }
    File[] files = dir.listFiles();
    ArrayList<File> validFileArrayList = new ArrayList<>();
    for (File file : files) {
      if (!file.getName().startsWith(".") && file.isFile()) {
        validFileArrayList.add(file);
      }
    }
    _iterable = validFileArrayList;
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
    return new PathWrapper.Builder().setPath(_iterable.iterator().next().getAbsolutePath()).build();
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
