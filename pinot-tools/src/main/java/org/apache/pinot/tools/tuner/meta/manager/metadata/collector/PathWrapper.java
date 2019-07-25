package org.apache.pinot.tools.tuner.meta.manager.metadata.collector;

import java.io.File;
import javax.annotation.Nonnull;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;


public class PathWrapper extends AbstractQueryStats {
  private File _file;

  @Override
  public String toString() {
    return "PathWrapper{" + "_path='" + _file + '\'' + ", _tableNameWithoutType='" + _tableNameWithoutType + '\'' + '}';
  }

  public File getFile() {
    return _file;
  }

  public String getTableNameWithoutType() {
    return _tableNameWithoutType;
  }

  private PathWrapper(Builder builder) {
    _tableNameWithoutType = builder._tableNameWithoutType;
    _file = builder._file;
  }

  public static final class Builder {
    private String _tableNameWithoutType;
    private File _file;

    public Builder() {
    }

    @Nonnull
    public Builder setTableNameWithoutType(@Nonnull String val) {
      _tableNameWithoutType = val;
      return this;
    }

    @Nonnull
    public Builder setFile(@Nonnull File val) {
      _file = val;
      return this;
    }

    @Nonnull
    public PathWrapper build() {
      return new PathWrapper(this);
    }
  }
}
