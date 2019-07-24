package org.apache.pinot.tools.tuner.meta.manager.metadata.collector;

import javax.annotation.Nonnull;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;


public class PathWrapper extends AbstractQueryStats {
  private String _path;

  @Override
  public String toString() {
    return "PathWrapper{" + "_path='" + _path + '\'' + ", _tableNameWithoutType='" + _tableNameWithoutType + '\'' + '}';
  }

  public String getPath() {
    return _path;
  }

  public String getTableNameWithoutType() {
    return _tableNameWithoutType;
  }

  private PathWrapper(Builder builder) {
    _tableNameWithoutType = builder._tableNameWithoutType;
    _path = builder._path;
  }

  public static final class Builder {
    private String _tableNameWithoutType;
    private String _path;

    public Builder() {
    }

    @Nonnull
    public Builder setTableNameWithoutType(@Nonnull String val) {
      _tableNameWithoutType = val;
      return this;
    }

    @Nonnull
    public Builder setPath(@Nonnull String val) {
      _path = val;
      return this;
    }

    @Nonnull
    public PathWrapper build() {
      return new PathWrapper(this);
    }
  }
}
