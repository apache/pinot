package org.apache.pinot.tools.tuner.meta.manager.metadata.collector;

import javax.annotation.Nonnull;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;


public class PathWrapper extends AbstractQueryStats {
  private String _path;

  @Override
  public String toString() {
    return "PathWrapper{" + "_path='" + _path + '\'' + '}';
  }

  private PathWrapper(Builder builder) {
    _path = builder._path;
  }

  public static final class Builder {
    private String _path;

    public Builder() {
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
