package org.apache.pinot.tools.tuner.driver;

import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.QuerySrc;
import org.apache.pinot.tools.tuner.strategy.BasicStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class TunerDriver {
  protected static final Logger LOGGER = LoggerFactory.getLogger(TunerDriver.class);

  protected QuerySrc _querySrc = null;
  protected MetaManager _metaManager = null;
  protected BasicStrategy _strategy = null;

  protected TunerDriver setQuerySrc(QuerySrc querySrc) {
    _querySrc = querySrc;
    return this;
  }

  protected TunerDriver setMetaManager(MetaManager metaManager) {
    _metaManager = metaManager;
    return this;
  }

  protected TunerDriver setStrategy(BasicStrategy strategy) {
    _strategy = strategy;
    return this;
  }

  public abstract void excute();
}
