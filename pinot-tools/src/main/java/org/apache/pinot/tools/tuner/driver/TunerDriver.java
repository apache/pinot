package org.apache.pinot.tools.tuner.driver;

import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.QuerySrc;
import org.apache.pinot.tools.tuner.strategy.BasicStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
TunerDriver is an executable interface, has three pluggable modules:
    MetaData Manager: a manger for MetaDataProperties, which is an interface to access segment metadata.
    QuerySrc: an iterator interface over input source, has a pluggable BasicQueryParser, who parses each item in input source, and returns BasicQueryStats, a wrapper of relevant fields in nput.
    Strategy, which has four user defined functions operating on a map of Map<Long, Map<String, Map<String, ColumnStatsObj>>>:
                                                                                |		       |				    |				      |
                                                                            ThreadID	TableName	    ColumnName		Abstract object of stats for a column
        Filter: A function to filter BasicQueryStats, by table name, number of entries scanned in filters, number of entries scanned post filter, etc. The relevant BasicQueryStats will be feed to Accumulator.
        Accumulator: A function to process BasicQueryStats and MetaDataProperties; then accumulate stats to corresponding ColumnStatsObj entry.
        Merger: A function to merge two ColumnStatsObj entries having the same TableName/ColumnName from different threads.
        Reporter: A function to postprocess and print(email) out the final results of a table.
*/
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
