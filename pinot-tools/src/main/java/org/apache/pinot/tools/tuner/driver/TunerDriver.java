package org.apache.pinot.tools.tuner.driver;

import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.QuerySrc;
import org.apache.pinot.tools.tuner.strategy.BasicStrategy;


/*
TunerDriver is an executable interface, has three pluggable modules:
    MetaData Manager: a manger for MetaDataProperties, which is an interface to access segment metadata.
    QuerySrc: an iterator interface over input source, has a pluggable BasicQueryParser, who parses each item in input source, and returns BasicQueryStats, a wrapper of relevant fields in nput.
    Strategy, which has four user defined functions operating on a map of Map<Long, Map<String, Map<String, MergerObj>>>:
                                                                                |		       |				    |				  |
                                                                            ThreadID	TableName	    ColumnName		Abstract object of stats for a column
        Filter: A function to filter BasicQueryStats, by table name, number of entries scanned in filters, number of entries scanned post filter, etc. The relevant BasicQueryStats will be feed to Accumulator.
        Accumulator: A function to process BasicQueryStats and MetaDataProperties; then accumulate stats to corresponding MergerObj entry.
        Merger: A function to merge two MergerObj entries having the same TableName/ColumnName from different threads.
        Reporter: A function to postprocess and print(email) out the final results of a table.
*/
public abstract class TunerDriver {

  protected QuerySrc _querySrc = null;
  protected MetaManager _metaManager = null;
  protected BasicStrategy _strategy = null;

  public abstract void excute();
}
