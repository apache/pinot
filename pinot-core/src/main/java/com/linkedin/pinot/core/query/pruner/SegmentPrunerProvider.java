package com.linkedin.pinot.core.query.pruner;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.configuration.Configuration;


/**
 * A static SegmentPrunerProvider will give SegmentPruner instance based on prunerClassName and configuration.
 * 
 * @author Xiang Fu <xiafu@linkedin.com>
 *
 */
public class SegmentPrunerProvider {

  private static Map<String, Class<? extends SegmentPruner>> keyToFunction =
      new ConcurrentHashMap<String, Class<? extends SegmentPruner>>();

  static {
    keyToFunction.put("timesegmentpruner", TimeSegmentPruner.class);
    keyToFunction.put("dataschemasegmentpruner", DataSchemaSegmentPruner.class);
  }

  public static SegmentPruner getSegmentPruner(String prunerClassName, Configuration segmentPrunerConfig) {
    try {
      Class<? extends SegmentPruner> cls = keyToFunction.get(prunerClassName.toLowerCase());
      if (cls != null) {
        SegmentPruner segmentPruner = (SegmentPruner) cls.newInstance();
        segmentPruner.init(segmentPrunerConfig);
        return segmentPruner;
      }
    } catch (Exception ex) {
      throw new RuntimeException("Not support SegmentPruner type with - " + prunerClassName);
    }
    throw new UnsupportedOperationException("No SegmentPruner type with - " + prunerClassName);
  }
}
