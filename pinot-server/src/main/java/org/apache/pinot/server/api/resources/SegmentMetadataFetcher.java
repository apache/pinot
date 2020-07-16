package org.apache.pinot.server.api.resources;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.restlet.resources.SegmentStatus;
import org.apache.pinot.core.data.manager.SegmentDataManager;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentMetadataFetcher {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMetadataFetcher.class);

  public static String getSegmentMetadata(SegmentDataManager segmentDataManager, List<String> columns) {
    LOGGER.trace("Inside getSegmentMetadata()");
    SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) segmentDataManager.getSegment().getSegmentMetadata();
    Set<String> columnSet;
    if (columns.size() == 1 && columns.get(0).equals("*")) {
      columnSet = null;
    } else {
      columnSet = new HashSet<>(columns);
    }
    JsonNode indexes = SegmentColumnIndexesFetcher.getIndexesForSegmentColumns(segmentDataManager, columnSet);
    JsonNode segmentMetadataJson = segmentMetadata.toJson(columnSet);
    ObjectNode segmentMetadataObject = segmentMetadataJson.deepCopy();
    segmentMetadataObject.set("indexes", indexes);
    LOGGER.debug("Fetched all metadata for the segment.");
    return segmentMetadataObject.toString();
  }

  /**
   * This is a helper method to fetch segment reload status.
   * @param segmentDataManager
   * @return segment refresh time
   */
  public static SegmentStatus getSegmentReloadStatus(SegmentDataManager segmentDataManager) {
    SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) segmentDataManager.getSegment().getSegmentMetadata();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS' UTC'");
    long refreshTime = segmentMetadata.getRefreshTime();
    String refreshTimeReadable = refreshTime != Long.MIN_VALUE ? dateFormat.format(new Date(refreshTime)) : "";
    return new SegmentStatus(segmentDataManager.getSegmentName(), refreshTimeReadable);
  }
}
