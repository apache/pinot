package com.linkedin.thirdeye.reporting.util;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.joda.time.DateTime;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.reporting.api.TimeRange;
import com.linkedin.thirdeye.reporting.api.SegmentDescriptor;

public class SegmentDescriptorUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentDescriptorUtils.class);

  public static List<TimeRange> checkSegments(String serverUri, String collection, String timezone, DateTime currentStartHour, DateTime currentEndHour, DateTime baselineStartHour,
      DateTime baselineEndHour) throws JsonParseException, JsonMappingException, UnsupportedEncodingException, IOException {

    List<TimeRange> missingSegments = new ArrayList<TimeRange>();

    // get existing data segments
    ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    URL url = new URL(serverUri + "/collections/" + collection + "/segments");
    SegmentDescriptor[] sd = OBJECT_MAPPER.readValue((new InputStreamReader(url.openStream(), "UTF-8")), SegmentDescriptor[].class);
    List<SegmentDescriptor> segmentDescriptors = new ArrayList<SegmentDescriptor>();
    for (SegmentDescriptor segment : sd) {
      if (segment != null && segment.getFile() != null) {
        segmentDescriptors.add(segment);
      }
    }
    Collections.sort(segmentDescriptors, new Comparator<SegmentDescriptor>() {

      @Override
      public int compare(SegmentDescriptor o1, SegmentDescriptor o2) {
        return o1.getStartWallTime().compareTo(o2.getStartWallTime());
      }
    });
    for (SegmentDescriptor segment : segmentDescriptors) {
      LOGGER.info(segment.getStartWallTime(timezone).getMillis() + " " +segment.getEndWallTime(timezone).getMillis());
      LOGGER.info("Segment : " + segment.getStartWallTime(timezone) + " " + segment.getEndWallTime(timezone) + " " + segment.getFile());
    }

    // find missing ranges in current and baseline
    missingSegments.addAll(getMissingSegments(segmentDescriptors, timezone, currentStartHour, currentEndHour));
    missingSegments.addAll(getMissingSegments(segmentDescriptors, timezone, baselineStartHour, baselineEndHour));
    return missingSegments;

  }

  private static List<TimeRange> getMissingSegments(List<SegmentDescriptor> segmentDescriptors, String timezone, DateTime startDate, DateTime endDate) {
    List<TimeRange> missingRanges = new ArrayList<TimeRange>();
    long startHour = startDate.getMillis();
    long endHour = endDate.getMillis();

    LOGGER.info(startHour + " " +endHour);
    LOGGER.info("Start : " + startDate + " End : " + endDate);
    int start = -1;
    for (int i = 0; i < segmentDescriptors.size(); i ++) {
      LOGGER.info("segment i : "+segmentDescriptors.get(i).getStartWallTime(timezone).getMillis()+ " " +segmentDescriptors.get(i).getEndWallTime(timezone).getMillis() + " starthour" +startHour );;
      if (segmentDescriptors.get(i).includesTime(startHour, timezone)) {
        start = i;
        break;
      }
    }
    if (start == -1) {
      missingRanges.add(new TimeRange(startHour, endHour, timezone));
      LOGGER.info("Completely missing");
    } else {
      Long previousEnd = null;
      Long currentEnd = null;
      long currentStart;
      while (start < segmentDescriptors.size()) {
        currentStart = segmentDescriptors.get(start).getStartWallTime(timezone).getMillis();
        currentEnd = segmentDescriptors.get(start).getEndWallTime(timezone).getMillis();
        LOGGER.info("previousend : "+previousEnd+" currrentstart : "+currentStart + " currentend : "+currentEnd);
        if (previousEnd != null && currentStart != previousEnd) {
          missingRanges.add(new TimeRange(previousEnd, currentStart, timezone));
          LOGGER.info("Adding missing range : "+previousEnd +" "+currentStart);
        }
        previousEnd = currentEnd;

        if (endHour <= currentEnd) {
          LOGGER.info("current end "+currentEnd+" exceeded endhour "+endHour);
          break;
        }
        start ++;
      }
      if (currentEnd < endHour) {
        LOGGER.info("currentend is " + currentEnd + " endHour is "+endHour);
        missingRanges.add(new TimeRange(currentEnd, endHour, timezone));
      }
    }
    return missingRanges;
  }

}
