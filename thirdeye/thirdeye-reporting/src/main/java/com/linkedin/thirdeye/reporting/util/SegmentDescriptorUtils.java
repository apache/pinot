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

  public static List<TimeRange> checkSegments(String serverUri, String collection, DateTime currentStartHour, DateTime currentEndHour, DateTime baselineStartHour,
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
      LOGGER.info("Segment : " + segment.getStartWallTime() + " " + segment.getEndWallTime() + " " + segment.getFile());
    }

    // find missing ranges in current and baseline
    missingSegments.addAll(getMissingSegments(segmentDescriptors, currentStartHour.getMillis(), currentEndHour.getMillis()));
    missingSegments.addAll(getMissingSegments(segmentDescriptors, baselineStartHour.getMillis(), baselineEndHour.getMillis()));
    return missingSegments;

  }

  private static List<TimeRange> getMissingSegments(List<SegmentDescriptor> segmentDescriptors, long startHour, long endHour) {
    List<TimeRange> missingRanges = new ArrayList<TimeRange>();

    LOGGER.info("Start : " + startHour + " End : " + endHour);
    int start = -1;
    for (int i = 0; i < segmentDescriptors.size(); i ++) {
      if (segmentDescriptors.get(i).includesTime(startHour)) {
        start = i;
        break;
      }
    }
    if (start == -1) {
      missingRanges.add(new TimeRange(startHour, endHour));
    } else {
      Long previousEnd = null;
      Long currentEnd = null;
      long currentStart;
      while (start < segmentDescriptors.size()) {
        currentStart = segmentDescriptors.get(start).getStartWallTime().getMillis();
        currentEnd = segmentDescriptors.get(start).getEndWallTime().getMillis();
        if (previousEnd != null && currentStart != previousEnd) {
          missingRanges.add(new TimeRange(previousEnd, currentStart));
        }
        previousEnd = currentEnd;

        if (endHour <= currentEnd) {
          break;
        }
        start ++;
      }
      if (currentEnd < endHour) {
        missingRanges.add(new TimeRange(currentEnd, endHour));
      }
    }
    return missingRanges;
  }

}
