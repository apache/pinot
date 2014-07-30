package com.linkedin.pinot.server.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;

import com.linkedin.pinot.core.indexsegment.ColumnarReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.SegmentMetadata;
import com.linkedin.pinot.query.utils.SimpleColumnMetadata;
import com.linkedin.pinot.query.utils.SimpleColumnarReader;
import com.linkedin.pinot.query.utils.SimpleIndexSegment;
import com.linkedin.pinot.query.utils.SimpleMetadataPersistentManager;
import com.linkedin.pinot.query.utils.SimpleSegmentMetadata;


public class SegmentLoader {
  private static String METADATA_PROPERTIE = "metadata.properties";

  public static IndexSegment loadIndexSegmentFromDir(String segmentDir) throws Exception {
    return loadIndexSegmentFromDir(new File(segmentDir));
  }

  public static IndexSegment loadIndexSegmentFromDir(File segmentDir) throws Exception {
    InputStream metadata = new FileInputStream(new File(segmentDir, METADATA_PROPERTIE));
    PropertiesConfiguration properties = new PropertiesConfiguration();
    properties.setDelimiterParsingDisabled(true);
    properties.load(metadata);
    IOUtils.closeQuietly(metadata);
    String indexType = properties.getString("segment.index.type", "simple");
    if (indexType.equalsIgnoreCase("simple")) {
      SegmentMetadata segmentMetadata = SimpleSegmentMetadata.load(properties);
      Map<String, SimpleColumnMetadata> metadataMap = SimpleMetadataPersistentManager.readFromConfig(properties);
      IndexSegment indexSegment = readSimpleIndexeSegment(metadataMap, segmentDir, segmentMetadata);
      return indexSegment;
    }
    return null;
  }

  private static SimpleIndexSegment readSimpleIndexeSegment(Map<String, SimpleColumnMetadata> metadataMap,
      File indexDir, SegmentMetadata globalProperties) throws IOException {
    Map<String, ColumnarReader> forwardIndexMap = readForwardIndexes(metadataMap, indexDir);
    long numRecords = ((SimpleSegmentMetadata) globalProperties).getTotalDocs();
    SimpleIndexSegment ret = new SimpleIndexSegment(numRecords, forwardIndexMap);
//    ret.setSegmentMetadata(globalProperties);
//    ret.setAssociatedDirectory(indexDir.getAbsolutePath());
//    ret.setSegmentName(indexDir.getName());
    return ret;
  }

  private static Map<String, ColumnarReader> readForwardIndexes(Map<String, SimpleColumnMetadata> metadataMap,
      File indexDir) throws IOException {
    Map<String, ColumnarReader> forwardIndexMap = new HashMap<String, ColumnarReader>();
    for (String columnName : metadataMap.keySet()) {
      ColumnarReader columnarReader =
          SimpleColumnarReader.readFromFile(metadataMap.get(columnName).getSize(), new File(indexDir, columnName
              + ".simple"));
      forwardIndexMap.put(columnName, columnarReader);
    }

    return forwardIndexMap;
  }
}
