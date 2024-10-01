package org.apache.pinot.core.segment.processing.genericrow;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;


public interface FileManagerFactory {

  void init(Map<String, String> configs);

  FileManager createFileManager(File outputDir, List<FieldSpec> fieldSpecs, boolean includeNullFields,
      int numSortFields);
}