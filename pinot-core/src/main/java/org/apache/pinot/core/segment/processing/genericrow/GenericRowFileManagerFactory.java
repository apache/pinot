package org.apache.pinot.core.segment.processing.genericrow;

import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec;

public class GenericRowFileManagerFactory implements FileManagerFactory {
  private Map<String, String> _configs;

  @Override
  public void init( Map<String, String> configs) {
    _configs = configs;
  }

  @Override
  public FileManager createFileManager(File outputDir, List<FieldSpec> fieldSpecs, boolean includeNullFields,
      int numSortFields) {
    return new GenericRowFileManager(outputDir, fieldSpecs, includeNullFields, numSortFields);
  }
}
