package org.apache.pinot.core.segment.processing.genericrow;

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.core.segment.processing.framework.SegmentProcessorConfig;
import org.apache.pinot.spi.plugin.PluginManager;


public class FileManagerFactoryProvider {
  public static final String FILE_WRITER_FACTORY_CLASS_KEY = "segmentprocessor.file.manager.factory.class";
  public static final String DEFAULT_FILE_WRITER_FACTORY_CLASS =
      "org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManagerFactory";
  public static final String FILE_WRITER_CONFIG_PREFIX = "segmentProcessor.file.manager.config.";

  public static FileManagerFactory create(SegmentProcessorConfig config) {
    FileManagerFactory factory = null;
    Map<String, String> taskConfig = config.getTaskConfigs();

    String fileWriterClass = taskConfig.get(FILE_WRITER_FACTORY_CLASS_KEY);

    if (fileWriterClass == null) {
      fileWriterClass = DEFAULT_FILE_WRITER_FACTORY_CLASS;
    }

    try {
      factory = PluginManager.get().createInstance(fileWriterClass);
      // get all configs with prefix in a seperate map
      Map<String, String> fileManagerConfig = config.getTaskConfigs().entrySet().stream()
          .filter(entry -> entry.getKey().startsWith(FILE_WRITER_CONFIG_PREFIX)).collect(
              Collectors.toMap(entry -> entry.getKey().substring(FILE_WRITER_CONFIG_PREFIX.length()),
                  Map.Entry::getValue));
      factory.init(fileManagerConfig);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create FileWriterFactory", e);
    }
    return factory;
  }
}
