package org.apache.pinot.thirdeye.model.download;

import java.nio.file.Path;
import java.util.Map;


/**
 * The model downloader interface. It downloads model files (e.x., trained tensorflow models), into a local path.
 * The implementation of this class can be configured to run at a certain frequency in ThirdEye server, so that the
 * models can be kept up-to-date.
 */
public abstract class ModelDownloader {
  private final Map<String, Object> properties;

  /**
   * Create a model downloader.
   * @param properties the properties
   */
  public ModelDownloader(Map<String, Object> properties) {
    this.properties = properties;
  }

  /**
   * fetch the models into the local path.
   * @param destination the destination path
   */
  public abstract void fetchModel(Path destination);
}
