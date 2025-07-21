package org.apache.pinot.common.utils.filesystem;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.plugin.PluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LiPinotFS implements PinotFS {
  public static final String S3_SCHEME = "s3";
  public static final String FILE_SCHEME = "file";
  public static final String HTTPS_SCHEME = "https";
  private static final Logger LOGGER = LoggerFactory.getLogger(LiPinotFS.class);
  private static final Map<String, PinotFS> PINOT_FS_MAP = new HashMap<>();
  private String _s3DataPath;
  private String _bucketName;

  @Override
  public void init(PinotConfiguration config) {
    String[] schemes = config.getProperty("schemes").split(",");
    for (String scheme : schemes) {
      PinotConfiguration fsConfiguration = config.subset(scheme);
      String fsClassName = fsConfiguration.getProperty("class");
      try {
        PinotFS pinotFS = PluginManager.get().createInstance(fsClassName);
        pinotFS.init(fsConfiguration);
        PINOT_FS_MAP.put(scheme, pinotFS);
        _s3DataPath = fsConfiguration.getProperty("s3DataPath", "");
        _bucketName = fsConfiguration.getProperty("bucketName", "");
      } catch (Exception e) {
        LOGGER.error("Could not instantiate file system for class {} with scheme {}", fsClassName, scheme, e);
        throw new RuntimeException(e);
      }
    }
    // This is needed if s3 API is not enabled.
    PINOT_FS_MAP.put(FILE_SCHEME, PinotFSFactory.create(FILE_SCHEME));
  }

  public String getScheme(URI uri) {
    switch (uri.getScheme()) {
      case S3_SCHEME:
        return uri.getScheme();
      case FILE_SCHEME:
      case HTTPS_SCHEME:
        return FILE_SCHEME;
      default:
        throw new RuntimeException("Unsupported scheme in uri: " + uri);
    }
  }

  /**
   * The incoming URI could be of the following types:
   * 1. https://{pinot-controller endpoint}
   * 2. s3://{s3 endpoint}
   */
  public URI convertURI(URI uri) {
    // s3 endpoint is enabled.
    if (uri.getScheme().equals("s3")) {
      return uri;
    }
    if (uri.getHost().contains("pinot-controller")) {
      // this is a pinot-controller vip endpoint, form the s3 endpoint and inject controller data path to the s3 path.
      return URIUtils.getUri("s3://" + _bucketName + _s3DataPath + extractPath(uri.getPath().substring(1)));
    } else {
      throw new RuntimeException("Unsupported scheme in uri: " + uri);
    }
  }

  private String extractPath(String fullPath) {
    return fullPath.substring(fullPath.indexOf("/"));
  }

  @Override
  public boolean mkdir(URI uri)
      throws IOException {
    uri = convertURI(uri);
    return PINOT_FS_MAP.get(getScheme(uri)).mkdir(uri);
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException {
    segmentUri = convertURI(segmentUri);
    return PINOT_FS_MAP.get(getScheme(segmentUri)).delete(segmentUri, forceDelete);
  }

  @Override
  public boolean deleteBatch(List<URI> segmentUris, boolean forceDelete)
      throws IOException {
    boolean success = true;
    for (URI uri : segmentUris) {
      uri = convertURI(uri);
      success &= PINOT_FS_MAP.get(getScheme(uri)).delete(uri, forceDelete);
    }
    return success;
  }

  @Override
  public boolean move(URI srcUri, URI dstUri, boolean overwrite)
      throws IOException {
    srcUri = convertURI(srcUri);
    dstUri = convertURI(dstUri);
    return PINOT_FS_MAP.get(getScheme(srcUri)).move(srcUri, dstUri, overwrite);
  }

  @Override
  public boolean copyDir(URI srcUri, URI dstUri)
      throws IOException {
    srcUri = convertURI(srcUri);
    dstUri = convertURI(dstUri);
    return PINOT_FS_MAP.get(getScheme(srcUri)).copyDir(srcUri, dstUri);
  }

  @Override
  public boolean exists(URI fileUri)
      throws IOException {
    fileUri = convertURI(fileUri);
    return PINOT_FS_MAP.get(getScheme(fileUri)).exists(fileUri);
  }

  @Override
  public long length(URI fileUri)
      throws IOException {
    fileUri = convertURI(fileUri);
    return PINOT_FS_MAP.get(getScheme(fileUri)).length(fileUri);
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive)
      throws IOException {
    fileUri = convertURI(fileUri);
    return PINOT_FS_MAP.get(getScheme(fileUri)).listFiles(fileUri, recursive);
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception {
    srcUri = convertURI(srcUri);
    PINOT_FS_MAP.get(getScheme(srcUri)).copyToLocalFile(srcUri, dstFile);
  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception {
    dstUri = convertURI(dstUri);
    PINOT_FS_MAP.get(getScheme(dstUri)).copyFromLocalFile(srcFile, dstUri);
  }

  @Override
  public boolean isDirectory(URI uri)
      throws IOException {
    uri = convertURI(uri);
    return PINOT_FS_MAP.get(getScheme(uri)).isDirectory(uri);
  }

  @Override
  public long lastModified(URI uri)
      throws IOException {
    uri = convertURI(uri);
    return PINOT_FS_MAP.get(getScheme(uri)).lastModified(uri);
  }

  @Override
  public boolean touch(URI uri)
      throws IOException {
    uri = convertURI(uri);
    return PINOT_FS_MAP.get(getScheme(uri)).touch(uri);
  }

  @Override
  public InputStream open(URI uri)
      throws IOException {
    uri = convertURI(uri);
    return PINOT_FS_MAP.get(getScheme(uri)).open(uri);
  }
}
