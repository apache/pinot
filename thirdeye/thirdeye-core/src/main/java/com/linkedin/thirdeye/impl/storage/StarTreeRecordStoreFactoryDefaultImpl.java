package com.linkedin.thirdeye.impl.storage;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.api.TimeRange;

/**
 * Creates {@link StarTreeRecordStoreDefaultImpl}
 * <p>
 * This factory assumes the following directory structure:
 *
 * <pre>
 *     rootDir/
 *        collection/
 *          config.yml
 *          tree.bin
 *          data/
 *            dimensionStore/
 *              fileId.buf
 *              fileId.idx
 *              fileId.dict
 *            metricStore/
 *              fileId.buf
 *              fileId.idx
 * </pre>
 *
 * </p>
 * <p>
 * "someId" here is a unique identifier for the file, not
 * </p>
 * <p>
 * The .dict file will contain the term to id mapping for dimension values by name.
 * </p>
 * <p>
 * The .buf file stores the raw data in sorted dimension order. The dimension key at index i in
 * dimensionStore/fileId.buf corresponds to the metric time-series at index i in
 * metricStore/fileId.buf.
 * </p>
 * <p>
 * The dimensionStore/*.idx file contains entries in the following format:
 *
 * <pre>
 *      leafId fileId dictStartOffset length bufStartOffset length
 * </pre>
 *
 * </p>
 * <p>
 * The metricStore/*.idx file contains entries in the following format:
 *
 * <pre>
 *      leafId fileId startOffset length startTime endTime
 * </pre>
 *
 * This points to the region in the metricStore/*.buf file that has metric data corresponding to the
 * leafId.
 * </p>
 */
public class StarTreeRecordStoreFactoryDefaultImpl implements StarTreeRecordStoreFactory {
  public static String PROP_METRIC_STORE_MUTABLE = "metricStoreMutable";

  private static final Logger LOGGER = LoggerFactory
      .getLogger(StarTreeRecordStoreFactoryDefaultImpl.class);

  private final Object sync = new Object();

  // nodeId to dimension index
  private final Map<UUID, DimensionIndexEntry> dimensionIndex =
      new HashMap<UUID, DimensionIndexEntry>();

  // nodeId to metric index
  private final Map<UUID, List<MetricIndexEntry>> metricIndex =
      new HashMap<UUID, List<MetricIndexEntry>>();

  // nodeId to metric store
  private final Map<UUID, MetricStore> metricStores = new HashMap<UUID, MetricStore>();

  // nodeId to metric store listener
  private final Map<UUID, MetricStoreListener> metricStoreListeners =
      new HashMap<UUID, MetricStoreListener>();

  // fileId to buffer
  private final Map<UUID, ByteBuffer> dictionarySegments = new HashMap<UUID, ByteBuffer>();

  // fileId to buffer
  private final Map<UUID, ByteBuffer> dimensionSegments = new HashMap<UUID, ByteBuffer>();

  // fileId to buffer
  private final Map<UUID, ByteBuffer> metricSegments = new HashMap<UUID, ByteBuffer>();

  // fileId to descriptor
  private final Map<UUID, FileDescriptor> indexDescriptors = new HashMap<UUID, FileDescriptor>();

  // index fileId to dimension index entries
  private final Map<UUID, Set<DimensionIndexEntry>> dimensionIndexByFile =
      new HashMap<UUID, Set<DimensionIndexEntry>>();

  // index fileId to metric index entries
  private final Map<UUID, Set<MetricIndexEntry>> metricIndexByFile =
      new HashMap<UUID, Set<MetricIndexEntry>>();

  private final List<FileChannel> recordStoreFactoryFileChannels = new ArrayList<FileChannel>();

  private File rootDir;
  private boolean isInit;
  private StarTreeConfig starTreeConfig;
  private boolean metricStoreMutable;

  @Override
  public void init(File rootDir, StarTreeConfig starTreeConfig, Properties recordStoreConfig)
      throws IOException {
    synchronized (sync) {
      if (isInit) {
        return;
      }

      this.rootDir = rootDir;
      this.isInit = true;
      this.starTreeConfig = starTreeConfig;
      
      InputStream indexMetadataFile = new FileInputStream(new File(rootDir, StarTreeConstants.METADATA_FILE_NAME));
      Properties indexMetadataProps = new Properties();
      indexMetadataProps.load(indexMetadataFile);
      indexMetadataFile.close();
      
      indexMetadata = IndexMetadata.fromProperties(indexMetadataProps);

      //check if the data needs to be converted
      if(indexMetadata.getIndexFormat().equals(IndexFormat.FIXED_SIZE)){
        LOGGER.info("START: Converting data from Fixed format to  Variable format at {}", rootDir);
        FixedToVariableFormatConvertor convertor = new FixedToVariableFormatConvertor(rootDir);
        convertor.convert();
        
        LOGGER.info("DONE: Converted data from Fixed format to  Variable format at {}", rootDir);
      }
      if (recordStoreConfig != null) {
        Object metricStoreMutableProp = recordStoreConfig.get(PROP_METRIC_STORE_MUTABLE);

        if (metricStoreMutableProp != null) {
          if (metricStoreMutableProp instanceof String) {
            this.metricStoreMutable = Boolean.valueOf((String) metricStoreMutableProp);
          } else {
            this.metricStoreMutable = (Boolean) metricStoreMutableProp;
          }
        }
      }

      File dimensionStore = new File(rootDir, StarTreeConstants.DIMENSION_STORE);
      FileUtils.forceMkdir(dimensionStore);
      File[] dimensionIndexFiles = dimensionStore.listFiles(INDEX_FILE_FILTER);
      if (dimensionIndexFiles != null) {
        for (File indexFile : dimensionIndexFiles) {
          loadDimensionIndex(indexFile);
        }
      }

      File metricStore = new File(rootDir, StarTreeConstants.METRIC_STORE);
      FileUtils.forceMkdir(metricStore);
      File[] metricIndexFiles = metricStore.listFiles(INDEX_FILE_FILTER);
      if (metricIndexFiles != null) {
        for (File indexFile : metricIndexFiles) {
          loadMetricIndex(indexFile);
        }
      }

      loadDimensionBuffers(dimensionIndex.values());

      for (List<MetricIndexEntry> entryGroup : metricIndex.values()) {
        loadMetricBuffers(entryGroup);
      }
    }
  }

  @Override
  public StarTreeRecordStore createRecordStore(UUID nodeId) throws IOException {
    synchronized (sync) {
      // Dimension store
      DimensionIndexEntry dimensionIndexEntry = dimensionIndex.get(nodeId);
      if (dimensionIndexEntry == null) {
        throw new IllegalArgumentException("No dimension index entry for " + nodeId);
      }
      DimensionDictionary dictionary = getDictionary(dimensionIndexEntry);
      ByteBuffer dimensionBuffer = getDimensionBuffer(dimensionIndexEntry);
      DimensionStore dimensionStore =
          new DimensionStoreImmutableImpl(starTreeConfig, dimensionBuffer, dictionary);

      // Metric store
      ConcurrentMap<TimeRange, List<ByteBuffer>> metricBuffers =
          new ConcurrentHashMap<TimeRange, List<ByteBuffer>>();
      List<MetricIndexEntry> metricIndexEntries = metricIndex.get(nodeId);
      if (metricIndexEntries != null) {
        for (MetricIndexEntry indexEntry : metricIndexEntries) {
          List<ByteBuffer> bufferList = metricBuffers.get(indexEntry.getTimeRange());
          if (bufferList == null) {
            bufferList = new CopyOnWriteArrayList<ByteBuffer>();
            metricBuffers.put(indexEntry.getTimeRange(), bufferList);
          }
          bufferList.add(getMetricBuffer(indexEntry));
        }
      }

      MetricStore metricStore;
      if (metricStoreMutable) {
        metricStore = new MetricStoreMutableImpl(starTreeConfig);
      } else {
        MetricStoreImmutableImpl immutableStore =
            new MetricStoreImmutableImpl(starTreeConfig, metricBuffers);
        metricStoreListeners.put(nodeId, immutableStore);
        metricStore = immutableStore;
      }
      metricStores.put(nodeId, metricStore);

      return new StarTreeRecordStoreDefaultImpl(starTreeConfig, dimensionStore, metricStore);
    }
  }

  private void loadDimensionIndex(File indexFile) throws IOException {
    List<DimensionIndexEntry> entries = StorageUtils.readDimensionIndex(indexFile);

    FileDescriptor fileDescriptor =
        FileDescriptor.fromString(indexFile.getName(), StarTreeConstants.INDEX_FILE_SUFFIX);

    dimensionIndexByFile.put(fileDescriptor.getId(), new HashSet<DimensionIndexEntry>());
    indexDescriptors.put(fileDescriptor.getId(), fileDescriptor);

    for (DimensionIndexEntry entry : entries) {
      dimensionIndex.put(entry.getNodeId(), entry);
      dimensionIndexByFile.get(fileDescriptor.getId()).add(entry);
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Loaded dimension index {}", indexFile);
    }
  }

  private void loadMetricIndex(File indexFile) throws IOException {
    List<MetricIndexEntry> entries = StorageUtils.readMetricIndex(indexFile);

    FileDescriptor fileDescriptor =
        FileDescriptor.fromString(indexFile.getName(), StarTreeConstants.INDEX_FILE_SUFFIX);

    metricIndexByFile.put(fileDescriptor.getId(), new HashSet<MetricIndexEntry>());
    indexDescriptors.put(fileDescriptor.getId(), fileDescriptor);

    for (MetricIndexEntry entry : entries) {
      List<MetricIndexEntry> nodeEntries = metricIndex.get(entry.getNodeId());
      if (nodeEntries == null) {
        nodeEntries = new ArrayList<MetricIndexEntry>();
        metricIndex.put(entry.getNodeId(), nodeEntries);
      }
      nodeEntries.add(entry);
      metricIndexByFile.get(fileDescriptor.getId()).add(entry);
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Loaded metric index {}", indexFile);
    }
  }

  private void loadDimensionBuffers(Collection<DimensionIndexEntry> indexEntries)
      throws IOException {
    File dimensionStore = new File(rootDir, StarTreeConstants.DIMENSION_STORE);
    for (DimensionIndexEntry indexEntry : indexEntries) {
      FileDescriptor associatedDescriptor = indexDescriptors.get(indexEntry.getFileId());
      if (associatedDescriptor == null) {
        throw new IllegalStateException("No index descriptor for " + indexEntry);
      }

      if (!dimensionSegments.containsKey(indexEntry.getFileId())) {
        File bufferFile =
            new File(dimensionStore,
                associatedDescriptor.toString(StarTreeConstants.BUFFER_FILE_SUFFIX));
        dimensionSegments.put(indexEntry.getFileId(), mapBuffer(bufferFile));
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Loaded buffer file {}", bufferFile);
        }
      }

      if (!dictionarySegments.containsKey(indexEntry.getFileId())) {
        File bufferFile =
            new File(dimensionStore,
                associatedDescriptor.toString(StarTreeConstants.DICT_FILE_SUFFIX));
        dictionarySegments.put(indexEntry.getFileId(), mapBuffer(bufferFile));
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Loaded buffer file {}", bufferFile);
        }
      }
    }
  }

  private void loadMetricBuffers(Collection<MetricIndexEntry> indexEntries) throws IOException {
    File metricStore = new File(rootDir, StarTreeConstants.METRIC_STORE);
    for (MetricIndexEntry indexEntry : indexEntries) {
      FileDescriptor associatedDescriptor = indexDescriptors.get(indexEntry.getFileId());
      if (associatedDescriptor == null) {
        throw new IllegalStateException("No index descriptor for " + indexEntry);
      }

      if (!metricSegments.containsKey(indexEntry.getFileId())) {
        File bufferFile =
            new File(metricStore,
                associatedDescriptor.toString(StarTreeConstants.BUFFER_FILE_SUFFIX));
        metricSegments.put(indexEntry.getFileId(), mapBuffer(bufferFile));
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Loaded buffer file {}", bufferFile);
        }
      }
    }
  }

  private DimensionDictionary getDictionary(DimensionIndexEntry indexEntry) throws IOException {
    ByteBuffer dictionaryBuffer = dictionarySegments.get(indexEntry.getFileId());
    if (dictionaryBuffer == null) {
      throw new IllegalStateException("No mapped buffer for file " + indexEntry.getFileId()
          + StarTreeConstants.DICT_FILE_SUFFIX);
    }

    dictionaryBuffer.rewind();
    dictionaryBuffer.position(indexEntry.getDictionaryStartOffset());
    byte[] dictionaryBytes = new byte[indexEntry.getDictionaryLength()];
    dictionaryBuffer.get(dictionaryBytes);
    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(dictionaryBytes));

    DimensionDictionary dictionary;
    try {
      dictionary = (DimensionDictionary) ois.readObject();
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

    return dictionary;
  }

  private ByteBuffer getDimensionBuffer(DimensionIndexEntry indexEntry) throws IOException {
    ByteBuffer dimensionBuffer = dimensionSegments.get(indexEntry.getFileId());
    if (dimensionBuffer == null) {
      throw new IllegalStateException("No mapped buffer for file " + indexEntry.getFileId()
          + StarTreeConstants.BUFFER_FILE_SUFFIX);
    }

    dimensionBuffer.rewind();
    dimensionBuffer.position(indexEntry.getBufferStartOffset());

    ByteBuffer slicedBuffer = dimensionBuffer.slice();
    slicedBuffer.limit(indexEntry.getBufferLength());

    return slicedBuffer;
  }

  private ByteBuffer getMetricBuffer(MetricIndexEntry indexEntry) throws IOException {
    ByteBuffer metricBuffer = metricSegments.get(indexEntry.getFileId());
    if (metricBuffer == null) {
      throw new IllegalStateException("No mapped buffer for file " + indexEntry.getFileId()
          + StarTreeConstants.BUFFER_FILE_SUFFIX);
    }

    metricBuffer.rewind();
    metricBuffer.position(indexEntry.getStartOffset());

    ByteBuffer slicedBuffer = metricBuffer.slice();
    slicedBuffer.limit(indexEntry.getLength());

    return slicedBuffer;
  }

  private ByteBuffer mapBuffer(File bufferFile) throws IOException {
    FileChannel channel = new RandomAccessFile(bufferFile, "r").getChannel();
    recordStoreFactoryFileChannels.add(channel);
    ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, bufferFile.length());
    buffer.order(ByteOrder.BIG_ENDIAN);
    return buffer;
  }

  public void close() throws IOException {

    LOGGER.info("Closing record store factory");
    for (FileChannel recordStoreFactoryFileChannel : recordStoreFactoryFileChannels) {
      recordStoreFactoryFileChannel.close();
    }
  }

  private static final FileFilter INDEX_FILE_FILTER = new FileFilter() {
    @Override
    public boolean accept(File file) {
      return file.getName().endsWith(StarTreeConstants.INDEX_FILE_SUFFIX);
    }
  };

  private IndexMetadata indexMetadata;

  private static class FileDescriptor {
    private final UUID id;
    private String suffix;

    FileDescriptor(UUID id, String suffix) {
      this.id = id;
      this.suffix = suffix;
    }

    UUID getId() {
      return id;
    }

    @Override
    public String toString() {
      return id + suffix;
    }

    public String toString(String alternateSuffix) {
      return id + alternateSuffix;
    }

    static FileDescriptor fromString(String fileName, String expectedSuffix) {
      UUID id = UUID.fromString(fileName.substring(0, fileName.indexOf(expectedSuffix)));
      return new FileDescriptor(id, expectedSuffix);
    }
  }
}
