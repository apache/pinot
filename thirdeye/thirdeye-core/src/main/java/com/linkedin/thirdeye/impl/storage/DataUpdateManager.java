package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.*;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryHashMapImpl;
import com.linkedin.thirdeye.impl.TarGzCompressionUtils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DataUpdateManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataUpdateManager.class);

  private final File rootDir;
  private final ConcurrentMap<String, Lock> collectionLocks;

  /**
   * A utility to manage data on the file system.
   *
   * @param rootDir
   *  The root directory on the file system under which collection data is stored
   */
  public DataUpdateManager(File rootDir) {
    this.rootDir = rootDir;
    this.collectionLocks = new ConcurrentHashMap<String, Lock>();
  }

  /**
   * Deletes all data for a collection (i.e. rm -rf rootDir/collection).
   */
  public void deleteCollection(String collection) throws Exception {
    File collectionDir = new File(rootDir, collection);

    if (!collectionDir.isAbsolute()) {
      throw new IllegalArgumentException("Collection dir cannot be relative " + collectionDir);
    }

    FileUtils.forceDelete(collectionDir);
  }

  /**
   * Deletes a specific data directory for a collection (min/max time must be exact).
   */
  public void deleteData(String collection,
                         String schedule,
                         DateTime minTime,
                         DateTime maxTime) throws Exception {
    Lock lock = collectionLocks.get(collection);
    if (lock == null) {
      collectionLocks.putIfAbsent(collection, new ReentrantLock());
      lock = collectionLocks.get(collection);
    }

    lock.lock();
    LOGGER.info("Locked collection {} using lock {} for data delete", collection, lock);
    try {
      // Find files prefixed with the parameters (i.e. not including treeId)
      final String dataDirPrefix = StorageUtils.getDataDirPrefix(schedule, minTime, maxTime);
      File collectionDir = new File(rootDir, collection);
      File[] matchingDirs = collectionDir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.startsWith(dataDirPrefix);
        }
      });

      if (matchingDirs == null || matchingDirs.length == 0) {
        throw new FileNotFoundException("No directory with prefix " + dataDirPrefix);
      }

      for (File dataDir : matchingDirs) {
        FileUtils.forceDelete(dataDir); // n.b. will trigger watch on collection dir
        LOGGER.info("Deleted {}", dataDir);
      }
    } finally {
      lock.unlock();
      LOGGER.info("Unlocked collection {} using lock {} for data delete", collection, lock);
    }
  }
  public void updateData(String collection,
                         String schedule,
                         DateTime minTime,
                         DateTime maxTime,
                         byte[] data) throws Exception {
    updateData(collection, schedule, minTime, maxTime, new ByteArrayInputStream(data));
  }

  /**
   * Loads a data segment.
   *
   * @param collection
   *  The collection name (store in rootDir/collection)
   * @param schedule
   *  E.g. HOURLY or DAILY
   * @param minTime
   *  The lower bound wall-clock time
   * @param maxTime
   *  The upper bound wall-clock time
   * @param data
   *  A gzipped tar archive containing data in the appropriate directory structure
   */
  public void updateData(String collection,
                         String schedule,
                         DateTime minTime,
                         DateTime maxTime,
                         InputStream data) throws Exception {
    Lock lock = collectionLocks.get(collection);
    if (lock == null) {
      collectionLocks.putIfAbsent(collection, new ReentrantLock());
      lock = collectionLocks.get(collection);
    }

    lock.lock();
    LOGGER.info("Locked collection {} using lock {} for data update", collection, lock);
    try {
      File collectionDir = new File(rootDir, collection);
      if (!collectionDir.exists()) {
        FileUtils.forceMkdir(collectionDir);
        LOGGER.info("Created {}", collectionDir);
      }

      if (schedule.contains("_")) {
        throw new IOException("schedule cannot contain '_'");
      }

      String loadId = "load_" + UUID.randomUUID();
      File tmpDir = new File(new File(rootDir, collection), loadId);

      try {
        // Extract into tmp dir
        FileUtils.forceMkdir(tmpDir);
        File tarGzFile = new File(tmpDir, "data.tar.gz");
        OutputStream os = new FileOutputStream(tarGzFile);
        IOUtils.copy(data, os);
        os.close();
        TarGzCompressionUtils.unTar(tarGzFile, tmpDir);
        LOGGER.info("Extracted data into {}", tmpDir);

        // Read tree to get ID
        File tmpTreeFile = new File(tmpDir, StarTreeConstants.TREE_FILE_NAME);
        ObjectInputStream treeStream = new ObjectInputStream(new FileInputStream(tmpTreeFile));
        StarTreeNode rootNode = (StarTreeNode) treeStream.readObject();
        String treeId = rootNode.getId().toString();
        LOGGER.info("Tree ID for {} is {}", loadId, treeId);

        // Move into data dir
        File dataDir = new File(collectionDir, StorageUtils.getDataDirName(treeId, schedule, minTime, maxTime));
        if (dataDir.exists()) {
          throw new Exception("Data is already uploaded for timerange:" + minTime + " to "
              + maxTime + ". Please delete the existing data for this range and try again");
        }
        FileUtils.forceMkdir(dataDir);
        StorageUtils.moveAllFiles(tmpDir, dataDir);
        LOGGER.info("Moved files from {} to {}", tmpDir, dataDir);

        // Touch data dir to trigger watch service
        if (!dataDir.setLastModified(System.currentTimeMillis())) {
          LOGGER.warn("setLastModified on dataDir failed - watch service will not be triggered!");
        }
      } finally {
        FileUtils.forceDelete(tmpDir);
        LOGGER.info("Deleted tmp dir {}", tmpDir);
      }
    } finally {
      lock.unlock();
      LOGGER.info("Unlocked collection {} using lock {} for data update", collection, lock);
    }
  }

  /**
   * Creates a data segment from the data that's currently stored in an index.
   *
   * @param schedule
   *  The schedule at which the index is being persisted
   * @param minTime
   *  The wall-clock time that the first record was added to the tree
   * @param maxTime
   *  The wall-clock time that the last record was added to the tree
   * @param starTree
   *  The index to persist
   */
  public void persistTree(String collection,
                          String schedule,
                          DateTime minTime,
                          DateTime maxTime,
                          final StarTree starTree) throws Exception {
    Lock lock = collectionLocks.get(collection);
    if (lock == null) {
      collectionLocks.putIfAbsent(collection, new ReentrantLock());
      lock = collectionLocks.get(collection);
    }

    lock.lock();
    LOGGER.info("Locked collection {} using lock {} for persist tree", collection, lock);
    try {
      File collectionDir = new File(rootDir, collection);
      if (!collectionDir.exists()) {
        FileUtils.forceMkdir(collectionDir);
        LOGGER.info("Created {}", collectionDir);
      }

      if (schedule.contains("_")) {
        throw new IOException("schedule cannot contain '_'");
      }

      // Create temp directory
      String persistId = "persist_" + UUID.randomUUID();
      final File tmpDir = new File(new File(rootDir, collection), persistId);
      final File leafBufferDir = new File(tmpDir, "leafBuffers");
      final File segmentBufferDir = new File(tmpDir, "segmentBuffers");
      LOGGER.info("Beginning persist {}", persistId);

      // Create leaf buffer files in that directory
      LOGGER.info("Creating leaf buffer files in {}", leafBufferDir);
      final AtomicLong minDataTime = new AtomicLong(-1);
      final AtomicLong maxDataTime = new AtomicLong(-1);
      starTree.eachLeaf(new StarTreeCallback() {
        @Override
        public void call(StarTreeNode node) {
          try {
            Map<DimensionKey, MetricTimeSeries> records = new HashMap<>();
            for (StarTreeRecord record : node.getRecordStore()) {
              MetricTimeSeries timeSeries = record.getMetricTimeSeries();
              for (Long time : timeSeries.getTimeWindowSet()) {
                if (minDataTime.get() == -1 || minDataTime.get() > time) {
                  minDataTime.set(time);
                }

                if (maxDataTime.get() == -1 || maxDataTime.get() < time) {
                  maxDataTime.set(time);
                }
              }
              records.put(record.getDimensionKey(), timeSeries);
            }

            // Add a catch-all record
            List<DimensionSpec> dimensions = starTree.getConfig().getDimensions();
            String[] catchAll = new String[dimensions.size()];
            Arrays.fill(catchAll, StarTreeConstants.OTHER);
            for (int i = 0; i < dimensions.size(); i++) {
              String name = dimensions.get(i).getName();
              String value = node.getAncestorDimensionValues().get(name);
              if (value != null) {
                catchAll[i] = value;
              }
            }
            DimensionKey catchAllKey = new DimensionKey(catchAll);
            List<MetricSpec> metrics = starTree.getConfig().getMetrics();
            MetricTimeSeries timeSeries = new MetricTimeSeries(MetricSchema.fromMetricSpecs(metrics)); // empty
            if (!records.containsKey(catchAllKey)) {
              records.put(catchAllKey, timeSeries);
            }

            DimensionDictionary dictionary = new DimensionDictionary(node.getRecordStore().getForwardIndex());
            FixedBufferUtil.createLeafBufferFiles(leafBufferDir, node.getId().toString(), starTree.getConfig(), records, dictionary);
          } catch (Exception e) {
            LOGGER.error("Error creating leaf buffer files for {}", node.getId(), e);
          }
        }
      });

      // Create tree output stream
      LOGGER.info("Serializing star tree");
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(starTree.getRoot());
      oos.flush();
      InputStream treeStream = new ByteArrayInputStream(baos.toByteArray());

      // Combine those into the segment buffers
      LOGGER.info("Combining data files into {}", segmentBufferDir);
      FixedBufferUtil.combineDataFiles(treeStream, leafBufferDir, segmentBufferDir);

      // Create index metadata
      File metadataFile = new File(segmentBufferDir, StarTreeConstants.METADATA_FILE_NAME);
      LOGGER.info("Creating index metadata {}", metadataFile);
      IndexMetadata metadata = new IndexMetadata(minDataTime.get(), maxDataTime.get());
      OutputStream metadataStream = new FileOutputStream(metadataFile);
      metadata.toProperties().store(metadataStream, "This segment was created via DataUpdateManager#persistTree");
      metadataStream.close();

      // Move the segment buffers into actual data directory
      String treeId = starTree.getRoot().getId().toString();
      File dataDir = new File(collectionDir, StorageUtils.getDataDirName(treeId, schedule, minTime, maxTime));
      if (dataDir.exists()) {
        throw new Exception("Data is already persisted for timerange:" + minTime + " to " + maxTime);
      }
      LOGGER.info("Moving segments into {}", dataDir);
      FileUtils.forceMkdir(dataDir);
      StorageUtils.moveAllFiles(segmentBufferDir, dataDir);

      // Touch data dir to trigger watch service
      if (!dataDir.setLastModified(System.currentTimeMillis())) {
        LOGGER.warn("setLastModified on dataDir failed - watch service will not be triggered!");
      }

      // Remove tmp dir
      LOGGER.info("Removing tmp directory {}", tmpDir);
      FileUtils.forceDelete(tmpDir);
    } finally {
      lock.unlock();
      LOGGER.info("Unlocked collection {} using lock {} for persist tree", collection, lock);
    }
  }

  /**
   * Returns a merged tree with all dimensions and data.
   *
   * @param starTrees
   *  A collection of star trees all with the same configuration.
   */
  public StarTree mergeTrees(Collection<StarTree> starTrees) throws Exception {
    // Create the merged tree structure
    StarTreeConfig baseConfig = starTrees.iterator().next().getConfig();
    StarTreeConfig inMemoryConfig = new StarTreeConfig(baseConfig.getCollection(),
        StarTreeRecordStoreFactoryHashMapImpl.class.getCanonicalName(),
        new Properties(),
        baseConfig.getAnomalyDetectionFunctionClass(),
        baseConfig.getAnomalyDetectionFunctionConfig(),
        baseConfig.getAnomalyHandlerClass(),
        baseConfig.getAnomalyHandlerConfig(),
        baseConfig.getAnomalyDetectionMode(),
        baseConfig.getDimensions(),
        baseConfig.getMetrics(),
        baseConfig.getTime(),
        baseConfig.getJoinSpec(),
        baseConfig.getRollup(),
        baseConfig.getSplit(),
        false);
    final StarTree mergedTree = new StarTreeImpl(inMemoryConfig);
    mergedTree.open();

    // Add all records to the merged tree
    for (StarTree starTree : starTrees) {
      starTree.eachLeaf(new StarTreeCallback() {
        @Override
        public void call(StarTreeNode node) {
          for (StarTreeRecord record : node.getRecordStore()) {
            mergedTree.add(record);
          }
        }
      });
    }

    return mergedTree;
  }

  /**
   * Performs in-memory roll-up on a star tree, and returns the rolled up tree.
   */
  public StarTree rollUp(final StarTree starTree) throws Exception {
    if (starTree.getConfig().getRollup() == null) {
      LOGGER.warn("Rollup is null, will just use same tree");
      return starTree;
    }

    RollupSpec rollup = starTree.getConfig().getRollup();
    Constructor<?> constructor = Class.forName(rollup.getFunctionClass()).getConstructor(Map.class);
    final RollupThresholdFunction threshold = (RollupThresholdFunction) constructor.newInstance(rollup.getFunctionConfig());
    LOGGER.info("Rolling up using function {}", threshold);

    // Iterate over all dimension combinations, split into those below / above threshold
    final Map<DimensionKey, MetricTimeSeries> aboveThreshold = new HashMap<>();
    final Map<DimensionKey, MetricTimeSeries> belowThreshold = new HashMap<>();
    starTree.eachLeaf(new StarTreeCallback() {
      @Override
      public void call(StarTreeNode node) {
        for (StarTreeRecord record : node.getRecordStore()) {
          if (threshold.isAboveThreshold(record.getMetricTimeSeries())) {
            updateMap(aboveThreshold, record.getDimensionKey(), record.getMetricTimeSeries());
          } else {
            updateMap(belowThreshold, record.getDimensionKey(), record.getMetricTimeSeries());
          }
        }
      }
    });
    LOGGER.info("Above threshold = {}, below threshold = {}", aboveThreshold.size(), belowThreshold.size());

    // Get mapping of dimension name to position
    Map<String, Integer> nameToPosition = new HashMap<>();
    for (int i = 0; i < starTree.getConfig().getDimensions().size(); i++) {
      DimensionSpec dimensionSpec = starTree.getConfig().getDimensions().get(i);
      nameToPosition.put(dimensionSpec.getName(), i);
    }

    // Generate aggregates for all combinations for those below threshold
    LOGGER.info("Generating aggregates for all combinations below threshold...");
    Map<DimensionKey, MetricTimeSeries> allCombinations = new HashMap<>();
    Map<DimensionKey, Set<DimensionKey>> allOptions = new HashMap<>();
    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : belowThreshold.entrySet()) {
      String[] oldValues = entry.getKey().getDimensionValues();
      for (String dimensionName : rollup.getOrder()) {
        // Aggregate
        String[] newValues = Arrays.copyOf(oldValues, oldValues.length);
        int idx = nameToPosition.get(dimensionName);
        newValues[idx] = StarTreeConstants.OTHER;
        DimensionKey newKey = new DimensionKey(newValues);
        updateMap(allCombinations, newKey, entry.getValue());

        // Record option
        Set<DimensionKey> options = allOptions.get(entry.getKey());
        if (options == null) {
          options = new HashSet<>();
          allOptions.put(entry.getKey(), options);
        }
        options.add(newKey);
      }
    }

    // Find combination with least others that passes the threshold and select that
    LOGGER.info("Selecting combination with least others for all combinations below threshold...");
    Map<DimensionKey, MetricTimeSeries> selectedCombinations = new HashMap<>();

    // Add all other record
    String[] allOther = new String[starTree.getConfig().getDimensions().size()];
    Arrays.fill(allOther, StarTreeConstants.OTHER);
    DimensionKey allOtherKey = new DimensionKey(allOther);
    MetricSchema metricSchema = MetricSchema.fromMetricSpecs(starTree.getConfig().getMetrics());
    selectedCombinations.put(allOtherKey, new MetricTimeSeries(metricSchema));

    for (Map.Entry<DimensionKey, Set<DimensionKey>> entry : allOptions.entrySet()) {
      DimensionKey selected = null;
      int minOther = Integer.MAX_VALUE;
      for (DimensionKey option : entry.getValue()) {
        // Count others
        int numOther = 0;
        for (int i = 0; i < option.getDimensionValues().length; i++) {
          if (StarTreeConstants.OTHER.equals(option.getDimensionValues()[i])) {
            numOther++;
          }
        }

        // Apply threshold function
        MetricTimeSeries rollUpSeries = allCombinations.get(option);
        if (numOther < minOther && threshold.isAboveThreshold(rollUpSeries)) {
          minOther = numOther;
          selected = option;
        }
      }

      // Just use all others if can't find combination
      if (selected == null) {
        selected = allOtherKey;
      }

      // Add the record's raw values to the aggregate for selected key
      MetricTimeSeries rawTimeSeries = belowThreshold.get(entry.getKey());
      updateMap(selectedCombinations, selected, rawTimeSeries);
    }

    // Create new star tree from both
    LOGGER.info("Creating rolled up star tree...");
    StarTree rolledUpTree = new StarTreeImpl(starTree.getConfig());
    rolledUpTree.open();
    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : aboveThreshold.entrySet()) {
      rolledUpTree.add(new StarTreeRecordImpl(starTree.getConfig(), entry.getKey(), entry.getValue()));
    }

    return rolledUpTree;
  }

  private static void updateMap(Map<DimensionKey, MetricTimeSeries> map,
                                DimensionKey key,
                                MetricTimeSeries timeSeries) {
    MetricTimeSeries series = map.get(key);
    if (series == null) {
      series = new MetricTimeSeries(timeSeries.getSchema());
      map.put(key, series);
    }
    series.aggregate(timeSeries);
  }
}
