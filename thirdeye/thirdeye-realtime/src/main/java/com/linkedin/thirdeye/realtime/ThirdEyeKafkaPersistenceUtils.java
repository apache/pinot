package com.linkedin.thirdeye.realtime;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeCallback;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.storage.DimensionDictionary;
import com.linkedin.thirdeye.impl.storage.FixedBufferUtil;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

public class ThirdEyeKafkaPersistenceUtils
{
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeKafkaPersistenceUtils.class);
  private static final File THIRDEYE_TMP_DIR = new File(System.getProperty("java.io.tmpdir"), "THIRDEYE_TMP");
  private static final String LEAF_BUFFER_DIRECTORY_NAME = "leafBuffers";
  private static final String COMBINED_BUFFER_DIRECTORY_NAME = "combinedBuffers";

  public static void persistMetrics(final StarTree starTree, File metricStoreDirectory) throws IOException
  {
    // Clear any existing tmp directory
    if (THIRDEYE_TMP_DIR.exists())
    {
      FileUtils.forceDelete(THIRDEYE_TMP_DIR);
    }
    FileUtils.forceMkdir(THIRDEYE_TMP_DIR);

    // Write leaf buffers
    final File leafBufferDirectory = new File(THIRDEYE_TMP_DIR, LEAF_BUFFER_DIRECTORY_NAME);
    LOG.info("Persisting leaf buffers for {}", starTree.getConfig().getCollection());
    starTree.eachLeaf(new StarTreeCallback()
    {
      @Override
      public void call(StarTreeNode node)
      {
        if (node.getRecordStore() == null)
        {
          throw new IllegalArgumentException("Cannot persist node with null record store " + node.getId());
        }

        Map<DimensionKey, MetricTimeSeries> data = new HashMap<DimensionKey, MetricTimeSeries>();
        boolean hasData = false;
        for (StarTreeRecord record : node.getRecordStore())
        {
          if (!record.getMetricTimeSeries().getTimeWindowSet().isEmpty())
          {
            hasData = true;
          }
          data.put(record.getDimensionKey(), record.getMetricTimeSeries());
        }

        if (!hasData)
        {
          if (LOG.isDebugEnabled())
          {
            LOG.debug("No data for node {} will not write buffer", node.getId());
          }
          return;
        }

        DimensionDictionary dimensionDictionary = new DimensionDictionary(node.getRecordStore().getForwardIndex());

        try
        {
          FixedBufferUtil.createLeafBufferFiles(
                  leafBufferDirectory, node.getId().toString(), starTree.getConfig(), data, dimensionDictionary);
        }
        catch (IOException e)
        {
          throw new IllegalStateException(e);
        }
      }
    });
    LOG.info("Wrote leaf buffers to {}", leafBufferDirectory);

    // Combine leaf buffers
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream treeOutputStream = new ObjectOutputStream(baos);
    treeOutputStream.writeObject(starTree.getRoot());
    treeOutputStream.flush();
    InputStream treeInputStream = new ByteArrayInputStream(baos.toByteArray());
    final File combinedBufferDirectory = new File(THIRDEYE_TMP_DIR, COMBINED_BUFFER_DIRECTORY_NAME);
    FixedBufferUtil.combineDataFiles(treeInputStream, leafBufferDirectory, combinedBufferDirectory);
    LOG.info("Wrote combined leaf buffers for {}", starTree.getConfig().getCollection());

    // Copy metric buffers to metric store
    File tmpMetricBufferDirectory = new File(combinedBufferDirectory, StarTreeConstants.METRIC_STORE);
    File[] metricBuffers = tmpMetricBufferDirectory.listFiles();
    if (metricBuffers == null || metricBuffers.length == 0)
    {
      LOG.warn("There were no metric buffers in {}", tmpMetricBufferDirectory);
    }
    else
    {
      for (File metricBuffer : metricBuffers)
      {
        FileUtils.copyFileToDirectory(metricBuffer, metricStoreDirectory);
        LOG.info("Copied {} to {}", metricBuffer, metricStoreDirectory);
      }
      LOG.info("Successfully persisted metrics for {}", starTree.getConfig().getCollection());
    }
  }
}
