package com.linkedin.pinot.core.data.manager.config;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;


/**
 * The config used for ResourceDataManager.
 * 
 * @author xiafu
 *
 */
public class ResourceDataManagerConfig {

  public static String PARTITIONS = "partitions";
  public static String PARTITION_DOT = "partition.";
  public static String ID = "id";
  public static String TYPE = "type";
  public static String PARTITION_TYPE = "partitionType";
  public static String DIRECTORY = "directory";

  private Configuration _resourceDataManagerConfig = null;
  private int[] _paritionArray;
  private List<PartitionDataManagerConfig> _partitionDataManagerConfigList;

  public ResourceDataManagerConfig(Configuration resourceDataManagerConfig) throws ConfigurationException {
    _resourceDataManagerConfig = resourceDataManagerConfig;
    _paritionArray = buildPartitions(_resourceDataManagerConfig.getStringArray(PARTITIONS));
    _partitionDataManagerConfigList = new ArrayList<PartitionDataManagerConfig>();
    for (int i = 0; i < _paritionArray.length; ++i) {
      Configuration partitionConfig = _resourceDataManagerConfig.subset(PARTITION_DOT + i);
      partitionConfig.addProperty(ID, i);
      if (partitionConfig.getString(PARTITION_TYPE) == null) {
        partitionConfig.addProperty(TYPE, _resourceDataManagerConfig.getString(PARTITION_TYPE));
      }
      if (partitionConfig.getString(DIRECTORY) == null) {
        partitionConfig.addProperty(DIRECTORY, _resourceDataManagerConfig.getString(DIRECTORY) + "/shard" + i);
      }
      _partitionDataManagerConfigList.add(new PartitionDataManagerConfig(partitionConfig));
    }
  }

  public Configuration getConfig() {
    return _resourceDataManagerConfig;
  }

  public int[] getPartitionArray() throws ConfigurationException {
    return _paritionArray;
  }

  static final Pattern PARTITION_PATTERN = Pattern.compile("[\\d]+||[\\d]+-[\\d]+");

  public static int[] buildPartitions(String[] partitionArray) throws ConfigurationException {
    IntSet partitions = new IntOpenHashSet();
    try {
      for (int i = 0; i < partitionArray.length; ++i) {
        Matcher matcher = PARTITION_PATTERN.matcher(partitionArray[i]);
        if (!matcher.matches()) {
          throw new ConfigurationException("Invalid partition: " + partitionArray[i]);
        }
        String[] partitionRange = partitionArray[i].split("-");
        int start = Integer.parseInt(partitionRange[0]);
        int end;
        if (partitionRange.length > 1) {
          end = Integer.parseInt(partitionRange[1]);
          if (end < start) {
            throw new ConfigurationException("invalid partition range: " + partitionArray[i]);
          }
        } else {
          end = start;
        }

        for (int k = start; k <= end; ++k) {
          partitions.add(k);
        }
      }
    } catch (Exception e) {
      throw new ConfigurationException("Error parsing resource config : " + PARTITIONS + "="
          + Arrays.toString(partitionArray), e);
    }

    int[] ret = partitions.toIntArray();
    Arrays.sort(ret);
    return ret;
  }

  public PartitionDataManagerConfig getPartitionConfig(int i) {
    return _partitionDataManagerConfigList.get(i);
  }
}
