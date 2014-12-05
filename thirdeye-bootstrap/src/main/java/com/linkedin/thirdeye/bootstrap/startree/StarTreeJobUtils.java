package com.linkedin.thirdeye.bootstrap.startree;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class StarTreeJobUtils
{
  public static Map<String, Map<Long, StarTreeRecord>> aggregateRecords(
          StarTreeConfig config,
          Iterable<AvroValue<GenericRecord>> avroRecords,
          int numTimeBuckets)
  {
    Map<String, Map<Long, StarTreeRecord>> records = new HashMap<String, Map<Long, StarTreeRecord>>();

    // Aggregate records
    for (AvroValue<GenericRecord> avroRecord : avroRecords)
    {
      StarTreeRecord record = StarTreeUtils.toStarTreeRecord(config, avroRecord.datum());

      // Initialize buckets
      Map<Long, StarTreeRecord> timeBuckets = records.get(record.getKey(false));
      if (timeBuckets == null)
      {
        timeBuckets = new HashMap<Long, StarTreeRecord>();
        records.put(record.getKey(false), timeBuckets);
      }

      // Get bucket
      long bucket = record.getTime() % numTimeBuckets;

      // Merge or overwrite existing record
      StarTreeRecord aggRecord = timeBuckets.get(bucket);
      if (aggRecord == null || aggRecord.getTime() < record.getTime())
      {
        timeBuckets.put(bucket, record);
      }
      else if (aggRecord.getTime().equals(record.getTime()))
      {
        timeBuckets.put(bucket, StarTreeUtils.merge(Arrays.asList(record, aggRecord)));
      }
    }

    return records;
  }

  /**
   * Traverses tree structure and collects all combinations of record that are present (star/specific)
   */
  public static void collectRecords(StarTreeNode node, StarTreeRecord record, Map<UUID, StarTreeRecord> collector)
  {
    if (node.isLeaf())
    {
      collector.put(node.getId(), record);
    }
    else
    {
      StarTreeNode target = node.getChild(record.getDimensionValues().get(node.getChildDimensionName()));
      if (target == null)
      {
        target = node.getOtherNode();
        record = record.aliasOther(target.getDimensionName());
      }
      collectRecords(target, record, collector);
      collectRecords(node.getStarNode(), record.relax(target.getDimensionName()), collector);
    }
  }
}
