package com.linkedin.thirdeye.bootstrap.startree;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

  /**
   * Given a fixed list of combinations, finds the combination with fewest "other" values that dimensionKey
   * maps to, and returns the integer representation of that combination.
   */
  public static int[] findBestMatch(DimensionKey dimensionKey,
                                    List<String> dimensionNames,
                                    List<int[]> dimensionCombinations,
                                    Map<String, Map<String, Integer>> forwardIndex)
  {
    // Convert dimension key
    int[] target = new int[dimensionKey.getDimensionsValues().length];
    for (int i = 0; i < dimensionNames.size(); i++)
    {
      String dimensionName = dimensionNames.get(i);
      String dimensionValue = dimensionKey.getDimensionsValues()[i];

      Integer intValue = forwardIndex.get(dimensionName).get(dimensionValue);
      if (intValue == null)
      {
        //TODO: this check is only valid for dimensions that are already split. 
       // throw new IllegalArgumentException("No mapping for " + dimensionName + ":" + dimensionValue + " in index");
        
        intValue = -1;
      }

      target[i] = intValue;
    }

    // Find node with least others
    int[] closestCombination = null;
    Integer closestScore = null;
    for (int[] combination : dimensionCombinations)
    {
      int score = 0;
      for (int i = 0; i < target.length; i++)
      {
        if (target[i] != combination[i])
        {
          if (combination[i] == StarTreeConstants.OTHER_VALUE)
          {
            score += 1;
          }
          else if (combination[i] == StarTreeConstants.STAR_VALUE)
          {
            score += 0;
          } 
          else if(target[i] == StarTreeConstants.STAR_VALUE)
          {
            score += 0;
          }
          else
          {
            score = -1;
            break;
          }
        }
        // else, match and contribute 0
      }
      if (score >= 0 && (closestScore == null || score < closestScore))
      {
        closestScore = score;
        closestCombination = combination;
      }
    }

    // Check
    if (closestCombination == null)
    {
      StringBuilder sb = new StringBuilder();
      for(int[] combination:dimensionCombinations){
        sb.append(Arrays.toString(combination));
        sb.append("\n");
      }
      throw new IllegalArgumentException("Could not find matching combination for " + dimensionKey + " in \n" + sb.toString() +"\n"+ " forwardIndex:"+ forwardIndex);
    }

    return closestCombination;
  }
}
