package com.linkedin.thirdeye.bootstrap.startree;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.DimensionKey;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StarTreeJobUtils
{
  /**
   * Traverses tree structure and collects all combinations of record that are present (star/specific)
   */
  public static void collectRecords(StarTreeConfig config, StarTreeNode node, StarTreeRecord record, Map<UUID, StarTreeRecord> collector)
  {
    if (node.isLeaf())
    {
      collector.put(node.getId(), record);
    }
    else
    {
      StarTreeNode target = node.getChild(record.getDimensionKey().getDimensionValue(config.getDimensions(), node.getChildDimensionName()));
      if (target == null)
      {
        target = node.getOtherNode();
        record = record.aliasOther(target.getDimensionName());
      }
      collectRecords(config, target, record, collector);
      collectRecords(config, node.getStarNode(), record.relax(target.getDimensionName()), collector);
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
    int[] target = new int[dimensionKey.getDimensionValues().length];
    for (int i = 0; i < dimensionNames.size(); i++)
    {
      String dimensionName = dimensionNames.get(i);
      String dimensionValue = dimensionKey.getDimensionValues()[i];

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
