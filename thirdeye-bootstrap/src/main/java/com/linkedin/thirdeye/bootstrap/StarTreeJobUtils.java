package com.linkedin.thirdeye.bootstrap;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StarTreeJobUtils
{
  /**
   * Converts a StarTreeRecord to GenericRecord
   */
  public static GenericRecord toGenericRecord(StarTreeConfig config, Schema schema, StarTreeRecord record, GenericRecord reuse)
  {
    GenericRecord genericRecord;
    if (reuse != null)
    {
      genericRecord = reuse;
    }
    else
    {
      genericRecord = new GenericData.Record(schema);
    }

    // Dimensions
    for (Map.Entry<String, String> dimension : record.getDimensionValues().entrySet())
    {
      switch (getType(schema.getField(dimension.getKey()).schema()))
      {
        case INT:
          genericRecord.put(dimension.getKey(), Integer.valueOf(dimension.getValue()));
          break;
        case LONG:
          genericRecord.put(dimension.getKey(), Long.valueOf(dimension.getValue()));
          break;
        case FLOAT:
          genericRecord.put(dimension.getKey(), Float.valueOf(dimension.getValue()));
          break;
        case DOUBLE:
          genericRecord.put(dimension.getKey(), Double.valueOf(dimension.getValue()));
          break;
        case BOOLEAN:
          genericRecord.put(dimension.getKey(), Boolean.valueOf(dimension.getValue()));
          break;
        case STRING:
          genericRecord.put(dimension.getKey(), dimension.getValue());
          break;
        default:
          throw new IllegalStateException("Unsupported dimension type " + schema.getField(dimension.getKey()));
      }
    }

    // Metrics
    for (Map.Entry<String, Integer> metric : record.getMetricValues().entrySet())
    {
      switch (getType(schema.getField(metric.getKey()).schema()))
      {
        case INT:
          genericRecord.put(metric.getKey(), metric.getValue());
          break;
        case LONG:
          genericRecord.put(metric.getKey(), metric.getValue().longValue());
          break;
        case FLOAT:
          genericRecord.put(metric.getKey(), metric.getValue().floatValue());
          break;
        case DOUBLE:
          genericRecord.put(metric.getKey(), metric.getValue().doubleValue());
          break;
        default:
          throw new IllegalStateException("Invalid metric schema type: " + schema.getField(metric.getKey()));
      }
    }

    // Time
    switch (getType(schema.getField(config.getTimeColumnName()).schema()))
    {
      case INT:
        genericRecord.put(config.getTimeColumnName(), record.getTime().intValue());
        break;
      case LONG:
        genericRecord.put(config.getTimeColumnName(), record.getTime());
        break;
      default:
        throw new IllegalStateException("Invalid time schema type: " + schema.getField(config.getTimeColumnName()));
    }

    // (Assume values we didn't touch are time, and fill in w/ 0, as these will be unused)
    for (Schema.Field field : schema.getFields())
    {
      if (!record.getDimensionValues().containsKey(field.name())
              && !record.getMetricValues().containsKey(field.name())
              && !config.getTimeColumnName().equals(field.name()))
      {
        switch (getType(field.schema()))
        {
          case INT:
            genericRecord.put(field.name(), 0);
            break;
          case LONG:
            genericRecord.put(field.name(), 0L);
            break;
          default:
            throw new IllegalStateException("Invalid time schema type: " + field.schema().getType());
        }
      }
    }

    return genericRecord;
  }

  /**
   * Returns the type of a schema, handling ["null", {type}]-style optional fields.
   */
  public static Schema.Type getType(Schema schema)
  {
    Schema.Type type = null;

    if (Schema.Type.UNION.equals(schema.getType()))
    {
      List<Schema> schemas = schema.getTypes();
      for (Schema s : schemas)
      {
        if (!Schema.Type.NULL.equals(s.getType()))
        {
          type = s.getType();
        }
      }
    }
    else
    {
      type = schema.getType();
    }

    if (type == null)
    {
      throw new IllegalStateException("Could not unambiguously determine type of schema " + schema);
    }

    return type;
  }

  /**
   * Converts a GenericRecord to a StarTreeRecord
   */
  public static StarTreeRecord toStarTreeRecord(StarTreeConfig config, GenericRecord record)
  {
    StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();

    // Dimensions
    for (String dimensionName : config.getDimensionNames())
    {
      Object dimensionValue = record.get(dimensionName);
      if (dimensionValue == null)
      {
        throw new IllegalStateException("Record has no value for dimension " + dimensionName);
      }
      builder.setDimensionValue(dimensionName, dimensionValue.toString());
    }

    // Metrics (n.b. null -> 0L)
    for (String metricName : config.getMetricNames())
    {
      Object metricValue = record.get(metricName);
      if (metricValue == null)
      {
        metricValue = 0L;
      }
      builder.setMetricValue(metricName, ((Number) metricValue).intValue());
    }

    // Time
    Object time = record.get(config.getTimeColumnName());
    if (time == null)
    {
      throw new IllegalStateException("Record does not have time column " + config.getTimeColumnName() + ": " + record);
    }
    builder.setTime(((Number) time).longValue());

    return builder.build();
  }

  public static Map<String, Map<Long, StarTreeRecord>> aggregateRecords(
          StarTreeConfig config,
          Iterable<AvroValue<GenericRecord>> avroRecords,
          int numTimeBuckets)
  {
    Map<String, Map<Long, StarTreeRecord>> records = new HashMap<String, Map<Long, StarTreeRecord>>();

    // Aggregate records
    for (AvroValue<GenericRecord> avroRecord : avroRecords)
    {
      StarTreeRecord record = toStarTreeRecord(config, avroRecord.datum());

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
