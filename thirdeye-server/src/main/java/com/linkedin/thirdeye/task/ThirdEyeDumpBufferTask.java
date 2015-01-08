package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreCircularBufferImpl;
import io.dropwizard.servlets.tasks.Task;

import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;

public class ThirdEyeDumpBufferTask extends Task
{
  private final StarTreeManager manager;

  public ThirdEyeDumpBufferTask(StarTreeManager manager)
  {
    super("dumpBuffer");
    this.manager = manager;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception
  {
    Collection<String> collectionParam = params.get("collection");
    if (collectionParam == null || collectionParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide collection");
    }
    String collection = collectionParam.iterator().next();

    Collection<String> idParam = params.get("id");
    if (idParam == null || idParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide id");
    }
    String id = idParam.iterator().next();

    StarTree starTree = manager.getStarTree(collection);
    if (starTree == null)
    {
      throw new IllegalArgumentException("No star tree for collection " + collection);
    }

    StarTreeNode node = findNode(starTree.getRoot(), UUID.fromString(id));
    if (node == null)
    {
      throw new IllegalArgumentException("No buffer with id " + id);
    }

    // n.b. this is some terrible code, but just intended to inspect buffers

    if (node.getRecordStore() instanceof StarTreeRecordStoreCircularBufferImpl)
    {
      Field field = node.getRecordStore().getClass().getDeclaredField("buffer");
      field.setAccessible(true);
      ByteBuffer buffer = (ByteBuffer) field.get(node.getRecordStore());
      buffer.clear();
      StarTreeRecordStoreCircularBufferImpl.dumpBuffer(
              buffer,
              printWriter,
              starTree.getConfig().getDimensionNames(),
              starTree.getConfig().getMetricNames(),
              Integer.valueOf(starTree.getConfig().getRecordStoreFactoryConfig().getProperty("numTimeBuckets")));
      printWriter.flush();
    }
    else
    {
      throw new IllegalStateException("Buffer dump not supported");
    }
  }

  private StarTreeNode findNode(StarTreeNode node, UUID uuid)
  {
    if (node.isLeaf())
    {
      return node.getId().equals(uuid) ? node : null;
    }
    else
    {
      StarTreeNode targetNode = null;

      for (StarTreeNode child : node.getChildren())
      {
        targetNode = findNode(child, uuid);
        if (targetNode != null)
        {
          break;
        }
      }

      if (targetNode == null)
      {
        targetNode = findNode(node.getOtherNode(), uuid);
      }

      if (targetNode == null)
      {
        targetNode = findNode(node.getStarNode(), uuid);
      }

      return targetNode;
    }
  }
}
