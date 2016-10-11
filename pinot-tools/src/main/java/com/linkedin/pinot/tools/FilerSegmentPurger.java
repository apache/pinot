/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.IdealState;
import org.kohsuke.args4j.Option;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class FilerSegmentPurger extends AbstractBaseCommand implements Command {

  @Option(name = "-zkAddress", required = true, metaVar = "", usage = "Zookeeper address. comma separated host:port pairs")
  private String _zkAddress;

  @Option(name = "-clusterName", required = true, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName;

  @Option(name = "-segmentMountDir", required = true, metaVar = "<String>", usage = "Pinot segment directory mount path on NFS. Segments for a table must be found at segmentMountDir/<clusterName>/<tableName>")
  private String _segmentMountDir;

  @Option(name = "-help", required = false, help = true, aliases={"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help = false;
  
  @Override
  public boolean execute() throws Exception {
    ZKHelixAdmin zkHelixAdmin = new ZKHelixAdmin(_zkAddress);
    Set<String> tableNamesFromZK = Sets.newHashSet();
    List<String> tableNames = zkHelixAdmin.getResourcesInCluster(_clusterName);
    for (String tableName : tableNames) {
      tableNamesFromZK.add(tableName);
    }

    Map<String, Set<File>> segmentsToPurgeMap = Maps.newHashMap();

    for (String tableName : tableNamesFromZK) {
      Set<String> segmentsFromIS = Collections.emptySet();
      // get segment list from Helix if the table exists
      if (tableNamesFromZK.contains(tableName)) {
        IdealState idealState = zkHelixAdmin.getResourceIdealState(_clusterName, tableName);
        segmentsFromIS = idealState.getPartitionSet();
      }
      // get segment list from Filer if the table exists
      String rawTableName = tableName.replace("_OFFLINE", "").replace("_REALTIME", "");
      File tableRootDir = new File(new File(_segmentMountDir, _clusterName), rawTableName);
      Set<File> canBePurged = Sets.newHashSet();
      if (tableRootDir.exists()) {
        File[] segmentFiles = tableRootDir.listFiles();
        for(File segmentFile:segmentFiles){
          //we might have files with or without extension  
          String segmentName = segmentFile.getName().replace(".tar.gz", "");
          if(!segmentsFromIS.contains(segmentName)){
            canBePurged.add(segmentFile);
          }
        }
      }
      if (!canBePurged.isEmpty()) {
        segmentsToPurgeMap.put(tableName, canBePurged);
      }
    }

    System.out.println("SEGMENTS THAT CAN BE PURGED FROM FILER");
    long totalPurgeableSize = 0;
    for (String tableName : segmentsToPurgeMap.keySet()) {
      System.out.println(tableName);
      for (File segmentFile : segmentsToPurgeMap.get(tableName)) {
        System.out.println("\t" + segmentFile.length() + "\t" + segmentFile.getAbsolutePath());
        totalPurgeableSize += segmentFile.length();
      }
    }
    System.out.println("PURGEABLE SIZE " + totalPurgeableSize + " bytes");
    return true;
  }

  @Override
  public String description() {
    return "Tool that lists (does not delete) the files that can be purged from NFS." + "This should be run on the controller node.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }
}
