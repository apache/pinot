/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.compat;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.representer.Representer;


public class CompatibilityOpsRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompatibilityOpsRunner.class);

  private String _parentDir;
  private final String _configFileName;
  private int _generationNumber;

  private CompatibilityOpsRunner(String configFileName, int generationNumber) {
    _configFileName = configFileName;
    _generationNumber = generationNumber;
  }

  private boolean runOps()
      throws Exception {
    Path path = Paths.get(_configFileName);
    _parentDir = path.getParent().toString();
    InputStream inputStream = Files.newInputStream(path);

    Representer representer = new Representer(new DumperOptions());
    representer.getPropertyUtils().setSkipMissingProperties(true);
    Yaml yaml = new Yaml(new CustomConstructor(new LoaderOptions()), representer);

    CompatTestOperation operation = yaml.loadAs(inputStream, CompatTestOperation.class);
    LOGGER.info("Running compat verifications from file:{} ({})", path.toString(), operation.getDescription());

    boolean passed = true;
    for (BaseOp op : operation.getOperations()) {
      op.setParentDir(_parentDir);
      if (!op.run(_generationNumber)) {
        passed = false;
        System.out.println("Failure");
        break;
      }
    }
    return passed;
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length != 2) {
      throw new IllegalArgumentException("Need exactly one file name and one generation_number as arguments");
    }
    ClusterDescriptor clusterDescriptor = ClusterDescriptor.getInstance();
    clusterDescriptor.setControllerPort(System.getProperty("ControllerPort"));
    clusterDescriptor.setBrokerQueryPort(System.getProperty("BrokerQueryPort"));
    clusterDescriptor.setServerAdminPort(System.getProperty("ServerAdminPort"));

    CompatibilityOpsRunner runner = new CompatibilityOpsRunner(args[0], Integer.valueOf(args[1]));
    int exitStatus = 1;
    if (runner.runOps()) {
      exitStatus = 0;
    }
    System.exit(exitStatus);
  }

  public static class CustomConstructor extends Constructor {

    public CustomConstructor(LoaderOptions loadingConfig) {
      super(loadingConfig);
    }

    @Override
    protected Object constructObject(Node node) {
      if (node.getType() == BaseOp.class) {
        MappingNode mappingNode = (MappingNode) node;
        for (NodeTuple tuple : (mappingNode).getValue()) {
          if (((ScalarNode) tuple.getKeyNode()).getValue().equals("type")) {
            String type = ((ScalarNode) tuple.getValueNode()).getValue();
            switch (type) {
              case "segmentOp":
                node.setType(SegmentOp.class);
                break;
              case "tableOp":
                node.setType(TableOp.class);
                break;
              case "queryOp":
                node.setType(QueryOp.class);
                break;
              case "streamOp":
                node.setType(StreamOp.class);
                break;
              default:
                throw new RuntimeException("Unknown type: " + type);
            }
            mappingNode.getValue().remove(tuple);
            break;
          }
        }
      }
      return super.constructObject(node);
    }
  }
}
