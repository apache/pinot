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
package org.apache.pinot.graph.spi;

import java.util.Collections;
import java.util.Map;


/**
 * Graph schema configuration that defines vertex labels and edge labels,
 * mapping them to underlying Pinot tables and columns.
 *
 * <p>This POJO is the bridge between the graph model (vertices, edges, labels)
 * and the relational model (tables, columns, joins) used by Pinot.</p>
 */
public class GraphSchemaConfig {
  private final Map<String, VertexLabel> _vertexLabels;
  private final Map<String, EdgeLabel> _edgeLabels;

  public GraphSchemaConfig(Map<String, VertexLabel> vertexLabels, Map<String, EdgeLabel> edgeLabels) {
    _vertexLabels = Collections.unmodifiableMap(vertexLabels);
    _edgeLabels = Collections.unmodifiableMap(edgeLabels);
  }

  public Map<String, VertexLabel> getVertexLabels() {
    return _vertexLabels;
  }

  public Map<String, EdgeLabel> getEdgeLabels() {
    return _edgeLabels;
  }

  /**
   * Defines a vertex label backed by a Pinot table.
   */
  public static class VertexLabel {
    private final String _tableName;
    private final String _primaryKey;
    private final Map<String, String> _properties;

    /**
     * @param tableName  the Pinot table backing this vertex label
     * @param primaryKey the column used as the vertex identifier
     * @param properties map from property name to column name
     */
    public VertexLabel(String tableName, String primaryKey, Map<String, String> properties) {
      _tableName = tableName;
      _primaryKey = primaryKey;
      _properties = Collections.unmodifiableMap(properties);
    }

    public String getTableName() {
      return _tableName;
    }

    public String getPrimaryKey() {
      return _primaryKey;
    }

    public Map<String, String> getProperties() {
      return _properties;
    }
  }

  /**
   * Defines an edge label backed by a Pinot table, connecting two vertex labels.
   */
  public static class EdgeLabel {
    private final String _tableName;
    private final String _sourceVertexLabel;
    private final String _sourceKey;
    private final String _targetVertexLabel;
    private final String _targetKey;
    private final Map<String, String> _properties;

    /**
     * @param tableName         the Pinot table backing this edge label
     * @param sourceVertexLabel the label of the source vertex
     * @param sourceKey         the column in the edge table referencing the source vertex
     * @param targetVertexLabel the label of the target vertex
     * @param targetKey         the column in the edge table referencing the target vertex
     * @param properties        map from property name to column name
     */
    public EdgeLabel(String tableName, String sourceVertexLabel, String sourceKey,
        String targetVertexLabel, String targetKey, Map<String, String> properties) {
      _tableName = tableName;
      _sourceVertexLabel = sourceVertexLabel;
      _sourceKey = sourceKey;
      _targetVertexLabel = targetVertexLabel;
      _targetKey = targetKey;
      _properties = Collections.unmodifiableMap(properties);
    }

    public String getTableName() {
      return _tableName;
    }

    public String getSourceVertexLabel() {
      return _sourceVertexLabel;
    }

    public String getSourceKey() {
      return _sourceKey;
    }

    public String getTargetVertexLabel() {
      return _targetVertexLabel;
    }

    public String getTargetKey() {
      return _targetKey;
    }

    public Map<String, String> getProperties() {
      return _properties;
    }
  }
}
