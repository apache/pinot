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

declare module 'Models';

// Type definitions for dagre and graphlib CommonJS modules
declare module 'dagre' {
  import { Graph } from 'graphlib';
  
  export namespace graphlib {
    class Graph {
      constructor(options?: any);
      setGraph(label: any): void;
      graph(): any;
      setNode(name: string, label?: any): void;
      node(name: string): any;
      setEdge(source: string, target: string, label?: any): void;
      edge(source: string, target: string): any;
      setDefaultEdgeLabel(callback: () => any): void;
      nodes(): string[];
      edges(): Array<{ v: string; w: string }>;
    }
  }
  
  export function layout(graph: any): void;
}

declare module 'graphlib' {
  export class Graph {
    constructor(options?: any);
    setGraph(label: any): void;
    graph(): any;
    setNode(name: string, label?: any): void;
    node(name: string): any;
    setEdge(source: string, target: string, label?: any): void;
    edge(source: string, target: string): any;
    setDefaultEdgeLabel(callback: () => any): void;
    nodes(): string[];
    edges(): Array<{ v: string; w: string }>;
  }
}
