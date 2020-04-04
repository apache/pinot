/*
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
import TreeItem from "@material-ui/lab/TreeItem";
import React from "react";

export default class Utils {

    static renderTree(nodes) {
        return <TreeItem key={nodes.id} nodeId={nodes.id} label={nodes.name}>
            {Array.isArray(nodes.children) ? nodes.children.map(node => this.renderTree(node)) : null}
        </TreeItem>
    };

    static getTableHeaderStyles() {
        return {
            backgroundColor: '#01579b',
            color: '#FFF'
        }
    }

    static populateNode(data, i, name) {
        const node = {};
        node.id = i;
        node.name = name;
        node.children =  [];
        for (const prop in data) {
            let value = data[prop];

            if(typeof value === 'object') {
                node.children.push(this.populateNode(value,++i, prop));
            } else {
                const child = {id: ++i, name: prop + ':'  +  value};
                node.children.push(child);
            }
        }
        return node;
    }


}