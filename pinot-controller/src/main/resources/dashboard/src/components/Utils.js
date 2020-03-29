import TreeItem from "@material-ui/lab/TreeItem";
import React from "react";

export default class Utils {

    static renderTree(nodes) {
        return <TreeItem key={nodes.id} nodeId={nodes.id} label={nodes.name}>
            {Array.isArray(nodes.children) ? nodes.children.map(node => this.renderTree(node)) : null}
        </TreeItem>
    };

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