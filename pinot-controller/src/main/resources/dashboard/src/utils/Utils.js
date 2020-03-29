export default class Utils {

    static createTree(data, i, name) {
        const node = {};
        node.id = i;
        node.name = name;
        node.children =  [];
        for (const prop in data) {
            let value = data[prop];

            if(typeof value === 'object') {
                node.children.push(this.createTree(value,++i, prop));
            } else {
                const child = {id: ++i, name: prop + ':'  +  value};
                node.children.push(child);
            }
        }
        return node;
    }
}