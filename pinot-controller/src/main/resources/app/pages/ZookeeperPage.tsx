/* eslint-disable no-nested-ternary */
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

import React, { useEffect, useState } from 'react';
import { makeStyles, useTheme } from '@material-ui/core/styles';
import { Grid, Paper, Tabs, Tab } from '@material-ui/core';
import _ from 'lodash';
import AppLoader from '../components/AppLoader';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import TreeDirectory from '../components/Zookeeper/TreeDirectory';
import TabPanel from '../components/TabPanel';
import Utils from '../utils/Utils';
import CustomCodemirror from '../components/CustomCodemirror';

const useStyles = makeStyles((theme) => ({
  root:{
    flexGrow: 1,
  },
  rightPanel: {

  },
  tabLabel: {
    textTransform: 'capitalize',
    fontWeight: 600
  },
  codeMirrorDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
  },
  lastRefreshDiv: {
    direction: 'rtl',
    margin: '-15px 0'
  }
}));

const ZookeeperPage = () => {
  const classes = useStyles();
  const theme = useTheme();
  const [fetching, setFetching] = useState(false);
  const [treeData, setTreeData] = useState([]);
  const [currentNodeData, setCurrentNodeData] =  useState({});
  const [currentNodeMetadata, setCurrentNodeMetadata] =  useState({});
  const [selectedNode, setSelectedNode] =  useState(null);
  const [count, setCount] = useState(1);
  const [leafNode, setLeafNode] = useState(false);

  // states and handlers for toggle and select of tree
  const [expanded, setExpanded] = React.useState<string[]>(['1']);
  const [selected, setSelected] = React.useState<string[]>(['1']);
  const [lastRefresh, setLastRefresh] = React.useState(null);

  const handleToggle = (event: React.ChangeEvent<{}>, nodeIds: string[]) => {
    setExpanded(nodeIds);
  };

  const handleSelect = (event: React.ChangeEvent<{}>, nodeIds: string[]) => {
    if(selected !== nodeIds){
      setSelected(nodeIds);
      const treeObj = Utils.findNestedObj(treeData, 'nodeId', nodeIds);
      if(treeObj){
        setLeafNode(treeObj.isLeafNode);
        setSelectedNode(treeObj.fullPath || '/');
        showInfoEvent(treeObj.fullPath || '/');
      }
    }
  };

  // on select, show node data and node metadata
  const showInfoEvent = async (fullPath) => {
    const nodeDataObj = await PinotMethodUtils.getNodeData(fullPath);
    setCurrentNodeData(nodeDataObj.currentNodeData);
    setCurrentNodeMetadata(nodeDataObj.currentNodeMetadata);
    setLastRefresh(new Date());
  };

  // handlers for Tabs
  const [value, setValue] = React.useState(0);
  const handleChange = (event: React.ChangeEvent<{}>, newValue: number) => {
    setValue(newValue);
  };
  
  // fetch and show children tree if not already fetched
  const showChildEvent = (pathObj) => {
    if(!pathObj.hasChildRendered){
      fetchInnerPath(pathObj);
    }
  };

  const fetchInnerPath = async (pathObj) => {
    const ZKDataObj = await PinotMethodUtils.getZookeeperData(pathObj.fullPath, count);
    pathObj.child = ZKDataObj.newTreeData[0].child;
    pathObj.isLeafNode = ZKDataObj.newTreeData[0].child.length === 0;
    pathObj.hasChildRendered = true;
    // setting the old treeData again here since pathObj has the reference of old treeData
    // and newTreeData is not useful here.
    setTreeData(treeData);
    setCurrentNodeData(ZKDataObj.currentNodeData);
    setCurrentNodeMetadata(ZKDataObj.currentNodeMetadata);
    setCount(ZKDataObj.counter);
    setExpanded([...expanded, pathObj.nodeId]);
  };

  const fetchData = async () => {
    setFetching(true);
    const path = '/';
    const ZKDataObj = await PinotMethodUtils.getZookeeperData(path, 1);
    setTreeData(ZKDataObj.newTreeData);
    setSelectedNode(path);
    setCurrentNodeData(ZKDataObj.currentNodeData || {});
    setCurrentNodeMetadata(ZKDataObj.currentNodeMetadata);
    setCount(ZKDataObj.counter);
    setExpanded(['1']);
    setSelected(['1']);
    setLastRefresh(new Date());
    setFetching(false);
  };

  useEffect(() => {
    fetchData();
  }, []);

  const renderLastRefresh = () => (
    <div className={classes.lastRefreshDiv}>
      <p>
        {`Last Refreshed: ${lastRefresh.toLocaleTimeString('en-US', {
          hour12: true,
          hour: 'numeric',
          minute: '2-digit',
          second: '2-digit'
        })}
        `}
      </p>
    </div>
  );

  return fetching ? (
    <AppLoader />
  ) : (
    <>
      <Grid item>
        <TreeDirectory
          treeData={treeData}
          selectedNode={selectedNode}
          showChildEvent={showChildEvent}
          expanded={expanded}
          selected={selected}
          handleToggle={handleToggle}
          handleSelect={handleSelect}
          isLeafNodeSelected={leafNode}
          currentNodeData={currentNodeData}
          currentNodeMetadata={currentNodeMetadata}
          showInfoEvent={showInfoEvent}
          fetchInnerPath={fetchInnerPath}
        />
      </Grid>
      <Grid item xs style={{ padding: 20, backgroundColor: 'white', maxHeight: 'calc(100vh - 70px)', overflowY: 'auto' }}>
        <Grid container>
          <Grid item xs={12} className={classes.rightPanel}>
            <Paper className={classes.root}>
              <Tabs
                value={value}
                onChange={handleChange}
                indicatorColor="primary"
                textColor="primary"
                centered
              >
                <Tab label="Node Data" className={classes.tabLabel} />
                <Tab label="Node Metadata" className={classes.tabLabel} />
              </Tabs>
            </Paper>
            <TabPanel
              value={value}
              index={0}
              dir={theme.direction}
            >
              {lastRefresh && renderLastRefresh()}
              <div className={classes.codeMirrorDiv}>
                <CustomCodemirror data={currentNodeData} />
              </div>
            </TabPanel>
            <TabPanel value={value} index={1} dir={theme.direction}>
              {lastRefresh && renderLastRefresh()}
              <div className={classes.codeMirrorDiv}>
                <CustomCodemirror data={currentNodeMetadata} />
              </div>
            </TabPanel>
          </Grid>
        </Grid>
      </Grid>
    </>
  );
};

export default ZookeeperPage;
