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

import React from 'react';
import { fade, makeStyles, withStyles, Theme, createStyles } from '@material-ui/core/styles';
import TreeView from '@material-ui/lab/TreeView';
import TreeItem, { TreeItemProps } from '@material-ui/lab/TreeItem';
import Collapse from '@material-ui/core/Collapse';
import { useSpring, animated } from 'react-spring/web.cjs'; // web.cjs is required for IE 11 support
import { TransitionProps } from '@material-ui/core/transitions';
import AddCircleOutlineOutlinedIcon from '@material-ui/icons/AddCircleOutlineOutlined';
import RemoveCircleOutlineOutlinedIcon from '@material-ui/icons/RemoveCircleOutlineOutlined';
import DescriptionOutlinedIcon from '@material-ui/icons/DescriptionOutlined';

function TransitionComponent(props: TransitionProps) {
  const style = useSpring({
    from: { opacity: 0, transform: 'translate3d(20px,0,0)' },
    to: { opacity: props.in ? 1 : 0, transform: `translate3d(${props.in ? 0 : 20}px,0,0)` },
  });

  return (
    <animated.div style={style}>
      <Collapse {...props} />
    </animated.div>
  );
}

const StyledTreeItem = withStyles((theme: Theme) =>
  createStyles({
    iconContainer: {
      '& .close': {
        opacity: 0.3,
      },
    },
    group: {
      marginLeft: 7,
      paddingLeft: 18,
      borderLeft: `1px dashed ${fade(theme.palette.text.primary, 0.4)}`,
    },
  }),
)((props: TreeItemProps) => <TreeItem {...props} TransitionComponent={TransitionComponent} />);

const useStyles = makeStyles(
  createStyles({
    root: {
      flexGrow: 1,
    },
  }),
);

type Props = {
  treeData: Array<any> | null;
  showChildEvent: (event: React.MouseEvent<HTMLLIElement, MouseEvent>) => void;
  expanded: any;
  selected: any;
  handleToggle: any;
  handleSelect: any;
};

type CustomTreeProps = {
  itemObj: any;
  showChildEvent: (event: React.MouseEvent<HTMLLIElement, MouseEvent>) => void;
};

const CustomTreeItem = ({ itemObj, showChildEvent }: CustomTreeProps) => {
  const nestedComments = (itemObj.child || []).map(item => {
    return <CustomTreeItem key={item.nodeId} itemObj={item} showChildEvent={showChildEvent} />;
  });

  return (
    <StyledTreeItem
      nodeId={itemObj.nodeId}
      label={itemObj.label}
      endIcon={itemObj.isLeafNode ? <DescriptionOutlinedIcon /> : <AddCircleOutlineOutlinedIcon />}
      onIconClick={(e)=> {
        showChildEvent(itemObj);
      }}
    >
      {nestedComments}
    </StyledTreeItem>
  );
};

export default function CustomizedTreeView({treeData, showChildEvent, expanded, selected, handleToggle, handleSelect}: Props) {
  const classes = useStyles();

  return (
    <TreeView
      className={classes.root}
      defaultExpanded={['1']}
      expanded={expanded}
      selected={selected}
      onNodeToggle={handleToggle}
      onNodeSelect={handleSelect}
      defaultCollapseIcon={<RemoveCircleOutlineOutlinedIcon />}
      defaultExpandIcon={<AddCircleOutlineOutlinedIcon />}
      defaultEndIcon={<DescriptionOutlinedIcon />}
    >
      {treeData.map((itemObj) => {
        return (
          <CustomTreeItem key={`parent_${itemObj.nodeId}`} itemObj={itemObj} showChildEvent={showChildEvent} />
        );
      })}
    </TreeView>
  );
}
