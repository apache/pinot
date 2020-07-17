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

import * as React from 'react';

import { createStyles, Theme, makeStyles } from '@material-ui/core/styles';
import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import CssBaseline from '@material-ui/core/CssBaseline';
import Box from '@material-ui/core/Box';
import Typography from '@material-ui/core/Typography';
import { NavLink } from 'react-router-dom';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faExternalLinkAlt } from '@fortawesome/free-solid-svg-icons';

const drawerWidth = 250;

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    root: {
      display: 'flex',
    },
    appBar: {
      zIndex: theme.zIndex.drawer + 1,
    },
    drawer: {
      width: drawerWidth,
      height: 'calc(100vh - 70px)',
      flexShrink: 0,
      backgroundColor: '#333333',
    },
    drawerPaper: {
      position: 'unset',
      width: drawerWidth,
      backgroundColor: '#F5F7F9',
    },
    drawerContainer: {
      overflow: 'auto',
      paddingTop: '20px'
    },
    content: {
      flexGrow: 1,
      padding: theme.spacing(3),
    },
    itemContainer: {
      color: '#3B454E',
      borderRadius: '4px'
    },
    selectedItem: {
      background: '#D8E1E8!important'
    },
    link: {
      textDecoration: 'none'
    }
  }),
);

type Props = {
  showMemu: boolean;
  list: Array<{id:number, name: string, link: string, target?: string}>;
  selectedId: number;
  highlightSidebarLink: (id: number) => void;
};

const Sidebar = ({ showMemu, list, selectedId, highlightSidebarLink }: Props) => {
  const classes = useStyles();

  return (
    <>
      <CssBaseline />
      <Drawer
        open={showMemu}
        className={classes.drawer}
        variant="permanent"
        classes={{
          paper: classes.drawerPaper,
        }}
      >
        <div className={classes.drawerContainer}>
          <List disablePadding>
            {list.map(({ id, name, link, target }) => (
              <Box width="210px" marginX="auto" marginBottom="5px" key={name}>
                {link !== 'help' ?
                  <NavLink to={link} className={classes.link} target={target}>
                    <ListItem color="white" button className={`${classes.itemContainer} ${selectedId === id ? classes.selectedItem : ''}`} selected={selectedId === id} onClick={(event) => highlightSidebarLink(id)}>
                      <Typography variant="subtitle2">{name} &ensp;</Typography>
                    </ListItem>
                  </NavLink>
                  :
                  <a href={`${window.location.origin}/${link}`} className={classes.link} target={target}>
                    <ListItem color="white" button className={`${classes.itemContainer}`}>
                      <Typography variant="subtitle2">{name} &ensp;
                        <FontAwesomeIcon icon={faExternalLinkAlt} />
                      </Typography>
                    </ListItem>
                  </a>}
              </Box>
            ))}
          </List>
        </div>
      </Drawer>
    </>
  );
};

export default Sidebar;
