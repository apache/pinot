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
import clsx from 'clsx';
import { createStyles, Theme, makeStyles } from '@material-ui/core/styles';
import Drawer from '@material-ui/core/Drawer';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import CssBaseline from '@material-ui/core/CssBaseline';
import Box from '@material-ui/core/Box';
import Typography from '@material-ui/core/Typography';
import { NavLink } from 'react-router-dom';

const drawerWidth = 260;

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
    drawerOpen: {
      width: drawerWidth,
      transition: theme.transitions.create('width', {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.enteringScreen,
      }),
    },
    drawerClose: {
      transition: theme.transitions.create('width', {
        easing: theme.transitions.easing.sharp,
        duration: theme.transitions.duration.leavingScreen,
      }),
      width: '90px',
    },
    drawerPaper: {
      position: 'unset',
      backgroundColor: '#F5F7F9',
      overflowY: 'visible'
    },
    drawerContainer: {
      padding: '20px 15px'
    },
    content: {
      flexGrow: 1,
      padding: theme.spacing(3),
    },
    itemContainer: {
      width: 'auto',
      display: 'block',
      color: '#3B454E',
      borderRadius: '4px',
      '&:hover': {
        backgroundColor: '#ecedef'
      }
    },
    selectedItem: {
      background: '#D8E1E8!important'
    },
    link: {
      textDecoration: 'none',
      '&:hover .menu-item': {
        display: 'inline-block'
      }
    },
    sidebarLabel: {
      marginLeft: '10px'
    },
    sidebarLabelClose: {
      display: 'none',
      marginLeft: '10px',
      top: 0,
      position: 'absolute',
      whiteSpace: 'nowrap',
      backgroundColor: 'inherit',
      padding: '8px 8px 8px 0',
      borderRadius: '0 4px 4px 0',
      zIndex: 9
    },
    popover: {
      pointerEvents: 'none',
    },
    paper: {
      padding: theme.spacing(1),
    },
  }),
);

type Props = {
  showMenu: boolean;
  list: Array<{id:number, name: string, link: string, target?: string, icon: any}>;
  selectedId: number;
  highlightSidebarLink: (id: number) => void;
};

const Sidebar = ({ showMenu, list, selectedId, highlightSidebarLink }: Props) => {
  const classes = useStyles();

  return (
    <>
      <CssBaseline />
      <Drawer
        className={clsx(classes.drawer, {
          [classes.drawerOpen]: showMenu,
          [classes.drawerClose]: !showMenu,
        })}
        classes={{
          paper: clsx(classes.drawerPaper, {
            [classes.drawerOpen]: showMenu,
            [classes.drawerClose]: !showMenu,
          }),
        }}
        transitionDuration={1000}
        variant="permanent"
      >
        <div className={classes.drawerContainer}>
          <List disablePadding>
            {list.map(({ id, name, link, target, icon }) => (
              <Box marginX="auto" marginBottom="5px" key={name}>
                {link !== 'help' ?
                  <NavLink to={link} className={classes.link} target={target}>
                    <ListItem color="white" button className={`${classes.itemContainer} ${selectedId === id ? classes.selectedItem : ''}`} selected={selectedId === id} onClick={(event) => highlightSidebarLink(id)}>
                      {icon}
                      <Typography
                        className={clsx('menu-item',{
                          [classes.sidebarLabel]: showMenu,
                          [classes.sidebarLabelClose]: !showMenu,
                        })}
                        component="span"
                      >{name} &ensp;</Typography>
                    </ListItem>
                  </NavLink>
                  :
                  <a href={`${window.location.origin}/${link}`} className={classes.link} target={target}>
                    <ListItem color="white" button className={`${classes.itemContainer}`}>
                      {icon}
                      <Typography
                        className={clsx('menu-item',{
                          [classes.sidebarLabel]: showMenu,
                          [classes.sidebarLabelClose]: !showMenu,
                        })}
                        component="span"
                      >{name} &ensp;</Typography>
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
