import React from 'react';
import { CircularProgress, makeStyles } from "@material-ui/core";

export const useAppLoadingIndicatorV1Styles = makeStyles({
  appLoadingIndicator: {
    height: "100vh",
    width: "100%",
    display: "flex",
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
  },
});

export const AppLoadingIndicator = () => {
  const classes = useAppLoadingIndicatorV1Styles();
  return (
    <div
      className={classes.appLoadingIndicator}
    >
      <CircularProgress size={80} color="primary" />
    </div>
  )
}
