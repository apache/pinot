import { MuiThemeProvider } from "@material-ui/core";
import { AuthProvider } from "./components/auth/AuthProviderWrapper";
import CustomNotification from "./components/CustomNotification";
import { NotificationContextProvider } from "./components/Notification/NotificationContextProvider";
import theme from "./theme";
import React from "react";
import ReactDOM from "react-dom";
import { App } from "./App";
import { HashRouter } from "react-router-dom";

ReactDOM.render(
    <HashRouter>
        <MuiThemeProvider theme={theme}>
        <NotificationContextProvider>
            <CustomNotification />
            <AuthProvider>
                <App />
            </AuthProvider>
        </NotificationContextProvider>
        </MuiThemeProvider>
    </HashRouter>, 
    document.getElementById('app')
);
