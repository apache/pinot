function HashService() {
  this.params = {};
  this.previousParams = {};
  this.dashboardController = null;
  this.anomalyResultController = null;
  this.analysisController = null;
}

HashService.prototype = {
  init : function() {
    console.log('HashService.init');
    window.onhashchange = this.onHashChangeEventHandler.bind(this);
    console.log("window.location:" + window.location + " path " + window.location.pathname);
    console.log(window.location);
    tab = constants.TAB_ANOMALIES;
    console.log(tab);
    console.log(constants);
    if (window.location.hash) {
      splits = window.location.hash.split('/');
      if (splits.length > 0) {
        console.log("hash split[0]" + splits[0]);
        tab = splits[0].replace("#", "");
      }
    }
    console.log("Setting tab to:" + tab)
    this.set(HASH_PARAMS.TAB, tab);
    urlFragment = window.location.hash.replace("#", "").replace("!", "");
    console.log(urlFragment);
    console.log('HashService.init ends');
  },
  setTabControllers : function(dashboardController, anomalyResultController, analysisController) {
    console.log('set tab controllers');
    this.dashboardController = dashboardController;
    this.anomalyResultController = anomalyResultController;
    this.analysisController = analysisController;
  },
  clear : function() {
    console.log('hash service.clear')
    this.params = {};
  },
  set : function(key, value) {
    this.params[key] = value;
  },
  setPrevious : function(key, value) {
    this.previousParams[key] = value;
  },
  get : function(key) {
    return this.params[key];
  },
  getPrevious : function(key) {
    return this.previousParams[key];
  },
  update : function(myparams) {
    console.log('hash service.update');
    for (var key in myparams) {
      this.params[key] = myparams[key];
    }
  },
  getParams : function() {
    return this.params;
  },
  refreshWindowHash : function(){
    window.location.href="#" + this.params[HASH_PARAMS.TAB] + this.getParamsUrlString();
  },
  // TODO: decide how to prevent unused params are listed in the URL
  // this function generate search URL, which starts with "?", from current hash params
  getParamsUrlString : function() {
    var paramsString = "?";
    var separator = "";
    for (var key in this.params) {
      if (key === HASH_PARAMS.TAB) {
        continue;
      }
      var value = JSON.stringify(this.params[key]);
      // if value was a String, the stringify method returns a string surround with quotes,
      // which are unwanted.
      if (value.startsWith("\"")) {
        value = value.slice(1, -1);
      }
      paramsString = paramsString + separator + key + "=" + value;
      separator = "&";
    }
    if (paramsString === "?") {
      return "";
    } else {
      return paramsString;
    }
  },
  // TODO: refine TE custom routing service to use this function
  // This function read params from search URL, which starts with "?"
  setHashParamsFromUrl : function() {
    console.log("Hash Params from URL:");

    console.log(window.location);
    var self = this;
    if (window.location.hash) {
      this.clear();
      var paramsUrl = window.location.hash.replace('#', '');
      var paramsUrlTokens = paramsUrl.split('?');
      if (paramsUrlTokens.length > 1) {
        self.set(HASH_PARAMS.TAB, paramsUrlTokens[0]);
        paramsUrl = paramsUrlTokens[1];
        paramsUrl.split("&").forEach(function(part) {
          var pair = part.split("=");
          self.set(pair[0], decodeURIComponent(pair[1]));
        });
        console.log(this.params);
      }
    }
  },
  route : function() {
    console.log('route');
    var params = this.getParams();
    console.log(params);
    var tab = params[HASH_PARAMS.TAB];
    this.clear();
    switch(tab) {
    case constants.TAB_DASHBOARD:
      console.log("dashboard params");
      // set tab
      this.setTab(constants.TAB_DASHBOARD);

      // set dashboard mode
      this.checkAndSetParam(params, HASH_PARAMS.DASHBOARD_MODE, constants.DASHBOARD_MODE_ANOMALY_SUMMARY);

      // set dashboard name
      this.checkAndSetParam(params, HASH_PARAMS.DASHBOARD_DASHBOARD_NAME, undefined);

      // set dashboard id
      this.checkAndSetParam(params, HASH_PARAMS.DASHBOARD_SUMMARY_DASHBOARD_ID, undefined);

      break;

    case constants.TAB_ANOMALIES:
      console.log("anomalies params");
      console.log(params);

      // set tab
      this.setTab(constants.TAB_ANOMALIES);

      // anomaliesSearchMode
      this.checkAndSetParam(params, HASH_PARAMS.ANOMALIES_ANOMALIES_SEARCH_MODE, constants.MODE_TIME);

      // startDate
      this.checkAndSetParamTime(params, HASH_PARAMS.ANOMALIES_START_DATE, moment().subtract(1, 'days').startOf('day').valueOf());

      // endDate
      this.checkAndSetParamTime(params, HASH_PARAMS.ANOMALIES_END_DATE, moment().subtract(0, 'days').startOf('day').valueOf());

      // pageNumber
      this.checkAndSetParam(params, HASH_PARAMS.ANOMALIES_PAGE_NUMBER, 1);

      // metricIds
      this.checkAndSetParam(params, HASH_PARAMS.ANOMALIES_METRIC_IDS, undefined);

      // dashboardId
      this.checkAndSetParam(params, HASH_PARAMS.ANOMALIES_DASHBOARD_ID, undefined);

      // anomalyIds
      this.checkAndSetParam(params, HASH_PARAMS.ANOMALIES_ANOMALY_IDS, undefined);

      break;

    case constants.TAB_ANALYSIS:
      this.setTab(constants.TAB_ANALYSIS);
      break;
    default:
      break;
    }
    this.set('rand', Math.random());
    console.log(this.params);
    this.refreshWindowHash();
  },
  onHashChangeEventHandler : function() {
    console.log('caught hash change');
    var tab = this.get(HASH_PARAMS.TAB);
    switch(tab) {
    case constants.TAB_DASHBOARD:
      this.dashboardController.handleAppEvent();
      break;
    case constants.TAB_ANOMALIES:
      this.anomalyResultController.handleAppEvent();
      break;
    case constants.TAB_ANALYSIS:
      this.analysisController.handleAppEvent();
      break;
    default:
      break;
    }
  },
  // HELPER FUNCTION: set param from new params
  checkAndSetParam : function(params, paramName, defaultValue) {
    if (params[paramName] != undefined) {// is param present in new param?
      this.set(paramName, params[paramName]);
    } else { // param not present
      if (this.getPrevious(paramName) != undefined) {   // param present in previous params?
        this.set(paramName, this.getPrevious(paramName));
      } else if (defaultValue != undefined){ // else set default, if given
        this.set(paramName, defaultValue);
      }
    }
    // previous is maintained, so that between tab switches (which clears params), previous values are retained
    this.setPrevious(paramName, this.get(paramName));
  },
  //HELPER FUNCTION: set param from new params
  checkAndSetParamTime : function(params, paramName, defaultValue) {
    if (params[paramName] != undefined) {// is param present in new param?
      this.set(paramName, params[paramName].valueOf());
    } else { // param not present
      if (this.getPrevious(paramName) != undefined) {   // param present in previous params?
        this.set(paramName, this.getPrevious(paramName));
      } else if (defaultValue != undefined){ // else set default, if given
        this.set(paramName, defaultValue);
      }
    }
    // previous is maintained, so that between tab switches (which clears params), previous values are retained
    this.setPrevious(paramName, this.get(paramName));
  },
  // HELPER FUNCTION:
  setTab : function(tabName) {
    this.set(HASH_PARAMS.TAB, tabName);
  }
};
