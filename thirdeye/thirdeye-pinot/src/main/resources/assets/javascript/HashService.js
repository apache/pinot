function HashService() {
  this.params = {};

  this.controllerNameToControllerMap = {};
  this.currentControllerName = null;
  this.currentController = null;
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
  registerController : function(controllerName, controller) {
    console.log('registerController ' + controllerName);
    this.controllerNameToControllerMap[controllerName] = controller;
  },
  clear : function() {
    console.log('hash service.clear')
    this.params = {};
  },
  set(key, value) {
    console.log('set ' + key + ' ' + value);
    // casting the value to the correct type
    switch (key) {
      case HASH_PARAMS.ANOMALIES_START_DATE:
      case HASH_PARAMS.ANOMALIES_END_DATE:
      case HASH_PARAMS.ANALYSIS_CURRENT_START:
      case HASH_PARAMS.ANALYSIS_CURRENT_END:
      case HASH_PARAMS.ANALYSIS_BASELINE_START:
      case HASH_PARAMS.ANALYSIS_BASELINE_END:
        value = moment(value);
        break;
      case HASH_PARAMS.ANOMALIES_PAGE_NUMBER:
        value = Number(value);
        break;
    }
    this.params[key] = value;
  },
  get : function(key) {
    return this.params[key];
  },
  update : function(paramsToUpdate) {
    console.log('hash service.update');
    console.log(paramsToUpdate)
    for (var key in paramsToUpdate) {
      this.set(key, paramsToUpdate[key]);
    }
  },
  getParams : function() {
    console.log('getParams');
    return this.params;
  },
  refreshWindowHashForRouting : function(controllerName){
    console.log('RefreshWindowHashForRouting ' + controllerName);
    window.location.href="#" + this.params[HASH_PARAMS.TAB] + this.getParamsStringForController(controllerName);
  },
  getParamsStringForController : function(controllerName) {
    console.log('getParamsStringForController ' + controllerName)
    var paramNamesToDefaultValuesMap = HASH_PARAMS.controllerNameToParamNamesMap[controllerName];
    var paramsString = "?";
    var separator = "";
    for (var paramName in paramNamesToDefaultValuesMap) {
      if (paramName === HASH_PARAMS.TAB) {
        continue;
      }
      var defaultValue = paramNamesToDefaultValuesMap[paramName];
      var value = this.get(paramName);
      if (!value && defaultValue) { //if default value is present, use that
        value = defaultValue;
        this.set(paramName, defaultValue);
      }
      if (value) {
        value = JSON.stringify(value);
        if (value.startsWith("\"")) {
          value = value.slice(1, -1);
        }
        paramsString = paramsString + separator + paramName + "=" + value;
        separator = "&";
      }
    }
    paramsString = paramsString + separator + HASH_PARAMS.RAND + "=" + Math.random();
    console.log('Params string : ' + paramsString)
    return paramsString;
  },
  setHashParamsFromUrl : function() {
    console.log("Hash Params from URL:");
    console.log(window.location);

    if (window.location.hash) {
      this.clear();
      var paramsUrl = window.location.hash.replace('#', '');
      const [newTab, newParams] = paramsUrl.split('?');
      if (newParams) {
        this.set(HASH_PARAMS.TAB, newTab);

        newParams.split('&').forEach((part) => {
          const [key, value] = part.split('=');
          this.set(key, decodeURIComponent(value));
        });
        console.log(this.params);
      }
    }
  },
  routeTo : function(controllerName) {
    console.log('routeTo' + controllerName);
    this.currentControllerName = controllerName;
    this.currentController = this.controllerNameToControllerMap[controllerName];
    this.currentController.handleAppEvent();
  },

  /**
   * determine based on the hash change event if a transition is needed
   * @param  {string} options.newURL [newURL of hash change event]
   * @return {boolean} - returns true if a transition is needed]
   */
  shouldTransition({ newURL }) {
    const currentTab = HASH_SERVICE.get('tab');
    const params = this.getParams();
    const [, newHash] = newURL.split('#');
    // getting the tab and params from the hash
    const [newTab, newParams] = newHash.split('?');
    // getting rid of 'rand' or '&rand' params
    const [paramsToCheck] = newParams.split(/[&]*rand/);

    if ((currentTab !== newTab)) {
      return true;
    }
    if (!paramsToCheck) {
      return false;
    }

    for (let param of paramsToCheck.split('&')){
      const [key, value] = param.split('=');
      const currentValue = params[key];

      if (JSON.stringify(currentValue) !== value) {
        return true;
      }
    }

    return false;
  },
  onHashChangeEventHandler : function(event) {
    const transitionTo = this.shouldTransition(event);

    if (transitionTo) {
      this.setHashParamsFromUrl();
      this.routeTo('app');
    } else {
      console.log('OnHashChangeEventhandler');
      console.log(this.params);
      console.log(this.currentController);
    }
  }
};
