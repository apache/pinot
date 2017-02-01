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
  set : function(key, value) {
    console.log('set ' + key + ' ' + value)
    this.params[key] = value;
  },
  get : function(key) {
    return this.params[key];
  },
  update : function(paramsToUpdate) {
    console.log('hash service.update');
    console.log(paramsToUpdate)
    for (var key in paramsToUpdate) {
      this.params[key] = paramsToUpdate[key];
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
      if (value == undefined && defaultValue != undefined) { //if default value is present, use that
        value = defaultValue;
        this.set(paramName, defaultValue);
      }
      if (value != undefined) {
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
  routeTo : function(controllerName) {
    console.log('routeTo' + controllerName);
    this.currentControllerName = controllerName;
    this.currentController = this.controllerNameToControllerMap[controllerName];
    this.refreshWindowHashForRouting(controllerName);
  },

  /**
   * determine based on the hash change event if a transition is needed
   * @param  {string} options.oldURL [oldURL of hash change event]
   * @param  {string} options.newURL [newURL of hash change event]
   * @return {boolean} - returns true if a transition is needed]
   */
  shouldTransition({ oldURL, newURL }) {
    const [_1, transitionFrom, paramsFrom]= oldURL.split(/(?:[#?]|[&]*rand)/);
    const [_2, transitionTo, paramsTo] = newURL.split(/(?:[#?]|[&]*rand)/);
    const currentTab = HASH_SERVICE.get('tab');

    if ((currentTab != transitionTo)) {
      return true;
    } else if (paramsTo) {
      for (let param of paramsTo.split('&')){
        const [key, value] = param.split('=');
        if (this.params[key] != value) {
          return true;
        }
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
      this.currentController.handleAppEvent();
    }
  }
};
