function HashService() {
  this.params = {};
}

HashService.prototype = {
  init : function() {
    console.log("window.location:" + window.location + " path " + window.location.pathname);
    console.log(window.location);
    tab = "anomalies";
    if (window.location.hash) {
      splits = window.location.hash.split('/');
      if (splits.length > 0) {
        console.log("hash split[0]" + splits[0]);
        tab = splits[0].replace("#", "");
      }
    }
    console.log("Setting tab to:" + tab)
    this.set("tab", tab);
    urlFragment = window.location.hash.replace("#", "").replace("!", "");
    console.log(urlFragment);
    // page("/thirdeye" + urlFragment);
  },
  clear : function() {
    this.params = {};
  },
  set : function(key, value) {
    console.log("setting key:" + key + " value:"+ value);
    this.params[key] = value;
    this.refreshWindowHash();
  },
  get : function(key) {
    return this.params[key];
  },
  update : function(myparams) {
    for (var key in myparams) {
      this.params[key] = myparams[key];
    }
    this.refreshWindowHash();
  },
  getParams : function() {
    return this.params;
  },
  refreshWindowHash : function(){
    window.location.href="#" + this.params["tab"];
    // TODO: insert search URL after the hash URL
    // window.location.href="#" + this.params["tab"] + this.getParamsUrlString();
  },
  // TODO: decide how to prevent unused params are listed in the URL
  // this function generate search URL, which starts with "?", from current hash params
  getParamsUrlString : function() {
    var paramsString = "?";
    var separator = "";
    for (var key in this.params) {
      if (key === "tab") {
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
    var paramsUrl = location.search.substr(1);
    this.clear();
    paramsUrl.split("&").forEach(function(part) {
      var pair = part.split("=");
      this.set(pair[0], decodeURIComponent(pair[1]));
    });
    console.log("Hash Params from URL:");
    console.log(this.params);
  }
};
