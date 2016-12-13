function HashService() {
  this.params = {};
}

HashService.prototype = {
  init : function() {
    console.log("window.location:" + window.location + " path " + window.location.pathname);
    console.log(window.location);
    tab = "dashboard";
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
    for ( var key in myparams) {
      this.params[key] = myparams[key];
    }
    this.refreshWindowHash();
  },
  getParams : function() {
    return this.params;
  },
  refreshWindowHash : function(){
    window.location.href="#" + this.params["tab"];
  }
}
