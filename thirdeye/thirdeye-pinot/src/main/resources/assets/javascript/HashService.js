function HashService() {
  this.params = {};
}

HashService.prototype = {

  clear: function() {
    this.params = {};
  },
  update: function(params) {
    for (var key in params) {
      this.params[key] = params[key];
    }
  },
  getParams: function() {
    return this.params;
  }

}