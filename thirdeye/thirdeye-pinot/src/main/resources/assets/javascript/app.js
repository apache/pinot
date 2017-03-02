// app entry point

var dataService = undefined;
var constants = undefined;
var HASH_SERVICE = undefined;
var HASH_PARAMS = undefined;
$(document).ready(function() {
    constants = new Constants();
    HASH_PARAMS = new HashParams();
    HASH_PARAMS.init();
    dataService = new DataService();
    HASH_SERVICE = new HashService();
    HASH_SERVICE.init();
    var app = new AppController();
    app.init();
    HASH_SERVICE.registerController('app', app);
    HASH_SERVICE.routeTo('app');
});
