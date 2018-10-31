// app entry point

var dataService = undefined;
var constants = undefined;
var HASH_SERVICE = undefined;
var HASH_PARAMS = undefined;

function createNavClickHandler() {
    $('#nav-help').click(function() {
        // toggle the nav-help menu and toggles the icon from down/up
        $('#help-menu').toggleClass('hidden');
        $('#chevron-icon').toggleClass('glyphicon-menu-down glyphicon-menu-up');
    });
}

$(document).ready(function() {

    // checking if user is authenticated
    // redirects to the login screen if not
    $.get('/auth/')
        .done(function() {
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
            createNavClickHandler();
        })
        .fail(function() {
            var fromUrl = encodeURIComponent(window.location.href);
            window.location.replace('/app/#/login?fromUrl=' + fromUrl);
        });
});
