/** HEADER related eventlisteners **/

$("#main-view").on("click", ".header-tab", function (event) {
    if (event.currentTarget.id === 'new-ui-tab') {
        window.location.href = 'thirdeye';
    } else {
        switchHeaderTab(this)
    }
});

function switchHeaderTab(target) {
    radioButtons(target)
    hash.view = $(target).attr("rel");

    if (hash.view == "self-service") {

        //Clear hash, only keep dataset & view as those are required to display the view
        if (hash.dataset) {
            var dataset = hash.dataset;
            hash = {}
            hash.dataset = dataset;
            hash.view = "self-service";

            /* window.location.hash change triggers window.onhashchange event
             that contains the ajax requests */
            window.location.hash = encodeHashParameters(hash);

        }
        addSelfServiceListeners()
    }
}
