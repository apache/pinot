$(document).ready(function() {
    $("#landing-submit").click(function(event) {
        event.preventDefault()
        var collection = $("#landing-collection").val()
        window.location.pathname = "/dashboard/" + collection
    })
})
