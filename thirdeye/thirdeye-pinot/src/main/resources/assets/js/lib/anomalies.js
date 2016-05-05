function getAnomalies() {

    //Todo: add the real endpoint
    var url = "/dashboard/data/datasets?" + window.location.hash.substring(1);
    getData(url).done(function(data) {
        renderAnomalies(data);
    });
};

function renderAnomalies(data) {
    $("#"+ hash.view +"-display-chart-section").append("<p>Anomalies content</p>")

}
