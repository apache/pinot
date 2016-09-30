function getCustomDashboard(tab) {
    var url = "/dashboard/data/customDashboard?"
        + window.location.hash.substring(1);
    getData(url, tab).done(function (data) {
        renderTabular(data, tab)
    });
};
