function getCustomDashboard(tab) {
    var hashString = window.location.hash.substring(1);
	var url = "/dashboard/data/customDashboard?"
			+ window.location.hash.substring(1);
	getData(url, tab).done(function(data) {
        //cache the query data
        window.sessionStorage.setItem(hashString, JSON.stringify(data));
		renderTabular(data, tab)
	});
};
