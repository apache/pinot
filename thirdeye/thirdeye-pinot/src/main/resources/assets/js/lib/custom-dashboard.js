function getCustomDashboard() {

	var url = "/dashboard/data/customDashboard?"
			+ window.location.hash.substring(1);
	getData(url).done(function(data) {
		renderTabular(data)
	});
};
