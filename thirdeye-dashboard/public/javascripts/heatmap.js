$(function() {
    // Get heat map data
    var path = $(location).attr('pathname');
    $.get("/data" + path, function(data) {
        addLogValue(data, "baseline");
        normalize(data, "logBaseline", getStats(data, "logBaseline"));
        var heatmap = generateHeatMap(path, data, 5);
        $("#heatmap").append(heatmap);
    })
})