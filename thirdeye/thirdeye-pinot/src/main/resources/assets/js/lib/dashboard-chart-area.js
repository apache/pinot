//In this js file we keep event listeners on the chart area elements present in  multiple views

//Cumulative checkbox
$("#main-view").on("click", ".cumulative", function () {
    toggleCumulative()
});

//On the FIRST click on cumulative btn trigger the cumulative column total calculation on contributors table
<!-- Hiding total feature till ratio metrics are handled -->
/*$("#main-view").one("click", ".cumulative", function() {
 calcCummulativeTotal(this)
 });*/

// Summary and details tab toggle
$("#main-view").on("click", "#sum-detail button", function () {
    toggleSumDetails(this)
});

/** Dashboard and tabular related listeners * */

// Clicking heat-map-cell should choose the related metrics and the derived
// metric the current time to the related hour then loading heatmap view
$("#main-view").on("click", "#funnels-table .heat-map-cell", function () {
    showHeatMap(this)
})

