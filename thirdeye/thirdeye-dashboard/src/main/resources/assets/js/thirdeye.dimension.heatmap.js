$(document).ready(function() {
    var data = $("#dimension-heat-map-data")
    var container = $("#dimension-heat-map-container")

    var options = {
        filter: function(cell) {
            return Math.abs(cell.stats['volume_difference']) > 0.005 // only show those w/ 0.5% or greater change
        },
        comparator: function(a, b) {
            var cmp = b.stats['current_value'] - a.stats['current_value'] // reverse
            if (cmp < 0) {
                return -1
            } else if (cmp > 0) {
                return 1
            } else {
                return 0
            }
        },
        display: function(cell) {
            var vd = (cell.stats['volume_difference'] * 100).toFixed(2) + '%'
            var cd = (cell.stats['contribution_difference'] * 100).toFixed(2) + '%'

            var cellContent = cell.value + '<br/>' + vd
            if (cell.stats['contribution_difference'] > 0) {
                cellContent += ' <span class="cell-down">(&#916;' + cd + ')</span>'
            } else {
                cellContent += ' <span class="cell-up">(&#916;' + cd + ')</span>'
            }

            return cellContent
        },
        backgroundColor: function(cell) {
            if (cell.stats['baseline_cdf_value'] == null) {
                return '#ffffff'
            }

            if (cell.stats['volume_difference'] >= 0) {
                return 'rgba(136, 138, 252, ' + cell.stats['baseline_cdf_value'].toFixed(3) + ')'
            } else {
                return 'rgba(252, 136, 138, ' + cell.stats['baseline_cdf_value'].toFixed(3) + ')'
            }
        },
        groupBy: 'METRIC'
    }

    renderHeatMap(data, container, options)
})
