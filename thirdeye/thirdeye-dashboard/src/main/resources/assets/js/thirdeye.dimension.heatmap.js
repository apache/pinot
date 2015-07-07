$(document).ready(function() {
    var data = $("#dimension-heat-map-data")
    var container = $("#dimension-heat-map-container")

    var volumeOptions = {
        filter: function(cell) {
            return true // just show all
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
            var value = cell.value
            if (value === '?') {
                value = 'OTHER'
            }
            return value + '<br/>' + (cell.stats['volume_difference'] * 100).toFixed(2) + '%'
        },
        backgroundColor: function(cell) {
            if (cell.stats['baseline_cdf_value'] == null) {
                return '#ffffff'
            }
            return 'rgba(136, 138, 252, ' + cell.stats['baseline_cdf_value'].toFixed(3) + ')'
        },
        groupBy: 'METRIC'
    }

    var contributionOptions = {
        filter: function(cell) {
            return true // just show all
        },
        comparator: function(a, b) {
            var cmp = b.stats['contribution_difference'] - a.stats['contribution_difference']
            if (cmp < 0) {
                return -1
            } else if (cmp > 0) {
                return 1
            } else {
                return 0
            }
        },
        display: function(cell) {
            var value = cell.value
            if (value === '?') {
                value = 'OTHER'
            }
            return value + '<br/>' + (cell.stats['contribution_difference'] * 100).toFixed(2) + '%'
        },
        backgroundColor: function(cell) {
            if (cell.stats['contribution_difference'] < 0) {
              return 'rgba(252, 136, 138, ' + (cell.stats['current_cdf_value']) + ')'
            } else {
              return 'rgba(138, 252, 136, ' + (cell.stats['current_cdf_value']) + ')'
            }
        },
        groupBy: 'METRIC'
    }

    var optionsMap = {
        'volume': volumeOptions,
        'contribution': contributionOptions
    }

    var toggleActive = function(elt) {
        $(".dimension-heat-map-button").removeClass("uk-active")
        elt.addClass("uk-active")
    }

    $(".dimension-heat-map-button").click(function() {
        var obj = $(this)
        var impl = obj.attr('impl')
        var options = optionsMap[impl]
        toggleActive(obj)
        renderHeatMap(data, container, options)
    })

    var hashParams = parseHashParameters(window.location.hash)
    var activeOptions = null
    var activeButton = null
    if (hashParams['heatMap'] == 'volume') {
        activeOptions = volumeOptions
        activeButton = $("#dimension-heat-map-button-volume")
    } else {
        activeOptions = contributionOptions
        activeButton = $("#dimension-heat-map-button-contribution")
    }

    toggleActive(activeButton)
    renderHeatMap(data, container, activeOptions)
})
