$(document).ready(function() {

    var data = $("#dimension-heat-map-data")
    var container = $("#dimension-heat-map-container")
    var filterToggle = $("#dimension-heat-map-filter")

    var hash = parseHashParameters(window.location.hash)
    if (hash['filterState']) {
        filterToggle.attr('state', hash['filterState'])
    }

    var currentMode = hash['heatMapMode']
    if (!currentMode) {
      currentMode = 'self'
    }

    var options = {
        filter: function(cell) {
            if (filterToggle.attr('state') === 'on') {
                // Only show cells which make up 1% or more of the dimension
                return cell.stats['baseline_ratio'] > 0.01 || cell.stats['current_ratio'] > 0.01
            } else {
                return true;
            }
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

            var cellContent = value + '<br/>'

            if (currentMode === 'self') {
              var currentValue = cell.stats['current_value']
              var baselineValue = cell.stats['baseline_value']
              var selfRatio = (currentValue - baselineValue) / baselineValue
              cellContent += (selfRatio * 100).toFixed(2) + '%'
            } else if (currentMode === 'others') {
              var baselineRatio = (cell.stats['baseline_ratio'] * 100).toFixed(2) + '%'
              var contributionDifference = (cell.stats['contribution_difference'] * 100).toFixed(2) + '%'
              cellContent += baselineRatio + ' (' + contributionDifference + ')'
            } else if (currentMode === 'all') {
              var volumeDifference = (cell.stats['volume_difference'] * 100).toFixed(2) + '%'
              cellContent += volumeDifference
            }

            return cellContent
        },
        backgroundColor: function(cell) {
            if (cell.stats['baseline_cdf_value'] == null) {
                return '#ffffff'
            }

            var testStatistic = null
            if (currentMode === 'self') {
              var currentValue = cell.stats['current_value']
              var baselineValue = cell.stats['baseline_value']
              testStatistic = (currentValue - baselineValue) / baselineValue
            } else if (currentMode === 'others') {
              testStatistic = cell.stats['contribution_difference']
            } else if (currentMode === 'all') {
              testStatistic = cell.stats['volume_difference']
            }

            if (testStatistic >= 0) {
                return 'rgba(136, 138, 252, ' + cell.stats['baseline_cdf_value'].toFixed(3) + ')'
            } else {
                return 'rgba(252, 136, 138, ' + cell.stats['baseline_cdf_value'].toFixed(3) + ')'
            }
        },
        groupBy: 'METRIC'
    }

    filterToggle.click(function() {
        var state = filterToggle.attr('state')
        var nextState = state === 'on' ? 'off' : 'on'
        filterToggle.attr('state', nextState)

        // Set in URI
        var hash = parseHashParameters(window.location.hash)
        hash['filterState'] = nextState
        window.location.hash = encodeHashParameters(hash)

        renderHeatMap(data, container, options)
    })

    $(".dimension-heat-map-mode").click(function() {
      currentMode = $(this).attr('mode')
      console.log('Switching to heat map mode ' + currentMode)

      // Set in URI
      var hash = parseHashParameters(window.location.hash)
      hash['heatMapMode'] = currentMode
      window.location.hash = encodeHashParameters(hash)

      // Display correct explanation
      $("#dimension-heat-map-explanation div").hide()
      $("#dimension-heat-map-explanation-" + currentMode).show()

      // Re-render heat map
      renderHeatMap(data, container, options)
    })

    $("#dimension-heat-map-mode-" + currentMode).trigger('click')
})
