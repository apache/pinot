$(document).ready(function() {
    var data = $("#dimension-heat-map-data")
    var container = $("#dimension-heat-map-container")

    var volumeOptions = {
        filter: function(cell) {
            return Math.abs(cell.stats['volume_difference']) >= 0.005
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
            return cell.value + '<br/>' + (cell.stats['volume_difference'] * 100).toFixed(2) + '%'
        },
        backgroundColor: function(cell) {
            return 'rgba(136, 138, 252, ' + cell.stats['baseline_p_value'].toFixed(3) + ')'
        }
    }

    var contributionOptions = {
        filter: function(cell) {
            return Math.abs(cell.stats['contribution_difference']) >= 0.005
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
            return cell.value + '<br/>' + (cell.stats['contribution_difference'] * 100).toFixed(2) + '%'
        },
        backgroundColor: function(cell) {
            if (cell.stats['contribution_difference'] < 0) {
              return 'rgba(252, 136, 138, ' + (cell.stats['current_p_value']) + ')'
            } else {
              return 'rgba(138, 252, 136, ' + (cell.stats['current_p_value']) + ')'
            }
        }
    }

    $("#dimension-heat-map-button-contribution").click(function() {
      window.location.hash = setHashParameter(window.location.hash, 'heatMap', 'contribution')
      renderHeatMap(data, container, contributionOptions)
    })

    $("#dimension-heat-map-button-volume").click(function() {
      window.location.hash = setHashParameter(window.location.hash, 'heatMap', 'volume')
      renderHeatMap(data, container, volumeOptions)
    })

    var hashParams = parseHashParameters(window.location.hash)
    if (hashParams['heatMap'] == 'volume') {
      renderHeatMap(data, container, volumeOptions)
    } else if (hashParams['heatMap'] == 'contribution') {
      renderHeatMap(data, container, contributionOptions)
    } else { // not provided -> default
      window.location.hash = setHashParameter(window.location.hash, 'heatMap', 'volume')
      renderHeatMap(data, container, volumeOptions)
    }
})
