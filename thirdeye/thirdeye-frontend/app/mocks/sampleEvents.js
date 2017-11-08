/**
 * Mock events for rca-poc
 */
export default [
  {
    "urn": "thirdeye:event:holiday:123456789",
    "score": 0.05,
    "label": "Halloween",
    "type": "event",
    "link": "http://google.com",
    "relatedEntities": [
    ],
    "start": 1509580800000,
    "end": 1509667200000,
    "eventType": "holiday",
    "details": "",
    duration: 86400000,
    relStart: 46800000,
    relEnd: null,
    relDuration: null,
    isBaseline: false,
    isFuture: true,
    isOngoing: false,
    displayStart: 1509476399999,
    displayEnd: 1509479999999,
    displayLabel: 'Halloween',
    color: 'green',
    displayColor: 'green',
    displayScore: 0.175,
    humanRelStart: '13 hours after',
    humanDuration: 'ongoing',
    "attributes": {
      "country": ["Canada"]
    }
  }, {
    "urn": "thirdeye:event:holiday:234567890",
    "score": 0.05,
    "label": "Christmas",
    "type": "event",
    "link": "http://google.com",
    "relatedEntities": [
    ],
    "start": 1509580800000,
    "end": 1509667200000,
    "eventType": "holiday",
    "details": "",
    duration: 86400000,
    relStart: 46800000,
    relEnd: null,
    relDuration: null,
    isBaseline: false,
    isFuture: true,
    isOngoing: false,
    displayStart: 1509476399999,
    displayEnd: 1509479999999,
    displayLabel: 'Christmas',
    color: 'green',
    displayColor: 'green',
    displayScore: 0.175,
    humanRelStart: '13 hours after',
    humanDuration: 'ongoing',
    "attributes": {
      "country": ["Canada", "Mexico", "USA"],
      "region": ['North America']
    }
  }, {
    "urn": "thirdeye:event:deployment:123456789",
    "score": 0.999,
    "label": "my system",
    "type": "event",
    "link": "http://google.com",
    "relatedEntities": [
    ],
    "start": 1509566414000,
    "end": 1509652772000,
    "eventType": "deployment",
    "details": "",
    duration: 744000,
    relStart: -945000,
    relEnd: -201000,
    relDuration: 744000,
    isBaseline: false,
    isFuture: false,
    isOngoing: false,
    displayStart: 1509471855000,
    displayEnd: 1509472599000,
    displayLabel: 'deployment event',
    color: 'red',
    displayColor: 'red',
    displayScore: 0.025,
    humanRelStart: '16 mins before',
    humanDuration: '12 mins',
    "attributes": {
      "fabric": ['prod']
    }
  }
];
