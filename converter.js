#!/usr/bin/env node

const csv = require('fast-csv')
const { through } = require('event-stream')
const moment = require('moment')
const assert = require('assert')

const travelTimeRE = /travel_time/

const vehicleTypes = {
  'NPMRDS (Trucks and passenger vehicles)': 'all_vehicles',
  'NPMRDS (Passenger vehicles)': 'passenger_vehicles',
  'NPMRDS (Trucks)': 'freight_trucks',
}

const csvInputStream = csv({
    headers: [
      'datasource',
      'tmc_code',
      'measurement_tstamp',
      'speed',
      'travel_time_minutes',
      'data_density'
    ],
  }).on("data", function(data){
    this.emit(data)
  })
  .on("end", function(){
  });


let curRow = {}
let curDate = 0
let curEpoch

const transformStream = through(
  function write(data) {
    const tmc = data.tmc_code.toUpperCase()

    if (curRow.tmc !== tmc) {
      Object.entries(curRow).forEach(([k,v]) => {
        if (k.match(travelTimeRE) && v) {
          curRow[k] = Math.round(v)
        }
      })
      if (curRow.tmc) {
        this.emit('data', curRow)
      }
      curRow = {}
    }

    const timestamp = moment(data.measurement_tstamp, 'YYYY-MM-DD HH:mm:ss')

    const date = +timestamp.format('YYYYMMDD')
    assert(curDate <= date)

    if (date !== curDate) {
      curDate = date
      curEpoch = 0
    }

    const mmtMidnight = moment(timestamp).startOf('day');
    const minutesIntoDay = timestamp.diff(mmtMidnight, 'minutes');

    assert(!(minutesIntoDay % 5))

    const epoch = Math.round(minutesIntoDay / 5)
    assert(curEpoch <= epoch)
    assert((epoch >= 0) && (epoch < 288))

    curEpoch = epoch

    Object.assign(curRow, { tmc, date, epoch })

    const vehicleType = vehicleTypes[data.datasource.trim()]

    assert(!!vehicleType)

    const ttA = data.travel_time_minutes && (data.travel_time_minutes * 60)
    const ttB = curRow[`travel_time_${vehicleType}`]

    const ttC = (ttA && ttB)
      ? (ttA + ttB)
      : (ttA || ttB)

    curRow[`travel_time_${vehicleType}`] = ttC
  },

  function end () {
    this.emit('data', curRow)
    this.emit('end')
  }
)

const csvOutputStream = csv
  .createWriteStream({
    transform: function (obj) {
      return obj
    },
    headers: [
      'tmc',
      'date',
      'epoch',
      'travel_time_all_vehicles',
      'travel_time_passenger_vehicles',
      'travel_time_freight_trucks',
    ],
  })

 
process.stdin
  .pipe(csvInputStream)
  .pipe(transformStream)
  .pipe(csvOutputStream)
  .pipe(process.stdout)
