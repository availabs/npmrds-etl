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
    // headers: [
      // 'datasource',
      // 'tmc_code',
      // 'measurement_tstamp',
      // 'speed',
      // 'average_speed',
      // 'reference_speed',
      // 'travel_time_seconds',
      // 'data_density'
    // ],
    headers: true,
  }).on("data", function(data){
    this.emit(data)
  })
  .on("end", function(){
  });


let curRow = {}
let curDate = 0
let curEpoch

const roundTimes = (row) => {
  Object.entries(row).forEach(([k,v]) => {
    if (k.match(travelTimeRE) && v) {
      row[k] = Math.round(v)
    }
  })
}

const transformStream = through(
  function write(data) {
    const tmc = data.tmc_code.toUpperCase()
    // const tmc = data.tmc_code

    let prevTMC = curRow.tmc
    let checkTMC = true

    if (curRow.tmc !== tmc) {
      if (curRow.tmc) {
        roundTimes(curRow)
        this.emit('data', curRow)
      }
      curRow = {}
    }

    const timestamp = moment(data.measurement_tstamp, 'YYYY-MM-DD HH:mm:ss')

    const date = +timestamp.format('YYYYMMDD')
    assert(curDate <= date, `curDate: ${curDate} | date: ${date}\n${JSON.stringify(data, null, 4)}`)

    if (date !== curDate) {
      curDate = date
      curEpoch = 0
      checkTMC = false
    }

    const mmtMidnight = moment(timestamp).startOf('day');
    const minutesIntoDay = timestamp.diff(mmtMidnight, 'minutes');

    assert(!(minutesIntoDay % 5))

    const epoch = Math.round(minutesIntoDay / 5)

    if (checkTMC && (epoch === curEpoch)) {
      assert(prevTMC <= tmc, 'TMCs not sorted');
    }

    assert(curEpoch <= epoch, 'Input data not sorted.')
    assert((epoch >= 0) && (epoch < 288), `EPOCH ${epoch} is out of the domain [0,287]`)

    curEpoch = epoch

    Object.assign(curRow, { tmc, date, epoch })

    const vehicleType = vehicleTypes[data.datasource.trim()]

    assert(!!vehicleType, `Unrecognized datasource: ${data.datasource}`)

    // For when we mutate the TMC codes
    const ttA = data.travel_time_seconds
    const ttB = curRow[`travel_time_${vehicleType}`]

    const ttC = (ttA && ttB)
      ? +(+ttA + +ttB)
      : +(+ttA || +ttB)

    curRow[`travel_time_${vehicleType}`] = ttC
    assert(Number.isFinite(ttC), `ttA: ${ttA}, ttB: ${ttB}, ttC: ${ttC}\n\n${JSON.stringify(curRow, null, 4)}\n---------------\n${JSON.stringify(data, null, 4)}`)
    // assert(!curRow[`travel_time_${vehicleType}`], `curRow[travel_time_${vehicleType}] was not empty`)

    // curRow[`travel_time_${vehicleType}`] = data.travel_time_seconds
  },

  function end () {
    roundTimes(curRow)
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
