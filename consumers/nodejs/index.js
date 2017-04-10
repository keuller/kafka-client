'use strict'

let kafka = require('kafka-node')


let client = new kafka.Client('localhost:2181', 'node-producer')
  , consumer = new kafka.Consumer(client, [ { topic:'sample' } ])

consumer.on('error', (err) => {
    client.close(() => { })
    process.exit()
})

consumer.on('message', (msg) => {
    // msg = { topic, value, offset, partition, key }
    console.log(`Node consumer: ${msg.value}`)
})

process.on('SIGINT', () => {
    consumer.close(true, () => { })
    process.exit()  
})

console.log('NodeJS Kafka consumer ready!')
