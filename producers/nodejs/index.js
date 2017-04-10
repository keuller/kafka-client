'use strict'

const kafka = require('kafka-node')
const TOPIC = 'sample'
const LIMIT = 3

let client = new kafka.Client('localhost:2181', 'node-producer')
  , producer = new  kafka.Producer(client)

producer.on('error', (err) => {
    client.close(() => { })
    process.exit()
})

producer.on('ready', () => {
    let count = 1, intPtr = null

    intPtr = setInterval(() => {
        let payload = [{ topic: TOPIC, messages: `Message ${count} from nodejs.` }]
          , isLimit = (count > LIMIT)

        count++

        if (isLimit) {
            clearInterval(intPtr)
            process.exit()
        }

        producer.send(payload, (err, data) => {
            if (err) throw err;
            console.log(`Message sent: ${payload[0].messages}`)
        })
    }, 1500)

})

process.on('SIGINT', () => {
    client.close(() => { })
    process.exit()
})

console.log('NodeJS Kafka producer is ready!')