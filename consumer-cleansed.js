const Kafka = require('node-rdkafka');
const { configFromPath } = require('./util');
const fs = require('fs')

function createConfigMap(config) {
  return {
    'bootstrap.servers': config['bootstrap.servers'],
    'group.id': 'kafka-nodejs-getting-started'
  }
}

function createConsumer(config, onData) {
  const consumer = new Kafka.KafkaConsumer(
      createConfigMap(config),
      {'auto.offset.reset': 'earliest'});

  return new Promise((resolve, reject) => {
    consumer
     .on('ready', () => resolve(consumer))
     .on('data', onData);

    consumer.connect();
  });
}

async function consumeCleansed() {
  if (process.argv.length < 3) {
    console.log("Please provide the configuration file path as the command line argument");
    process.exit(1);
  }

  let configPath = process.argv.slice(2)[0];
  const config = await configFromPath(configPath);

  let responseTimes = {"Connect": [], "Processing": [], "Waiting" : [], "Total": []}

  let topic = "cleansed-benchmark";

  const consumer = await createConsumer(config, ({key, value}) => {
    console.log(`Consumed event from topic ${topic}: key = ${key}\n value = ${value}`);
    responseTimes[key].push(value.toString());
    console.log(responseTimes)
    // Write to disk
    // Obviously not optimal as it writes at every message
    fs.writeFile('./testoutput.txt', JSON.stringify(responseTimes), err => {
        if (err) {
            throw err;
        }
        console.log("JSON data is saved.")
    })
  });
  consumer.subscribe([topic]);
  consumer.consume();

  process.on('SIGINT', () => {
    console.log('\nDisconnecting consumer ...');
    consumer.disconnect();
  });

  

}

consumeCleansed()
  .catch((err) => {
    console.error(`Something went wrong:\n${err}`);
    process.exit(1);
  });
