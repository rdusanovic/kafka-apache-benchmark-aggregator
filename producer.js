const Kafka = require('node-rdkafka');
const {configFromPath} = require('./util');
const util = require('util');
const exec = util.promisify(require('child_process').exec);

function createConfigMap(config) {
  return {
    'bootstrap.servers': config['bootstrap.servers'],
    'dr_msg_cb': true
  }
}

function createProducer(config, onDeliveryReport) {

  const producer = new Kafka.Producer(createConfigMap(config));

  return new Promise((resolve, reject) => {
    producer
      .on('ready', () => resolve(producer))
      .on('delivery-report', onDeliveryReport)
      .on('event.error', (err) => {
        console.warn('event.error', err);
        reject(err);
      });
    producer.connect();
  });
}

async function produceBenchmark() {
  if (process.argv.length < 3) {
    console.log("Please provide the configuration file path as the command line argument");
    process.exit(1);
  }
  let configPath = process.argv.slice(2)[0];
  const config = await configFromPath(configPath);

  let topic = "benchmark-logs";

  let command = "ab -n 1 https://google.com/"

  const producer = await createProducer(config, (err, report) => {
    if (err) {
      console.warn('Error producing', err)
    } else {
      const {topic, key, value} = report;
      console.log(`Produced event to topic ${topic}: key = ${key}\n value = ${value}`);
    }
  });

  let numEvents = 1;
  for (let idx = 0; idx < numEvents; ++idx) {

    const key = command;
    const { stdout, stderr, error } = await exec(command);
    if (error) {
      console.log(error)
      continue
    }
    const valueBuffer = Buffer.from(stdout);

    producer.produce(topic, -1, valueBuffer, key);
  }

  producer.flush(10000, () => {
    producer.disconnect();
  });
}

produceBenchmark()
  .catch((err) => {
    console.error(`Something went wrong:\n${err}`);
    process.exit(1);
  });
