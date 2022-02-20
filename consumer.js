const Kafka = require('node-rdkafka');
const { configFromPath } = require('./util');
const readline = require('readline');

function createConfigMapConsumer(config) {
  return {
    'bootstrap.servers': config['bootstrap.servers'],
    'group.id': 'kafka-nodejs-getting-started'
  }
}

function createConfigMapProducer(config) {
  return {
    'bootstrap.servers': config['bootstrap.servers'],
    'dr_msg_cb': true
    // 'group.id': 'kafka-nodejs-getting-started'
  }
}

function createConsumer(config, onData) {
  const consumer = new Kafka.KafkaConsumer(
      createConfigMapConsumer(config),
      {'auto.offset.reset': 'earliest'});

  return new Promise((resolve, reject) => {
    consumer
     .on('ready', () => resolve(consumer))
     .on('data', onData);

    consumer.connect();
  });
}

function createProducer(config, onDeliveryReport) {
  const producer = new Kafka.Producer(createConfigMapProducer(config));
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


async function consumerBenchmark() {
  if (process.argv.length < 3) {
    console.log("Please provide the configuration file path as the command line argument");
    process.exit(1);
  }

  let configPath = process.argv.slice(2)[0];
  const config = await configFromPath(configPath);

  let topic = "benchmark-logs";
  const consumer = await createConsumer(config, ({key, value}) => {
    console.log(`Consumed event from topic ${topic}: key = ${key}\n value = ${value}`);
    let data = cleanseData(value.toString())
    console.log(data)
    try {
      pipeCleansedData(data,config)
    } catch (error) {
      console.error(error)
    }
  });
  consumer.subscribe([topic]);
  consumer.consume();

  process.on('SIGINT', () => {
    console.log('\nDisconnecting consumer ...');
    consumer.disconnect();
  });
}



function cleanseData(data) {
  const lines = data.split("\n")
  const res = lines
    .filter( line => {
      return line.startsWith("Connect:") ||
        line.startsWith("Processing:") ||
        line.startsWith("Waiting:") ||
        line.startsWith("Total:")
    })
    .map( line => line.split(/[ \t]+/))
    .map( s => [s[0].slice(0,-1).trim(),s[1].trim()])
    .reduce ( (responseDict, [k,v] ) => {
      responseDict[k] = v;
      return responseDict;
    }, {});
  return res
}

async function pipeCleansedData(data, config) {
  let topic = "cleansed-benchmark"
  const producer = await createProducer(config, (err, report) => {
    if (err) {
      console.warn('Error producing', err)
    } else {
      const {topic, key, value} = report;
      console.log(`Produced event to topic ${topic}: key = ${key}\n value = ${value}`);
    }
  });

  for (const key in data) {
    console.log(key, data[key])
    producer.produce(topic, -1, Buffer.from(data[key]), key);
  }

  producer.flush(10000, () => {
    producer.disconnect();
  });
}

consumerBenchmark()
  .catch((err) => {
    console.error(`Something went wrong:\n${err}`);
    process.exit(1);
  });
