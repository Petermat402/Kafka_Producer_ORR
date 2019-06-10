const kafka = require('kafka-node');
const axios = require('axios');

try {
    const Producer = kafka.Producer;
    const client = new kafka.KafkaClient({kafkaHost: 'master:9092,slave01:9092,slave02:9092,slave03:9092,slave04:9092,slave05:9092'});
    const producer = new Producer(client);
    const kafka_topic = 'pollutionCities';
    console.log(kafka_topic);

    producer.on('ready', async function () {
        setInterval(() => {

            axios.all([
                axios.get('https://api.waqi.info/feed/warsaw/?token=1ae9f399f811ba4658b5f81937bec3ce5d60eda1'),
                axios.get('https://api.waqi.info/feed/paris/?token=1ae9f399f811ba4658b5f81937bec3ce5d60eda1'),
                axios.get('https://api.waqi.info/feed/berlin/?token=1ae9f399f811ba4658b5f81937bec3ce5d60eda1'),
                axios.get('https://api.waqi.info/feed/moscow/?token=1ae9f399f811ba4658b5f81937bec3ce5d60eda1'),
                axios.get('https://api.waqi.info/feed/london/?token=1ae9f399f811ba4658b5f81937bec3ce5d60eda1'),
                axios.get('https://api.waqi.info/feed/prague/?token=1ae9f399f811ba4658b5f81937bec3ce5d60eda1'),
                axios.get('https://api.waqi.info/feed/vienna/?token=1ae9f399f811ba4658b5f81937bec3ce5d60eda1'),
                axios.get('https://api.waqi.info/feed/oslo/?token=1ae9f399f811ba4658b5f81937bec3ce5d60eda1'),
                axios.get('https://api.waqi.info/feed/copenhagen/?token=1ae9f399f811ba4658b5f81937bec3ce5d60eda1'),
                axios.get('https://api.waqi.info/feed/rome/?token=1ae9f399f811ba4658b5f81937bec3ce5d60eda1'),
                axios.get('https://api.waqi.info/feed/madrid/?token=1ae9f399f811ba4658b5f81937bec3ce5d60eda1'),
                axios.get('https://api.waqi.info/feed/dublin/?token=1ae9f399f811ba4658b5f81937bec3ce5d60eda1'),
            ]).then(axios.spread((warsaw, paris, berlin, moscow, london, prague,
                                  vienna, oslo, copenhagen, rome, madrid, dublin) => {
                console.log('Warsaw PM25: ', warsaw.data.data.iaqi.no2.v);
                console.log('paris PM25: ', paris.data.data.iaqi.no2.v);
                console.log('berlin PM25: ', berlin.data.data.iaqi.no2.v);
                console.log('moscow PM25: ', moscow.data.data.iaqi.no2.v);
                console.log('london PM25: ', london.data.data.iaqi.no2.v);
                console.log('prague PM25: ', prague.data.data.iaqi.no2.v);
                console.log('vienna PM25: ', vienna.data.data.iaqi.no2.v);
                console.log('oslo PM25: ', oslo.data.data.iaqi.no2.v);
                console.log('copenhagen PM25: ', copenhagen.data.data.iaqi.no2.v);
                console.log('rome PM25: ', rome.data.data.iaqi.no2.v);
                console.log('madrid PM25: ', madrid.data.data.iaqi.no2.v);
                console.log('dublin PM25: ', dublin.data.data.iaqi.no2.v);
                let payloads = [
                    generatePayload('warsaw', warsaw, 0, kafka_topic),
                    generatePayload('paris', paris, 1, kafka_topic),
                    generatePayload('berlin', berlin, 2, kafka_topic),
                    generatePayload('moscow', moscow, 0, kafka_topic),
                    generatePayload('london', london, 1, kafka_topic),
                    generatePayload('prague', prague, 2, kafka_topic),
                    generatePayload('vienna', vienna, 0, kafka_topic),
                    generatePayload('oslo', oslo, 1, kafka_topic),
                    generatePayload('copenhagen', copenhagen, 2, kafka_topic),
                    generatePayload('rome', rome, 0, kafka_topic),
                    generatePayload('madrid', madrid, 1, kafka_topic),
                    generatePayload('dublin', dublin, 2, kafka_topic)
                ];

                let push_status = producer.send(payloads, (err, data) => {
                    if (err) {
                        console.log('[kafka-producer -> ' + kafka_topic + ']: broker update failed');
                        console.log(err)
                    } else {
                        console.log('[kafka-producer -> ' + kafka_topic + ']: broker update success');
                    }
                });

            }))
                .catch(err => {
                    console.error('Something went wrong while retrieving data from aqicn');
                    console.error(err);
                });
        }, 3600000);


    });

    producer.on('error', function (err) {
        console.log(err);
        console.log('[kafka-producer -> ' + kafka_topic + ']: connection errored');
        throw err;
    });
}
catch (e) {
    console.log(e);
}

function generatePayload(city, body, partition, topic) {
    return {
        topic: topic,
        messages: body.data.data.iaqi.no2.v,
        key: city,
        partition: partition,
        timestamp: Date.now()
    }
}