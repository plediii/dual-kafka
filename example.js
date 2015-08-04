"use strict";

var dualapi = require('dualapi')
.use(require('./index'));

var d = dualapi();

d.mount(['**'], function (body, ctxt) {
    console.log(ctxt.from.join('/'), ' -> ', ctxt.to.join('/'));
});

var k = d.kafkaClient();
console.log(k.client)
var kafkaNode = new require('kafka-node');
var producer = new kafkaNode.Producer(k.client);
var exampleTopic = 'dual-kafka-example';
console.log('Waiting for producer ready.');
producer.on('ready', function () {
    producer.createTopics([exampleTopic], true, function (err) {
        if (err) {
            console.error('Error creating example topic: ', err, err.stack);
            client.close();
            return;
        }
        console.log('Created topic ', exampleTopic);
        k.highLevelProducer();
        k.highLevelConsumer([{ topic: exampleTopic }]);
        
        d.mount(['kafka', 'consumer', exampleTopic, '**'], function (body) {
            console.log('Consumer received: ', body);
        });

        var testNum = 0;
        var sendTest = function () {
            d.request(['kafka', 'producer', exampleTopic], 'test body ' + (testNum++))
            .spread(function (body, options) {
                console.log('Producer response: ' + options.statusCode + ' ' + JSON.stringify(body));
                setTimeout(sendTest, 1000);
            });
        };
        sendTest();
    });
});


