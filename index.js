"use strict";

module.exports = function (Domain, libs) {
    var kafkaNode = libs['kafka-node'] || require('kafka-node');

    Domain.prototype.kafkaClient = function (params) {
        var d = this;
        var kafkaRoute = ['kafka'];
        params = params || {};
        var client = new kafkaNode.Client(params.connectionString, params.clientId, params.zkOptions);
        
        return {
            highLevelConsumer: function (payloads, options) {
                var consumer = new kafkaNode.HighLevelConsumer(client, payloads, options);
                var consumerRoute = kafkaRoute.concat(['consumer'])
                consumer.on('message', function (body) {
                    var topic = body.topic;
                    var partition = body.partition;
                    d.send(consumerRoute.concat([topic, partition]), kafkaRoute.concat(void 0), JSON.parse(body.value), {
                        kafka: {
                            offset: body.offset
                            , partition: partition
                            , topic: topic
                        }
                    });
                });
            }
            , highLevelProducer: function () {
                var producer = new kafkaNode.HighLevelProducer(client);
                var producerRoute = kafkaRoute.concat(['producer']);
                d.mount(producerRoute.concat(':topic'), function (body, ctxt) {
                    producer.send([{
                        topic: ctxt.params.topic
                        , messages: [JSON.stringify(body)]
                    }], function (err, body) {
                        if (err) {
                            ctxt.return(err, { statusCode: 500 });
                        } else {
                            ctxt.return(body, { statusCode: 201 });
                        }
                    });
                });
            }
        };
    };
};

