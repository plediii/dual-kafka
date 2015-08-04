/*jslint node: true */
"use strict";

var _ = require('lodash');
var assert = require('assert');
var dualapi = require('dualapi');

var kafkaNode = require('./mock-kafka');

describe('dual-kafka', function () {

    var d;
    var kafkaHooks;
    beforeEach(function () {
        kafkaHooks = {};
        d = dualapi
            .use(function (Domain, libs) {
                libs['kafka-node'] = kafkaNode(kafkaHooks)
            })
            .use(require('../index'))();
    });

    describe('kafkaClient', function () {
        it('should be available on domain', function () {
            assert(_.isFunction(d.kafkaClient));
        });

        it('should call the kafka-node constructor', function (done) {
            kafkaHooks.clientConstructor = function (connectionString, clientId, zkOptions) {
                assert.equal(connectionString, 'zookeeper:2190');
                assert.equal(clientId, 'test-client');
                assert.equal(zkOptions.back, 'indeed');
                done();
            };
            d.kafkaClient({
                connectionString: 'zookeeper:2190'
                , clientId: 'test-client'
                , zkOptions: {
                    back: 'indeed'
                }
            });
        });

        it('should expose the kafka-node client', function () {
            var k = d.kafkaClient();
            assert(k.client);
        });
    });

    describe('consumer', function () {
        var kafkaClient;
        beforeEach(function () {
            kafkaClient = d.kafkaClient();
        });
        
        describe('constructor', function () { 
            it('should be available on the client object', function () {
                assert(_.isFunction(kafkaClient.highLevelConsumer));
            });

            it('should construct a consumer with provided topics', function (done) {
                kafkaHooks.highLevelConsumerConstructor = function (client, payloads) {
                    assert.equal(payloads[0].topic, 'target')
                    done();
                }
                kafkaClient.highLevelConsumer([{ topic: 'target'}]);
            });

            it('should construct a consumer with provided options', function (done) {
                kafkaHooks.highLevelConsumerConstructor = function (client, payloads, options) {
                    assert.equal(options.right, 'on')
                    done();
                }
                kafkaClient.highLevelConsumer([], { right: 'on' });
            });
        });

        describe('onMessage', function () {
            var mockConsumer;
            beforeEach(function () {
                kafkaHooks.highLevelConsumerConstructor = function () {
                    mockConsumer = this;
                };
                kafkaClient.highLevelConsumer();
            });

            var mockSend = function () {
                mockConsumer._recv({
                    topic: 'promised',
                    value: '{"time":2440629141879,"value":96}',
                    offset: 197,
                    partition: 1,
                    key: new Buffer('key')
                });
            };

            it('should send messages to consumer routes', function (done) {
                d.mount(['kafka', 'consumer', '**'], function (body, ctxt) {
                    done();
                });
                mockSend();
            });

            it('should send messages to consumer routes with topic in options', function (done) {
                d.mount(['kafka', 'consumer', '**'], function (body, ctxt) {
                    assert.equal('promised', ctxt.options.kafka.topic);
                    done();
                });
                mockSend();
            });

            it('should send messages to consumer routes with offset in options', function (done) {
                d.mount(['kafka', 'consumer', '**'], function (body, ctxt) {
                    assert.strictEqual(197, ctxt.options.kafka.offset);
                    done();
                });
                mockSend();
            });

            it('should send messages to consumer routes with partition in options', function (done) {
                d.mount(['kafka', 'consumer', '**'], function (body, ctxt) {
                    assert.strictEqual(1, ctxt.options.kafka.partition);
                    done();
                });
                mockSend();
            });

            it('should send messages to consumer routes with message as body', function (done) {
                d.mount(['kafka', 'consumer', '**'], function (body, ctxt) {
                    assert.strictEqual(96, body.value);
                    done();
                });
                mockSend();
            });

            it('should send messages to specific topic subroute', function (done) {
                d.mount(['kafka', 'consumer', 'promised', '**'], function (body, ctxt) {
                    done();
                });
                mockSend();
            });

            it('should send messages to specific topic/partition subroute', function (done) {
                d.mount(['kafka', 'consumer', 'promised', '1'], function (body, ctxt) {
                    done();
                });
                mockSend();
            });
        });
    });

    describe('producer', function () {
        var kafkaClient;
        beforeEach(function () {
            kafkaClient = d.kafkaClient();
        });
        
        it('should be available on the client object', function () {
            assert(_.isFunction(kafkaClient.highLevelProducer));
        });

        it('should construct a producer from the client', function (done) {
            kafkaHooks.highLevelProducerConstructor = function (client) {
                done();
            }
            kafkaClient.highLevelProducer();
        });

        describe('send', function () {
            beforeEach(function () {
                kafkaClient.highLevelProducer();
            });

            it('should be mounted at route producer', function () {
                assert(d.listeners(['kafka', 'producer', '*']).length > 0);
            });

            it('should initiate a producer send on a topic described by route', function (done) {
                kafkaHooks.highLevelProducerSend = function (payloads) {
                    assert.equal(payloads[0].topic, 'child');
                    done();
                };
                d.send(['kafka', 'producer', 'child'], [], { ancient: 'legend' });
            });

            it('should initiate a producer send with a stringified copy of the body', function (done) {
                kafkaHooks.highLevelProducerSend = function (payloads) {
                    var body = JSON.parse(payloads[0].messages[0]);
                    assert.equal(body.ancient, 'legend');
                    done();
                };
                d.send(['kafka', 'producer', 'child'], [], { ancient: 'legend' });
            });

            it('should reply with 201 status code on successful send', function (done) {
                kafkaHooks.highLevelProducerReply = function (payloads) {
                    return [null, 'ok'];
                };
                d.request(['kafka', 'producer', 'child'], 'body')
                .spread(function (body, options) {
                    assert.equal(201, options.statusCode);
                    done();
                });
            });

            it('should reply with 500 status code on successful send', function (done) {
                kafkaHooks.highLevelProducerReply = function (payloads) {
                    return ['fail'];
                };
                d.request(['kafka', 'producer', 'child'], 'body')
                .spread(function (body, options) {
                    assert.equal(500, options.statusCode);
                    done();
                });
            });
        });
    });
});
