'use strict';

var mongo = require('mongodb'),
    async = require('async'),
    util = require('util'),
    ObjectID = require('mongodb').ObjectID,

    dbs = {},
    options = {
        debug: true,
        prefix: true,
        size: 100,
        max: 2610,
        name: 'statsd',
        host: '127.0.0.1',
        port: 27017,
        username: null,
        password: null,
        useAdminAuthDb: true,
    };

var connection_queue = async.queue(function (task, callback) {
    if (dbs[task.name]) {
        console.log('*** db connection exists: ' + task.name + ' ***');
        return callback(null, dbs[task.name]);
    } else {
        console.log('*** creating db connection "' + task.name + '" ***');

        var connectionOptions = {
            autoReconnect: true,
            w: 1
        }};

        if (options.username != null && options.password != null) {

            connectionOptions['user'] = options['username'];
            connectionOptions['password'] = options['password'];

            if (options.useAdminAuthDb) {
                console.log('*** authenticating using credentials from admin database for ' + task.name + ' ***');
                connectionOptions['authSource'] = 'admin';
            } else {
                console.log('*** authenticating using credentials from ' + task.name + ' database for ' + task.name + ' ***');
            }
        } else {
            console.log('*** connecting without authentication -- INSECURE');
        }

        var mongoConnectCallback = function (err, client) {
            if (err) {
                console.error('*** Error: ' + err);
                if (client) client.close();
                return callback(err);
            } else {
                console.log('** Processing connection, init successful **');
                var db = client.db(task.name);
                dbs[task.name] = db;
                return callback(null, db);
            }

        };

        if (options.connectionString == null) {
            console.log('** mongoConnectionString option is null, using the individual connection options')
            mongo.MongoClient.connect(new mongo.Server(options.host,options.port), connectionOptions, mongoConnectCallback);
        } else {
            console.log('** mongoConnectionString option is not null, using that and ignoring the individual connection options')
            mongo.MongoClient.connect(options.connectionString, connectionOptions, mongoConnectCallback);
        }

}, 1);

var database = function (name, inputDataSet, callback) {
    if (dbs[name]) {
        callback(null, dbs[name], inputDataSet);
    } else {
        connection_queue.push({name: name}, function (err) {
            callback(err, dbs[name], inputDataSet);
        });
    }
};

/**
 * Get the name of the database to connect to.
 * The name will be suffixed with _statsd if the option is enabled.
 */
var getDatabaseName = function (metric) {
    var tmp = options.prefix ? metric.split('.')[0] : options.name;

    if (tmp != 'statsd') {
        tmp += "_statsd";
        /* foolproofing "unique" database name*/
    }

    return tmp;
};

/**
 * Get the name of the collection to connect to.
 */
var getCollectionName = function (metric_type, metric) {
    var ary = metric.split('.');
    if (options.prefix) ary.shift();
    ary.unshift(metric_type);

    if (getDatabaseName(metric) != 'statsd') {
        ary.pop();
    }

    /*return ary.join('_') + (getDatabaseName(metric) == 'statsd' ? '_' + options.rate : '');*/

    return ary.join('_');
};

/**
 *    Prefix a metrics name
 */
var colMetric = function (metric) {

    var ary = metric.split('.');

    if (getDatabaseName(metric) != 'statsd') {
        return ary.pop();
    }

    return metric;
};


/**
 *    Aggregate the metrics
 */
var aggregate = {
    /**
     *    Aggregate some metrics bro
     *    @param {Number} time
     *    @param {Stirng} key
     *    @param {String} val
     */
    gauges: function (time, key, val) {
        return {
            db: getDatabaseName(key),
            col: getCollectionName('gauges', key),
            data: {
                metricKey: colMetric(key) != key ? colMetric(key) : null,
                time: time,
                gauge: val
            },
        };
    },
    /**
     *    Aggregate some counters bro
     *    @param {Number} time
     *    @param {Stirng} key
     *    @param {String} val
     */
    counters: function (time, key, val) {
        return {
            db: getDatabaseName(key),
            col: getCollectionName('counters', key),
            data: {
                metricKey: colMetric(key) != key ? colMetric(key) : null,
                time: time,
                value: val
            }
        };
    },
    /**
     *    Aggregate some sets bro
     *    @param {Number} time
     *    @param {Stirng} key
     *    @param {String} val
     */
    sets: function (time, key, val) {
        return {
            db: getDatabaseName(key),
            col: getCollectionName('sets', key),
            data: {
                metricKey: colMetric(key) != key ? colMetric(key) : null,
                time: time,
                set: val
            },
        };
    },
    /*== TIMERIT VAATII VIELÄ STEDAUSTA, DATA TULEE ERI MUODOSSA ==*/
    /**
     *    Aggregate some timer_data bro
     *    @param {Number} time
     *    @param {Stirng} key
     *    @param {String} vals
     */
    timer_data: function (time, key, val) {
        val.time = time;
        val.metricKey = (colMetric(key) != key ? colMetric(key) : null);

        return {
            db: getDatabaseName(key),
            col: getCollectionName('timers', key),
            data: val
        };
    },
    /**
     *    Aggregate some timers bro
     *    @param {Number} time
     *    @param {Stirng} key
     *    @param {String} vals
     */
    timers: function (time, key, val) {
        return {
            db: getDatabaseName(key),
            col: getCollectionName('timers', key),
            data: {
                metricKey: (colMetric(key) != key ? colMetric(key) : null),
                time: time,
                durations: val
            },
        };
    },

};

/**
 *    Insert the data to the database
 *    @method insert
 *    @param {String} database
 *    @param {String} collection
 *    @param {Object} metric
 *    @param {Function} callback
 */
var insert = function (dbName, collection, metric, callback) {
    var colInfo = {};

    /* saittikohtaisten datojen kokoa ei rajoiteta */
    if (dbName == 'statsd') {
        colInfo = {
            capped: true,
            size: options.size * options.max,
            max: options.max
        };
    }

    database(dbName, metric, function (err, db, metric) {
        if (err) {
            return callback(err);
        };

        var colExists = db.listCollections({ name: collection }).hasNext();
        if (colExists) {
            db.collection(collection, {}, function (err, collClient) {
                if (collClient == null) {
                    console.log(collection + ': failed to read an existing collection', err);
                    return null;
                }

                collClient.insert(metric, function (err, data) {
                    // console.log(collection + ': inserted into an existing collection', err);

                    if (err) callback(err);
                    if (!err) callback(false, collection);
                });
            });
        } else {
            // db.createCollection() fails if the collection already exists
            // https://www.mongodb.com/docs/manual/reference/method/db.createCollection/
            db.createCollection(collection, colInfo, function (err, collClient) {
                if (collClient == null) {
                    console.log(collection + ': failed to create a new collection', err);
                    return null;
                }

                collClient.insert(metric, function (err, data) {
                    if (err) callback(err);
                    if (!err) callback(false, collection);
                });
            });
        }
    });
};

/**
 *    our `flush` event handler
 */
var onFlush = function (time, metrics) {

    var metricTypes = ['gauges', 'timer_data', 'timers', 'counters', 'sets'];

    metricTypes.forEach(function (type, i) {
        for (var key in metrics[type]) {
            var object = aggregate[type](time, key, metrics[type][key]);

            /*
             * Handle counters collection differently
             * one row / key / date
             */
            if (object.db != 'statsd'
                && type == 'counters'
                && typeof object.data.metricKey != undefined
                && object.data.metricKey != null
            ) {
                /* Muutetaan aikaleima Date-objektiksi */
                var date = new Date(object.data.time * 1000);
                date.setUTCHours(0);
                date.setUTCMinutes(0);
                date.setUTCSeconds(0);
                object.data.time = date;

                database(object.db, object, function (err, db, object) {

                    if (err) {
                        if (db == null) console.log('could not open database connection');
                        return console.log(err);
                    }

                    /**
                     * Persist/update counter value
                     */
                    db.createCollection(object.col, {}, function (err, collClient) {

                        if (collClient == null) {
                            console.log(object.col + ': could fetch collection');
                            return null;
                        }

                        /* create indexes for collection */
                        db.ensureIndex(object.col, {'date': -1}, {}, function (err, result) {
                            if (err) console.log(err);
                            db.ensureIndex(object.col, {'metricKey': -1}, {}, function (err, result) {
                                if (err) console.log(err);
                            });
                        });

                        var _query = {
                            'metricKey': object.data.metricKey,
                            'date': object.data.time
                        };

                        var _update = {$inc: {'value': object.data.value}};

                        collClient.findAndModify(_query, {}, _update, {upsert: true, new: true}, function (err, document) {
                            if (err) console.log(err);
                            if (document !== undefined &&
                                (document.value == undefined
                                    || document.value == null
                                    || document.value < 0)
                            ) {
                                var _query = {'_id': document._id};
                                var _update = {$set: {'value': 0}};

                                collClient.findAndModify(_query, {}, _update, {new: true}, function (err, document) {
                                    if (err) console.log(err);
                                    console.log(document);
                                });
                            }
                        });
                    });


                    /**
                     * Maintain total sum in dedicated collection
                     */
                    db.createCollection(object.col + '_sum', {}, function (err, collClient) {

                        if (collClient == null) {
                            console.log(object.col + '_sum : could fetch collection');
                            return null;
                        }

                        /* create indexes for collection */
                        db.ensureIndex(object.col + '_sum', {'metricKey': -1}, {}, function (err, result) {
                            if (err) console.log(err);
                        });

                        var _query = {
                            'metricKey': object.data.metricKey,
                        };

                        var _update = {$inc: {'value': object.data.value}};

                        collClient.findAndModify(_query, {}, _update, {upsert: true, new: true}, function (err, document) {
                            if (err) console.log(err);
                            if (document !== undefined &&
                                (document.value == undefined
                                    || document.value == null
                                    || document.value < 0)
                            ) {
                                var _query = {'_id': document._id};
                                var _update = {$set: {'value': 0}};

                                collClient.findAndModify(_query, {}, _update, {new: true}, function (err, document) {
                                    if (err) console.log(err);
                                    console.log(document);
                                });
                            }
                        });
                    });


                });

                /* kaikki muut collectionit */
            } else {
                insert(object.db, object.col, object.data, function (err, data) {
                    if (err) return console.log(err);
                });
            }
            ;
        }
        ;
    });
};

/**
 *    Expose our init function to StatsD
 *    @param {Number} startup_time
 *    @param {Object} config
 *    @param {Object} events
 */
exports.init = function (startup_time, config, events) {
    if (!startup_time || !config || !events) return false;

    options.debug = config.debug;

    if (typeof config.mongoPrefix != 'boolean') {
        console.log('config.mongoPrefix must be a boolean value');
        return false;
    }

    options.rate = parseInt(config.flushInterval / 1000, 10);
    options.max = config.mongoMax ? parseInt(config.mongoMax, 10) : 2160;
    options.host = config.mongoHost || '127.0.0.1';
    options.prefix = config.mongoPrefix;
    options.name = config.mongoName;

    options.username = config.mongoUsername || options.username;
    options.password = config.mongoPassword || options.password;
    options.useAdminAuthDb = config.useAdminAuthDb || options.useAdminAuthDb;

    options.connectionString = config.mongoConnectionString || null;

    options.port = config.mongoPort || options.port;
    events.on('flush', onFlush);

    return true;
};
