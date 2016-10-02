'use strict'; // eslint-disable-line strict

const assert = require('assert');
const Readable = require('stream').Readable;
const fs = require('fs');

const CronJob = require('cron').CronJob;

const Logger = require('werelogs').Logger;
const EcLib = require('eclib');
const keygen = require('./keygen').keygen;
const dp = require('./dp').dp;
const topology = require('./topology').default;
const constants = require('../../../constants').default;

const config = require('../../Config').default;

const file = require('../file/backend').default;
const inMemory = require('../in_memory/backend').default;
const Sproxy = require('sproxydclient');

const dataPath = require('../../Config').default.filePaths.dataPath;

const SEPARATOR = {
    key: ':',
    frag: ',',
};
const PREF_BE = {
    mem: 'm',
    file: 'f',
    scality: 's',
};

const backends = process.env.ENABLE_DP === 'true' ? config.backends.data : [];
// DBE contains all data backends to store data
const DBE = {};
backends.forEach(be => {
    if (be === 'mem') {
        DBE.mem = inMemory;
    } else if (be === 'file') {
        DBE.file = file;
    } else if (be === 'scality') {
        DBE.scality = new Sproxy({
            bootstrap: config.sproxyd.bootstrap,
            log: config.log,
            chordCos: config.sproxyd.chordCos,
        });
    }
});

const backendId = {
    EC_BACKEND_NULL: 0,
    EC_BACKEND_JERASURE_RS_VAND: 1,
    EC_BACKEND_JERASURE_RS_CAUCHY: 2,
    EC_BACKEND_FLAT_XOR_HD: 3,
    EC_BACKEND_ISA_L_RS_VAND: 4,
    EC_BACKEND_SHSS: 5,
    EC_BACKEND_LIBERASURECODE_RS_VAND: 6,
    EC_BACKENDS_MAX: 99,
};

const checksumType = {
    CHKSUM_NONE: 1,
    CHKSUM_CRC32: 2,
    CHKSUM_MD5: 3,
    CHKSUM_TYPES_MAX: 99,
};

const gfDim = {
    RS: 8,
    XOR: 1,
};

const ecDefault = {
    bc_id: backendId.EC_BACKEND_JERASURE_RS_VAND, // eslint-disable-line
    k: 6,
    m: 3,
    w: gfDim.RS,
    hd: 4,
    ct: checksumType.CHKSUM_CRC32,
};

// list of all possible placements
const PLACEMENT = {
    none: 0,
    data: 1,
    parity: 2,
    both: 3,
};

const CHUNK_SIZE = 64 * 1024; // 64KB

const repairParams = {
    // repair every `number` of `unit`s
    // job starts from 2am
    schedule: {
        unit: 'day',
        number: 5,
    },
    retriesNb: 3,              // retries number for checking fragments
    interval: 30 * 60 * 1e3,   // 30 mins, for checking object's availability
};

/*
 * create a readable stream from a buffer
 */
function createReadbleStream(buf, size) {
    const chunkSize = Math.min(CHUNK_SIZE, size);
    let start = 0;
    return new Readable({
        read: function read() {
            while (start < size) {
                const finish = Math.min(start + chunkSize, size);
                this.push(buf.slice(start, finish));
                start += chunkSize;
            }
            if (start >= size) {
                this.push(null);
            }
        },
    });
}

class DpClient {
    /**
     * This represent our interface with the object client.
     * @constructor
     * @constructor
     * @param {Object} [opts] - Contains options used by the library
     * @param {String} [opts.pp] - placement policy
     * @param {Object} [opts.ec] - erasure codes parameters
     * @param {Number} [opts.ec.bc_id=0] - Backend ID
     * @param {Number} [opts.ec.k=8] - Number of data fragments
     * @param {Number} [opts.ec.m=4] - Number of parity fragments
     * @param {Number} [opts.ec.w=0] - word size (in bits)
     * @param {Number} [opts.ec.hd=0] - hamming distance (== m for Reed-Solomon)
     * @param {Number} [opts.ec.ct=0] - checksum type
     */
    constructor(opts) {
        const options = opts || {};
        this.ecParams = options.ec || ecDefault;
        this.kmin = this.ecParams.k + this.ecParams.m - this.ecParams.hd + 1;
        this.ecParamsStr = JSON.stringify(this.ecParams);
        this.ec = new EcLib(this.ecParams);
        this.ec.init();

        this.setupLogging(options.log);

        // data placement
        // `this.dp` is an array of 4 numbers [a, b, c, d]
        //  ith element define which type of fragments are stored in ith data
        //      backend
        //      - 0: not store in this backend
        //      - 1: store only data in this backend
        //      - 2: store only parity in this backend
        //      - 3: store both data & parity in this backend
        this.dp = options.dp;
        if (!this.dp) {
            this.dp = new Array(4).fill(0);
            backends.forEach((be, index) => {
                let idx = 'none';
                if (constants.topology[be] && constants.topology[be].dp) {
                    idx = constants.topology[be].dp;
                }
                this.dp[index] = PLACEMENT[idx];
            });
        }
        // determine backend for each fragment
        this.placement = {
            indiv: new Array(this.ecParams.k + this.ecParams.m),
            group: backends.map(() => []),
        };
        this.setPlacement();

        this.fragHeaderSize = this.ec.getHeaderSize();
        this.zeroHeader = new Buffer(this.fragHeaderSize).fill(0);

        // params for availability object checking
        this.cronParams = repairParams;
        this.jobs = {};

        // params for availability object checking
        this.cronParams = repairParams;
        this.jobs = {};

        this.topologies = {};
        if (options.topologies) {
            Object.keys(options.topologies).forEach(be => {
                this.topologies[be] = topology.update(options.topologies[be]);
            });
        } else {
            Object.keys(constants.topology).forEach(be => {
                const file =
                    `${__dirname}/../../../${constants.topology[be].name}.json`;
                this.topologies[be] = JSON.parse(fs.readFileSync(file));
            });
        }
    }

    setupLogging(config) {
        let options = undefined;
        if (config !== undefined) {
            options = {
                level: config.logLevel,
                dump: config.dumpLevel,
            };
        }
        this.logging = new Logger('DpClient', options);
    }

    createLogger(reqUids) {
        return reqUids ?
            this.logging.newRequestLoggerFromSerializedUids(reqUids) :
            this.logging.newRequestLogger();
    }

    getCronTime() {
        const date = new Date();
        // init time at 02am + current minutes, seconds
        let cronTime = `${date.getSeconds()} ${date.getMinutes()} 02 `;

        // support every 'number' days
        if (this.cronParams.schedule.unit === 'day') {
            const today = date.getDate();
            const day = [];
            for (let idx = 0; idx < 31;
                idx += this.cronParams.schedule.number) {
                day.push(`${(today - 1 + idx) % 31 + 1}`);
            }
            cronTime += `${day.join(',')} `;
        } else {
            cronTime += '* ';
        }

        // support every 'number' months
        if (this.cronParams.schedule.unit === 'month') {
            const thisMonth = date.getMonth();
            const month = [];
            for (let idx = 0; idx < 12;
                idx += this.cronParams.schedule.number) {
                month.push(`${(thisMonth + idx) % 12}`);
            }
            cronTime += `${month.join(',')} `;
        } else {
            cronTime += '* ';
        }

        // support every week
        if (this.cronParams.schedule.unit === 'week') {
            cronTime += `${date.getDay()} `;
        } else {
            cronTime += '* ';
        }

        return cronTime;
    }

    // determine backend for each fragment
    setPlacement() {
        const log = this.createLogger();
        const datasNb = this.dp.filter(e => (e === PLACEMENT.data)).length;
        const bothsNb = this.dp.filter(e => (e === PLACEMENT.both)).length;
        const paritiesNb = this.dp.filter(e => (e === PLACEMENT.parity)).length;
        const dataPart = Math.ceil(this.ecParams.k / datasNb);
        const parityPart = Math.ceil(this.ecParams.m / paritiesNb);
        if (datasNb + bothsNb + paritiesNb === 0) {
            log.warn('No backend to store frags. Set 1st backend to both');
            // set 1st data backend as 'both'
            this.dp[0] = PLACEMENT.both;
        }
        if (datasNb + bothsNb === 0) {
            // change 1st backend that currently stores 'parity' to stores
            // 'both'
            log.warn('No backend to store data frags. Set 1st backend to both');
            const idx = this.dp.indexOf(PLACEMENT.parity);
            this.dp[idx] = PLACEMENT.both;
        }
        if (bothsNb + paritiesNb === 0) {
            // change 1st backend that currently stores 'data' to stores
            // 'both'
            log.warn('No backend to store parities. Set 1st backend to both');
            const idx = this.dp.indexOf(PLACEMENT.data);
            this.dp[idx] = PLACEMENT.both;
        }

        // for data fragments
        let endD = 0;
        let fragIdx = 0;
        this.dp.forEach((dp, idx) => {
            if (dp === PLACEMENT.data) {
                endD += dataPart;
                if (endD > this.ecParams.k) {
                    endD = this.ecParams.k;
                }
                for (; fragIdx < endD; fragIdx++) {
                    this.placement.group[idx].push(fragIdx);
                    this.placement.indiv[fragIdx] = backends[idx];
                }
            }
        });
        endD = fragIdx;
        // for parity fragments
        let endP = this.ecParams.k + this.ecParams.m - 1;
        fragIdx = endP;
        this.dp.forEach((dp, idx) => {
            if (dp === PLACEMENT.parity) {
                endP -= parityPart;
                if (endP < this.ecParams.k - 1) {
                    endP = this.ecParams.k - 1;
                }
                for (; fragIdx > endP; fragIdx--) {
                    this.placement.group[idx].push(fragIdx);
                    this.placement.indiv[fragIdx] = backends[idx];
                }
            }
        });
        endP = fragIdx;
        // for both data+parity fragments from endD to endP
        const bothPart = Math.ceil((endP - endD + 1) / bothsNb);
        let end = endD;
        fragIdx = endD;
        this.dp.forEach((dp, idx) => {
            if (dp === PLACEMENT.both) {
                end += bothPart;
                if (end > endP + 1) {
                    end = endP + 1;
                }
                for (; fragIdx < end; fragIdx++) {
                    this.placement.group[idx].push(fragIdx);
                    this.placement.indiv[fragIdx] = backends[idx];
                }
            }
        });
    }

    /**
     * This sends a PUT request to object client.
     * @param {http.IncomingMessage} stream - Request with the data to send
     * @param {number} size - data size in the stream
     * @param {Object} params - parameters for key generation
     * @param {String} reqUids - The serialized request id
     * @param {Callback} callback - callback
     * @returns {undefined}
     */
    put(stream, size, params, reqUids, callback) {
        assert(stream.readable, 'stream should be readable');
        const log = this.createLogger(reqUids);

        // generate key for object
        const key = keygen.obj(params, this.dp, this.ecParams);

        if (size === 0) {
            return DBE[backends[0]].put(stream, size, params, reqUids, callback,
                key);
        }

        const codeLen = this.ecParams.k + this.ecParams.m;
        const alignedSize = this.ec.getAlignedDataSize(size);
        const fragSize = alignedSize / this.ecParams.k;
        const fullFragSize = fragSize + this.fragHeaderSize;
        let noError = true;

        // generate key for fragments
        const keys = keygen.all(key);

        const object = new Buffer(alignedSize);
        let cursor = 0;
        let fragCursor = 0;
        let dataLen = 0;

        // get locations for all fragments
        const paths = this.getLocations(keys);
        if (!paths) {
            const err = 'Cannot find locations to store fragments';
            log.error(err, { key });
            return callback(err);
        }
        const locations = paths.map((path, idx) => {
            if (this.placement.indiv[idx] === 'file') {
                return `${dataPath}/${path}/${keys[idx]}`;
            }
            return path ? `${path}/${keys[idx]}` : keys[idx];
        });
        const fullKey = this.encodeFullKey(key, paths);
        const done = cb => {
            // start a cron job for repair object
            const job = new CronJob({
                cronTime: this.getCronTime(),
                onTick: () => {
                    this.repair(fullKey, err => {
                        if (err) {
                            log.error('Cannot repair object',
                                { fullKey, error: err });
                        }
                    });
                },
                start: true,
            });
            this.jobs[fullKey] = job;
            return cb(null, fullKey);
        };

        let donesNb = 0;
        let donesAll = 0;
        stream.on('data', chunk => {
            chunk.copy(object, cursor);
            const cSize = chunk.length;
            cursor += cSize;
            dataLen += cSize;
            if (dataLen >= fragSize) {
                const fragsNb = Math.floor(dataLen / fragSize);
                const len = fragsNb * fragSize;
                const start = fragCursor * fragSize;
                const buf = object.slice(start, start + len);
                dataLen -= len;

                const startIdx = fragCursor;
                fragCursor += fragsNb;

                this.streamData(buf, len, startIdx, fragSize, size, keys,
                    locations, params, log, (err, keysNb) => {
                        if (err) {
                            noError = false;
                            log.error('error from datastore', { error: err });
                            return undefined;
                        }

                        donesNb += keysNb;
                        if (donesNb === codeLen) {
                            return done(callback);
                        }
                        return undefined;
                    });
            }
        });
        stream.on('error', err => {
            log.error('error from datastore', { error: err });
            return this.cleanFrags(err, keys, log, callback);
        });
        stream.on('end', () => {
            if (!noError) {
                return this.cleanFrags('error from datastore', keys, log,
                    callback);
            }
            noError = true;
            // last data fragments
            if (dataLen > 0 || fragCursor < this.ecParams.k) {
                object.fill(0, cursor);
                const start = fragCursor * fragSize;
                const buf = object.slice(start);
                this.streamData(buf, alignedSize - start, fragCursor, fragSize,
                    size, keys, locations, params, log, (err, keysNb) => {
                        donesAll++;

                        if (err) {
                            noError = false;
                            log.error('error from datastore', { error: err });
                            if (donesAll === 2) {
                                return this.cleanFrags('error from datastore',
                                    keys, log, callback);
                            }
                            return undefined;
                        }
                        donesNb += keysNb;
                        if (donesNb === codeLen) {
                            return done(callback);
                        }
                        return undefined;
                    });
            } else {
                donesAll++;
            }
            // generate parity fragments
            this.ec.encode(object, (err, dataArr, parityArr) => {
                if (err) {
                    noError = false;
                    log.error('error from datastore', { error: err });
                    return undefined;
                }
                let count = 0;

                return parityArr.forEach((frag, idx) => {
                    this.streamFrag(frag, this.ecParams.k + idx, fullFragSize,
                        keys, locations, params, log, err => {
                            count++;
                            if (count === this.ecParams.m) {
                                donesAll++;
                            }
                            if (err) {
                                noError = false;
                                log.error('error from datastore',
                                    { error: err });
                                if (donesAll === 2) {
                                    return this.cleanFrags(
                                        'error from datastore', keys, log,
                                        callback);
                                }
                                return undefined;
                            }
                            donesNb++;

                            if (donesNb === codeLen) {
                                return done(callback);
                            }
                            return undefined;
                        });
                });
            });
            return undefined;
        });
        return undefined;
    }

    /**
     * This sends a GET request to object client.
     * @param {String} key - The key associated to the value
     * @param { Number [] | Undefined} range - range (if any) with first
     * element the start and the second element the end
     * @param {String} reqUids - The serialized request id
     * @param {Callback} callback - callback
     * @returns {undefined}
     */
    get(key, range, reqUids, callback) {
        if (typeof key !== 'string') {
            key = key.toString(); // eslint-disable-line
        }
        const log = this.createLogger(reqUids);

        const res = this.decodeFullKey(key);
        const keys = res.keys;
        const locations = res.locations;
        const backend = res.backend;

        const fragments = [];
        let failedFragsNb = 0;
        let startDecoding = false;
        keys.forEach((keyFrag, fragIdx) => {
            DBE[backend[fragIdx]].get(keyFrag, null, reqUids, (err, val) => {
                if (err) {
                    log.error('error from dpClient get', { error: err });
                    failedFragsNb++;
                    if (failedFragsNb === this.ecParams.m + 1) {
                        return callback(err);
                    }
                    return undefined;
                }
                const buf = [];
                val.on('data', buffer => {
                    buf.push(buffer);
                });
                val.on('error', err => {
                    log.error('error from dpClient get', { error: err });
                    failedFragsNb++;
                    if (failedFragsNb === this.ecParams.m + 1) {
                        return callback(err);
                    }
                    return undefined;
                });
                val.on('end', () => {
                    const dataBuf = Buffer.concat(buf);

                    if (dataBuf.length === 0) {
                        return callback(null,
                            createReadbleStream(new Buffer(0), 0));
                    }

                    fragments.push(dataBuf);
                    startDecoding = (fragments.length === this.ecParams.k);

                    if (startDecoding) {
                        this.ec.decode(fragments, 0, (err, obj) => {
                            if (err) {
                                log.error('error from dpClient decode',
                                    { error: err });
                                return callback(err);
                            }

                            let stream;
                            if (range && range.length === 2) {
                                stream = createReadbleStream(
                                    obj.slice(range[0], range[1] + 1),
                                    range[1] - range[0] + 1);
                            } else {
                                stream = createReadbleStream(obj, obj.length);
                            }
                            return callback(null, stream);
                        });
                    }
                    return undefined;
                });

                return undefined;
            }, locations[fragIdx]);
        });
    }

    /**
     * This sends a DELETE request to object client.
     * @param {String} key - The key associated to the value
     * @param {String} reqUids - Serialized request ID
     * @param {Callback} callback - callback
     * @returns {undefined}
     */
    delete(key, reqUids, callback) {
        if (typeof key !== 'string') {
            key = key.toString(); // eslint-disable-line
        }
        let noError = true;

        const log = this.createLogger(reqUids);

        const res = this.decodeFullKey(key);
        const keys = res.keys;
        const locations = res.locations;
        const backend = res.backend;

        const len = keys.length;
        let idx = 0;

        keys.forEach((keyFrag, fragIdx) => {
            DBE[backend[fragIdx]].delete(keyFrag, reqUids, err => {
                if (err) {
                    log.error('error from dpClient delete', { error: err });
                    if (noError) {
                        noError = false;
                        return callback(err);
                    }
                    return undefined;
                }
                idx++;
                if (idx === len && noError) {
                    if (this.jobs[key]) {
                        log.debug('Release cron jobs for deleted object',
                            { key });
                        // stop cron repair for the deleted object
                        this.jobs[key].stop();
                        this.jobs[key] = undefined;
                    }
                    return callback();
                }
                return undefined;
            }, locations[fragIdx]);
        });
    }

    /**
     * This trigger a repair of an object
     * @param {String} key - The object's key
     * @param {Callback} callback - callback
     * @returns {undefined}
     */
    repair(key, callback) {
        assert.strictEqual(typeof key, 'string');
        const log = this.createLogger(key);

        log.debug('Repairing object', { key });

        const res = this.decodeFullKey(key);
        const keys = res.keys;
        const locations = res.locations;
        const backend = res.backend;

        const codeLen = this.ecParams.k + this.ecParams.m;

        const fragments = [];
        const lostFragsIds = [];

        const repairFrags = (lostFragsIds, liveFrags, cb) => {
            if (lostFragsIds.length > this.ecParams.m) {
                log.error('Cannot repair object', { key });

                if (this.jobs[key]) {
                    log.debug('Release cron jobs for un-recoverable object',
                        { key });
                    // stop cron repair for un-recoverable object
                    this.jobs[key].stop();
                    this.jobs[key] = undefined;
                }

                return cb('Cannot repair object');
            }
            const fragSize = liveFrags[0].length;
            // sort
            lostFragsIds.sort((a, b) => a - b);
            log.debug('Repairing lost frags', { lostFragsIds });

            this.ec.reconstruct(fragments, lostFragsIds, (err, allFrags) => {
                if (err) {
                    return cb(err);
                }
                log.debug('Recovered fragments. Re-writing..',
                    { lostFragsIds });
                const recoveredFrags = allFrags.filter((frag, idx) =>
                    lostFragsIds.indexOf(idx) > -1);

                let noError = true;
                let count = 0;
                // re-write lost fragments
                recoveredFrags.forEach((frag, idx) => {
                    const fragIdx = lostFragsIds[idx];
                    const fragKey = keys[fragIdx];
                    const location = locations[lostFragsIds[idx]];
                    const stream = createReadbleStream(frag, fragSize);
                    DBE[backend[fragIdx]].put(stream, fragSize, null, key,
                        err => {
                            if (err) {
                                log.error('error from datastore',
                                    { error: err, backend });
                                if (noError) {
                                    noError = false;
                                    return cb(err);
                                }
                                return undefined;
                            }
                            count++;
                            if (noError && count === recoveredFrags.length) {
                                log.debug('Recovered frags are stored',
                                    { lostFragsIds });
                                return cb();
                            }
                            return undefined;
                        }, fragKey, location);
                });
                return undefined;
            });
            return undefined;
        };

        keys.forEach((keyFrag, fragIdx) => {
            this.checkAvailFrag(keyFrag, log, (err, val) => {
                if (err) {
                    lostFragsIds.push(fragIdx);
                } else {
                    fragments.push(val);
                }
                if (lostFragsIds.length + fragments.length === codeLen) {
                    if (lostFragsIds.length > 0) {
                        return repairFrags(lostFragsIds, fragments, callback);
                    }
                    log.debug('Object is of full availability', { key });
                    return callback();
                }
                return undefined;
            }, locations[fragIdx], backend[fragIdx]);
        });
    }

    /**
     * Stream fragment
     * @param{buffer} frag - fragment to be stored
     * @param{number} fragIdx - index of fragment
     * @param{number} fragSize - full size of fragment (header included)
     * @param{array} keys - array of fragments' key
     * @param{array} locs - array of fragments' location
     * @param{object} params - parameters of object
     * @param{object} log - logger
     * @param{callback} callback - callback(err)
     * @return{this} this
     */
    streamFrag(frag, fragIdx, fragSize, keys, locs, params, log, callback) {
        const stream = createReadbleStream(frag, fragSize);
        const backend = this.placement.indiv[fragIdx];
        log.debug(`Streaming fragment ${fragIdx}\n`);
        DBE[backend].put(stream, fragSize, params,
            log.getSerializedUids(), err => {
                if (err) {
                    log.error('error from datastore',
                        { error: err, backend });
                    return callback(err);
                }
                log.debug(`Fragment ${fragIdx} is stored\n`);
                return callback();
            }, keys[fragIdx], locs[fragIdx]);
    }

    /**
     * split a buffer to multiple fragments then store them
     * supposing len is multiple of fragSize
     * @param{buffer} buf - buffer to store
     * @param{number} len - buffer size
     * @param{number} start - starting index of fragments
     * @param{number} fragSize - data fragment size
     * @param{number} objSize - object size
     * @param{array} keys - array of fragments' key
     * @param{array} locs - array of fragments' location
     * @param{object} params - parameters of object
     * @param{object} log - logger
     * @param{callback} callback - callback(err, fragsNb)
     * @return{this} this
     */
    streamData(buf, len, start, fragSize, objSize, keys, locs, params, log,
        callback) {
        const fragsNb = len / fragSize;
        let cursor = 0;
        let donesNb = 0;
        function cb(err) {
            if (err) {
                return callback(err);
            }
            donesNb++;
            if (donesNb === fragsNb) {
                return callback(null, fragsNb);
            }
            return undefined;
        }

        for (let idx = 0; idx < fragsNb; idx++, cursor += fragSize) {
            const fragIdx = start + idx;
            const frag = Buffer.concat([
                new Buffer(this.zeroHeader),
                buf.slice(cursor, cursor + fragSize),
            ]);
            // update header for fragments
            this.ec.addFragmentHeader(frag, fragIdx, objSize, fragSize);
            log.debug(`Added header for fragment ${fragIdx}\n`);
            this.streamFrag(frag, fragIdx, fragSize + this.fragHeaderSize,
                keys, locs, params, log, cb);
        }
    }

    /**
     * Delete stored fragments
     * @param{object} error - error to be return
     * @param{array} keys - array of fragments' key
     * @param{object} log - logger
     * @param{callback} callback - callback(error)
     * @return{this} this
     */
    cleanFrags(error, keys, log, callback) {
        const storedKeys = keys.filter(key => key !== undefined).join(',');
        if (storedKeys === '') {
            return callback(error);
        }
        return this.delete(storedKeys, log.getSerializedUids(), err => {
            if (err) {
                log.debug(`Failed to clean put ${err}`);
            }
            return callback(error);
        });
    }

    /**
     * This checks availability of a fragment
     * @param {String} key - The key associated to the fragment
     * @param {object} log - Logger
     * @param {Callback} callback - callback (err, fragment)
     * @param {string} path - fragment path
     * @param {string} backend - backend stores the fragment
     * @returns {undefined}
     */
    checkAvailFrag(key, log, callback, path, backend) {
        const maxRetriesNb = this.cronParams.retriesNb;
        const interval = this.cronParams.interval;

        assert.strictEqual(typeof key, 'string');

        const getFrag = (key, reqUids, retriesNb, cb) => {
            log.debug('Checking availability fragment',
                { key, retry: retriesNb });

            const next = (err, cb) => {
                log.error('error check availability', { error: err });
                const newRetriesNb = retriesNb + 1;
                if (newRetriesNb >= maxRetriesNb) {
                    return cb(err);
                }
                setTimeout(getFrag, interval, key, reqUids, newRetriesNb,
                    callback);
                return undefined;
            };

            DBE[backend].get(key, null, reqUids, (err, val) => {
                if (err) {
                    return next(err, cb);
                }
                const buf = [];
                val.on('data', buffer => {
                    buf.push(buffer);
                });
                val.on('error', err => {                // eslint-disable-line
                    return next(err, cb);
                });
                val.on('end', () => {
                    const frag = Buffer.concat(buf);
                    return cb(null, frag);
                });
                return undefined;
            }, path);
        };

        return getFrag(key, log.getSerializedUids(), 0, callback);
    }

    getLocations(keys) {
        const ids = keys.map(id => parseInt(id.slice(0, 6), 16).toString(2));
        ids.forEach((id, idx) => {
            while (ids[idx].length < 24) {
                ids[idx] = `0${ids[idx]}`;
            }
        });
        const locations = new Array(ids.length);
        this.placement.group.forEach((pl, index) => {
            if (pl.length === 0) {
                return undefined;
            }
            const _ids = pl.map(idx => ids[idx]);
            const res = dp.getLocations(this.topologies[backends[index]], _ids);
            pl.forEach((id, idx) => {
                locations[id] = res[idx];
            });
        });
        return locations;
    }

    encodeFullKey(key, paths) {
        const _paths = paths.map((path, idx) =>
            `${PREF_BE[this.placement.indiv[idx]]}${path}`);
        return `${key}${SEPARATOR.key}${_paths.join(SEPARATOR.frag)}`;
    }

    decodeFullKey(key) {
        const res = {};
        const _key = key.split(SEPARATOR.key);
        res.keys = keygen.all(_key[0]);
        const paths = _key[1].split(SEPARATOR.frag);
        res.backend = new Array(res.keys.length);
        res.locations = paths.map((path, idx) => {
            if (path.slice(0, 1) === PREF_BE.file) {
                res.backend[idx] = 'file';
                return `${dataPath}/${path.slice(1)}/${res.keys[idx]}`;
            }
            res.backend[idx] = 'mem';
            const _path = path.slice(1);
            return _path ? `${_path}/${res.keys[idx]}` : res.keys[idx];
        });
        return res;
    }
}

module.exports = DpClient;
