import crypto from 'crypto';
import kinetic from 'kineticlib';
import stream from 'stream';
import net from 'net';

import { errors } from 'arsenal';

import config from '../../Config';

let sequence = 1;

export const ds = [];

const kDrives = {
    port: config.kinetic ? config.kinetic.port : 8123,
    host: config.kinetic ? config.kinetic.host : 'localhost',
};

function newSocket() {
    const socket = new net.Socket({ allowHalfOpen: false });
    socket.setKeepAlive(true);
    socket.unref();
    return socket.connect(kDrives.port, kDrives.host);
}

/*
 * For GET operations:
 * There are probably two return PDU messages from Kinetic drives:
 * - 1st PDU indicates status of response, i.e. `success` or not
 * - 2nd PDU sent if the status is `success`. Its value contains requests object
 */
function decodeRecMsg(socket, cb) {
    socket.pause();
    // decode message
    kinetic.streamToPDU(socket, (err, pdu) => {
        if (err) {
            return cb(err);
        }
        const statusCode = pdu.getStatusCode();

        if (statusCode !== kinetic.errors.SUCCESS) {
            let err1;
            if (statusCode === kinetic.errors.NOT_FOUND) {
                err1 = errors.ObjNotFound;
            } else {
                err1 = kinetic.getErrorName(statusCode);
            }
            return cb(err1);
        }

        // This PDU contains requested object, return socket for getting data
        if (pdu.getMessageType() === kinetic.ops.GET_RESPONSE) {
            socket.resume();
            return cb(null, socket, pdu.getChunkSize());
        }
        // This message contains only status of response. Process 2nd message
        return decodeRecMsg(socket, cb);
    });
}

const backend = {
    put: function x(request, size, keyContext, reqUids, callback) {
        const value = [];
        request.on('data', data => {
            value.push(data);
        }).on('end', err => {
            if (err) {
                return callback(err);
            }
            const obj = Buffer.concat(value);
            const key = crypto.randomBytes(20);

            const options = {
                synchronization: 'WRITEBACK', // FLUSH
            };
            const pdu = new kinetic.PutPDU(sequence, key, obj.length,
                options);
            ++sequence;
            const header = pdu.read();
            const fullOBj = Buffer.concat([header, obj]);
            const socket = newSocket();
            socket.write(fullOBj, err => {
                if (err) {
                    return callback(err);
                }
                let statusCode;
                socket.on('data', chunk => {
                    const recPDU = new kinetic.PDU(chunk);
                    statusCode = recPDU.getStatusCode();
                    socket.end();
                }).on('end', err => {
                    if (err) {
                        return callback(err);
                    }
                    const err1 = (statusCode === kinetic.errors.SUCCESS) ?
                        null : kinetic.getErrorName(statusCode);
                    return callback(err1, key);
                });
                return undefined;
            });
            return undefined;
        });
    },

    get: function y(key, range, reqUids, callback) {
        const pdu = new kinetic.GetPDU(sequence, new Buffer(key.data));
        ++sequence;
        const header = pdu.read();

        const socket = newSocket();

        socket.write(header, err => {
            if (err) {
                return callback(err);
            }
            return decodeRecMsg(socket, (err, sk, objSize) => {
                if (err) {
                    return callback(err);
                }
                const value = [];
                let count = 0;
                sk.on('data', chunk => {
                    value.push(chunk);
                    count += chunk.length;
                    if (count === objSize) {
                        socket.end();
                    }
                }).on('end', err => {
                    if (err) {
                        return callback(err);
                    }
                    const obj = Buffer.concat(value);
                    return callback(null, new stream.Readable({
                        read() {
                            this.push(obj);
                            this.push(null);
                        },
                    }));
                });
                return undefined;
            });
        });
    },

    delete: function z(key, reqUids, callback) {
        if (!Buffer.isBuffer(key)) {
            key = new Buffer(key.data);
        }

        const pdu = new kinetic.DeletePDU(sequence, key);
        ++sequence;
        const socket = newSocket();
        socket.write(pdu.read(), err => {
            if (err) {
                return callback(err);
            }
            let statusCode;
            socket.on('data', chunk => {
                const recPDU = new kinetic.PDU(chunk);
                statusCode = recPDU.getStatusCode();
                socket.end();
            }).on('end', err => {
                if (err) {
                    return callback(err);
                }
                let err1;
                if (statusCode === kinetic.errors.NOT_FOUND) {
                    err1 = errors.ObjNotFound;
                } else if (statusCode !== kinetic.errors.SUCCESS) {
                    err1 = kinetic.getErrorName(statusCode);
                }
                return callback(err1);
            });
            return undefined;
        });
    },
};

export default backend;
