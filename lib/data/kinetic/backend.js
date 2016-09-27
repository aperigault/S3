import crypto from 'crypto';
import kinetic from 'kineticlib';
import stream from 'stream';
import net from 'net';

import { errors } from 'arsenal';

import config from '../../Config';

let sequence = 1;

const kDrives = {
    port: config.kinetic ? config.kinetic.port : 8123,
    host: config.kinetic ? config.kinetic.host : 'localhost',
};

function propError(pdu) {
    const statusCode = pdu.getStatusCode();
    let err;

    if (statusCode !== kinetic.errors.SUCCESS) {
        if (statusCode === kinetic.errors.NOT_FOUND) {
            err = errors.ObjNotFound;
        } else {
            err = kinetic.getErrorName(statusCode);
        }
        return err;
    }
}

function newSocket(callback) {
    const socket = new net.Socket({ allowHalfOpen: false }).pause();
    socket.setKeepAlive(true);
    socket.unref();
    return socket.connect(kDrives.port, kDrives.host, () => {
        kinetic.streamToPDU(socket, (err, pdu) => {
            if (err) {
                return callback(err);
            }
            const err1 = propError(pdu);
            return callback(err1, socket, pdu.getChunkSize());
        });
    });
}

function putKinetic(socket, value, options, callback) {
    const key = crypto.randomBytes(20);
    const obj = Buffer.concat(value);
    const pdu = new kinetic.PutPDU(sequence, key, obj.length, options);
    ++sequence;

    socket.write(Buffer.concat([pdu.read(), obj]), err => {
        if (err) {
            return callback(err);
        }
        kinetic.streamToPDU(socket, (err, pdu) => {
            const err1 = propError(pdu);
            return callback(err || err1, key);
        });
        return undefined;
    });
}

function getKinetic(socket, pdu, callback) {
    let count = 0;
    const value = [];
    const err = propError(pdu);
    socket.resume();
    socket.on('data', chunk => {
        value.push(chunk);
        count += chunk.length;
        if (count === pdu.getChunkSize()) {
            socket.end();
        }
    }).on('end', () => {
        return callback(err, new stream.Readable({
            read() {
                this.push(Buffer.concat(value));
                this.push(null);
            },
        }));
    });
}

function deleteKinetic(socket, key, callback) {
    const pdu = new kinetic.DeletePDU(sequence, key);
    ++sequence;
    socket.write(pdu.read(), err => {
        if (err) {
            return callback(err);
        }
        kinetic.streamToPDU(socket, (err, pdu) => {
            if (err) {
                return callback(err);
            }
            const err1 = propError(pdu);
            return callback(err || err1);
        });
        return undefined;
    });
}

const backend = {
    put: function putK(request, size, keyContext, reqUids, callback) {
        const value = [];
        request.on('data', data => {
            value.push(data);
        }).on('end', err => {
            if (err) {
                return callback(err);
            }
            newSocket((err, socket) => {
                const options = {
                    synchronization: 'WRITEBACK', // FLUSH
                };
                putKinetic(socket, value, options, callback);
            });
            return undefined;
        });
    },

    get: function getK(key, range, reqUids, callback) {
        newSocket((err, sock) => {
            if (err) {
                return callback(err);
            }
            const pdu = new kinetic.GetPDU(sequence, new Buffer(key.data));
            ++sequence;
            const header = pdu.read();
            sock.write(header, err => {
                if (err) {
                    return callback(err);
                }
                kinetic.streamToPDU(sock, (err, pdu) => {
                    getKinetic(sock, pdu, callback);
                    return undefined;
                });
            });
        });
    },

    delete: function delK(key, reqUids, callback) {
        if (!Buffer.isBuffer(key)) {
            key = new Buffer(key.data);
        }
        newSocket((err, socket) => {
            if (err) {
                return callback(err);
            }
            deleteKinetic(socket, key, callback);
        });
    },
};

export default backend;
