'use strict'; // eslint-disable-line strict

// a default topology
const topology = {
    Rack1: {
        id: 'rack1',
        weight: 0,
        Server1: {
            id: 'server1-1',
            weight: 10,
        },
        Server2: {
            id: 'server1-2',
            weight: 20,
        },
        Server3: {
            id: 'server1-3',
            weight: 45,
        },
        Server4: {
            id: 'server1-4',
            weight: 1,
        },
    },
    Rack2: {
        id: 'rack2',
        weight: 0,
        Server1: {
            id: 'server2-1',
            weight: 50,
        },
        Server2: {
            id: 'server2-2',
            weight: 20,
        },
        Server3: {
            id: 'server2-3',
            weight: 2,
        },
        Server4: {
            id: 'server2-4',
            weight: 35,
        },
    },
    Rack3: {
        id: 'rack3',
        weight: 0,
        Server1: {
            id: 'server3-1',
            weight: 50,
        },
        Server2: {
            id: 'server3-2',
            weight: 80,
        },
        Server3: {
            id: 'server3-3',
            weight: 5,
        },
        Server4: {
            id: 'server3-4',
            weight: 10,
        },
    },
};

// generate recursively topology
function genTopo(topo, prefix, params, index) {
    const number = params[index].number;
    const field = params[index].field;
    const weight = params[index].weight;
    const pref = prefix ? `-${prefix}` : '';
    for (let idx = 0; idx < number; idx++) {
        const obj = {
            id: `${field}${pref}-${idx + 1}`,
        };
        if (weight) {
            if (Array.isArray(weight) && weight.length === 2) {
                obj.weight = weight[0] +
                    Math.random() * (weight[1] - weight[0]);
            } else if (!isNaN(weight)) {
                obj.weight = weight;
            }
        }
        const key = `${field}${idx + 1}`;
        topo[key] = obj;                                // eslint-disable-line
        if (index < params.length - 1) {
            genTopo(topo[key], idx + 1, params, index + 1);
        }
    }
}

// update recursively weight of object = sum of its objects' weigth
function updateWeight(obj) {
    if (Object.keys(obj).every(val => obj[val].constructor !== Object)) {
        obj.leaf = true;                                // eslint-disable-line
        if (!obj.weight) {
            obj.weight = 1;                             // eslint-disable-line
        }
    }
    if (!obj.leaf && obj.constructor === Object) {
        obj.weight = 0;                                 // eslint-disable-line
        Object.keys(obj).forEach(val => {
            if (obj[val].constructor === Object) {
                obj.weight +=                           // eslint-disable-line
                    updateWeight(obj[val]);
            }
        });
    }
    return obj.weight || 0;
}

// get weight distribution
function getWeight(obj, depth, distr) {
    const arr = [];
    Object.keys(obj).forEach(val => {
        if (obj[val].constructor === Object) {
            arr.push(obj[val].weight || 0);
            if (!obj[val].leaf) {
                getWeight(obj[val], depth + 1, distr);
            }
        }
    });
    if (!distr[depth]) {
        distr[depth] = [];                              // eslint-disable-line
    }
    distr[depth].push(arr);
}

// generate weight distribution
function genWeightDistr(obj) {
    if (!obj.leaf && obj.constructor === Object) {
        obj.wdistr = [];                                // eslint-disable-line
    }
    Object.keys(obj).forEach(val => {
        if (obj[val].constructor === Object) {
            obj.wdistr.push(obj[val].weight || 0);
            if (!obj[val].leaf) {
                genWeightDistr(obj[val]);
            }
        }
    });
}

function weightProcessing(obj) {
    updateWeight(obj);
    genWeightDistr(obj);
}

// create a topology for given levels and dimension
const defaultInit = [{
    field: 'Rack',
    number: 3,
}, {
    field: 'Server',
    number: 3,
}, {
    field: 'Drive',
    number: 5,
    // number of `[min, max]` -> uniformly random between min and max
    weight: [0.2, 1.5],
}];
function initTopo(_init) {
    const init = _init || defaultInit;
    const topo = {};
    genTopo(topo, '', init, 0);
    weightProcessing(topo);
    return topo;
}

// weight process on default topology
weightProcessing(topology);

exports.topology = {
    default: topology,
    init: initTopo,
    update: weightProcessing,
    // weight: weights,
};
