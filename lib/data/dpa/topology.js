'use strict'; // eslint-disable-line strict

const fs = require('fs');

// generate recursively topology
function genTopo(topo, prefix, md, index) {
    const number = md[index].number;
    const domain = md[index].domain;
    const weight = md[index].weight;
    if (index === 0) {
        topo.MD = md;                               // eslint-disable-line
    }
    for (let idx = 0; idx < number; idx++) {
        const obj = {};
        if (weight) {
            if (Array.isArray(weight) && weight.length === 2) {
                obj.weight = weight[0] +
                    Math.random() * (weight[1] - weight[0]);
            } else if (!isNaN(weight)) {
                obj.weight = weight;
            }
        }
        const key = `${domain}${idx + 1}`;
        topo[key] = obj;                                // eslint-disable-line
        if (index < md.length - 1) {
            genTopo(topo[key], idx + 1, md, index + 1);
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

// generate weight distribution
function genWeightDistr(metadata, obj, depth) {
    if (!obj.leaf && obj.constructor === Object) {
        obj.wdistr = [{                                 // eslint-disable-line
            ids: Object.keys(obj).filter(key =>
                obj[key].constructor === Object),
            pdf: [],
            cdf: [],
        }];
    }
    Object.keys(obj).forEach(val => {
        if (obj[val].constructor === Object) {
            obj.wdistr[0].pdf.push(obj[val].weight || 0);
            if (!obj[val].leaf) {
                genWeightDistr(metadata, obj[val], depth + 1);
            }
        }
    });
    // normalize weight distributions
    // then update with number of bits for each domain
    if (obj.wdistr && obj.wdistr[0].pdf.length > 0) {
        const sum = obj.wdistr[0].pdf.reduce((a, b) => a + b);
        if (sum > 0) {
            /* maxValue */
            const maxValue = 1 << (metadata[depth].binImgRange[1] -
                                   metadata[depth].binImgRange[0]);
            // console.log(topoMD[depth].size, maxValue);
            /* normalize topo.MD[depth].wdistr[0].pdf */
            obj.wdistr[0].pdf.forEach((val, idx) => {
                obj.wdistr[0].pdf[idx] =                // eslint-disable-line
                    Math.floor(maxValue * val / sum);
            });
            /* compute cdf from pdf */
            obj.wdistr[0].pdf.reduce((a, b, idx) => {
                obj.wdistr[0].cdf[idx] = a + b;         // eslint-disable-line
                return obj.wdistr[0].cdf[idx];
            }, 0);
            // set last element is maxValue
            obj.wdistr[0].cdf[                          // eslint-disable-line
                obj.wdistr[0].cdf.length - 1] = maxValue;
        }
    }
}

// Remove all leaf components
// Remove `weight` property
function cleanTopo(obj) {
    if (obj.weight) {
        delete obj.weight;                              // eslint-disable-line
    }
    Object.keys(obj).forEach(val => {
        if (obj[val].constructor === Object) {
            if (obj[val].leaf) {
                delete obj[val];                        // eslint-disable-line
            } else {
                cleanTopo(obj[val]);
            }
        }
    });
}

// Move content of `wdistr` to its parent. Then delete `wdistr` property
function finalizeTopo(obj) {
    if (obj.wdistr) {
        Object.keys(obj.wdistr[0]).forEach(key => {
            obj[key] = obj.wdistr[0][key];        // eslint-disable-line
        });
        delete obj.wdistr;                 // eslint-disable-line
    }
    Object.keys(obj).forEach(val => {
        if (obj[val].constructor === Object) {
            finalizeTopo(obj[val]);
        }
    });
}

function updateMD(obj, topoMD) {
    if (!obj.MD) {
        obj.MD = topoMD.md;                             // eslint-disable-line
    }
    updateWeight(obj);

    // save raw topology
    const file = `${__dirname}/../../../${topoMD.name}.raw.json`;
    fs.writeFileSync(file, JSON.stringify(obj, null, 4), 'utf8');

    genWeightDistr(obj.MD, obj, 0);
}

// create a topology for given levels and dimension
function initTopo(topoMD) {
    const topo = {};
    genTopo(topo, '', topoMD.md, 0);
    updateMD(topo, topoMD);
    cleanTopo(topo);
    finalizeTopo(topo);
    return topo;
}

// update from a `raw` topology, i.e. contains all components with or without
// their weights
function update(topo, md) {
    updateMD(topo, md);
    cleanTopo(topo);
    finalizeTopo(topo);
    return topo;
}

exports.default = {
    init: initTopo,
    update,
};
