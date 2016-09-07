'use strict'; // eslint-disable-line strict

// const max = {
//     8: 0xFFFFFFFF,
//     16: 0xFFFFFFFFFFFFFFFF,
// };

/**
 * The function generate a random set of number
 * @param{array} weights - weight function
 *  .cdf: contain increasing order of numbers. The last number should be
 *      max.size
 *  .pdf: contain numbers whose sum = max.size
 * @param{boolean} replacement - flag allowing replacement of output numbers
 *  true: output could contain repeated number
 *  false: output contains distint numbers
 * @param{array} ids - array of input's identities that are hex string of length
 *  8 or 16 characters
 * @param{number} maxValue - max of ids' range
 * @return{array} array of ids.length random number drawn based on the weight
 *  distribution
 */
function dp(weights, replacement, ids, maxValue) {
    const len = ids.length;
    // not-found case
    if (!replacement && weights.length < len) {
        return undefined;
    }

    const arr = new Array(len);
    if (replacement) {
        ids.forEach((id, idx) => {
            const nb = parseInt(id, 16);
            let i = 0;
            while (weights.cdf[i] < nb) {
                i++;
            }
            arr[idx] = i;
        });
    } else {
        const weight = weights.pdf.slice();
        let drawnSum = 0;
        ids.forEach((id, idx) => {
            let nb = parseInt(id, 16) % (maxValue - drawnSum);
            let i = 0;
            while (nb > 0) {
                if (weight[i]) {
                    nb -= weight[i];
                }
                i++;
            }
            drawnSum += weight[i];
            weight[i] = undefined;
            arr[idx] = i;
        });
    }
    return arr;
}

// for test
// const weights = {
//     pdf: [1232342, 9800988, 980923],
//     cdf: [1232342, 11033330, 12014253],
// };
// const ids = ['0x2342123', '0xabd3242'];
// const res = dp(weights, true, ids);
// console.log(res);

exports.dp = dp;
