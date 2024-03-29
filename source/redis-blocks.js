const { createClient } = require('redis');
const { promisify } = require('util');

const debug = require('debug')('source:redis-blocks');

const RETRY_TIMEOUT = 1000;
async function* readBlocks({ startBlockHeight, endBlockHeight, redisUrl, streamKey = 'final_blocks', batchSize, abortController }) {
    debug('startBlockHeight:', startBlockHeight, 'endBlockHeight:', endBlockHeight, 'redisUrl:', redisUrl, 'streamKey:', streamKey, 'batchSize:', batchSize);
    let redisClient = createClient(redisUrl, {
        detect_buffers: true,
        no_ready_check: true
    });

    redisClient = {
        end: redisClient.end.bind(redisClient),
        xread: promisify(redisClient.xread).bind(redisClient),
        xrange: promisify(redisClient.xrange).bind(redisClient),
    };

    if (!startBlockHeight) {
        throw new Error('startBlockHeight is required');
    }

    try {
        let blockHeight = startBlockHeight;
        do {
            if (abortController && abortController.signal.aborted) {
                break;
            }

            let result;
            try {
                result = await redisClient.xread('COUNT', batchSize, 'BLOCK', '100', 'STREAMS', streamKey, blockHeight);
            } catch (error) {
                console.error('Error reading from Redis', error);
                if (error.code === 'UNCERTAIN_STATE') {
                    console.log('Retrying after timeout');
                    await new Promise((resolve) => setTimeout(resolve, RETRY_TIMEOUT));
                    continue;
                }
                throw error;
            }
            if (!result) {
                continue;
            }

            const items = result[0][1];
            for (let [id, [, block]] of items) {
                yield JSON.parse(block);
                blockHeight = parseInt(id.split('-')[0]);

                if (endBlockHeight && blockHeight >= endBlockHeight) {
                    return;
                }
            }
        } while (!endBlockHeight || blockHeight < endBlockHeight);
    } finally {
        const flush = true;
        await redisClient.end(flush);
    }
}

module.exports = {
    readBlocks
};