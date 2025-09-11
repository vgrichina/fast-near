const { readBlocks} = require('../source/redis-blocks');
const { SignedTransaction, BORSH_SCHEMA } = require('../data-model');
const { deserialize } = require('borsh');
const LRU = require('lru-cache');
const EventEmitter = require('events');
const debug = require('debug')('submit-transaction');

// TODO: Dedupe with json-rpc.js
const NODE_URL = process.env.FAST_NEAR_NODE_URL || 'https://rpc.mainnet.near.org';

async function sendJsonRpc(method, params) {
    const headers = {
        'Content-Type': 'application/json'
    };
    
    const authToken = process.env.FAST_NEAR_AUTH_TOKEN;
    if (authToken) {
        headers['Authorization'] = `Bearer ${authToken}`;
    }
    
    const res  = await fetch(`${NODE_URL}`, {
        method: 'POST',
        headers,
        body: JSON.stringify({
            jsonrpc: '2.0',
            id: 'fast-near',
            method,
            params
        })
    });
    const json = await res.json();
    if (json.error) {
        // TODO: Special RPCError?
        const error = new Error(`RPC Error: ${json.error.message}: ${json.error.data})`);
        error.jsonRpcError = json.error;
        throw error;
    }
    return json.result;
}

async function submitTransactionAsync(transactionData) {
    return await sendJsonRpc('broadcast_tx_async', [transactionData.toString('base64')]);
}

// TODO: Use our own DB instead
async function txStatus(txHash, accountId) {
    return await sendJsonRpc('tx', [txHash, accountId]);
}

let blockStream;
const txEventEmitter = new EventEmitter();
const txCache = new LRU({ max: 10000, maxAge: 1000 * 60 * 60 });

async function submitTransaction(transactionData) {
    if (!blockStream) {
        blockStream = true; // NOTE: Prevent reentrancy
        const headers = {
            'Content-Type': 'application/json'
        };
        
        const authToken = process.env.FAST_NEAR_AUTH_TOKEN;
        if (authToken) {
            headers['Authorization'] = `Bearer ${authToken}`;
        }
        
        const startBlockHeight = (await (await fetch(`${NODE_URL}/status`, {
            method: 'GET',
            headers
        })).json()).sync_info.latest_block_height;
        debug('startBlockHeight:', startBlockHeight);

        const redisUrl = process.env.BLOCKS_REDIS_URL;
        debug('redisUrl:', redisUrl);
        blockStream = readBlocks({ startBlockHeight, redisUrl, batchSize: 1 });
        await new Promise(async (resolve, reject) => {
            (async () => {
                let streamStarted = false;
                for await (const { block, shards } of blockStream) {
                    if (!streamStarted) {
                        streamStarted = true;
                        // NOTE: We are waiting for block stream to start and then it continues
                        resolve();
                    }

                    for (const { chunk } of shards) {
                        if (!chunk) {
                            continue;
                        }

                        for await (const { transaction, outcome } of chunk.transactions) {
                            const txHash = transaction.hash;
                            txCache.set(txHash, { transaction, outcome });
                        }
                    }
                    debug('Block:', block.header.height);
                    txEventEmitter.emit('block');
                }
            })().catch(reject);
        });
    }

    const txHash = await submitTransactionAsync(transactionData);
    debug('Transaction posted:', txHash);
    if (txCache.has(txHash)) {
        return txCache.get(txHash);
    }

    const SUBMIT_TX_STATUS_CHECK_TIMEOUT = 1000 * 10;
    const SUBMIT_TOTAL_TIMEOUT = 1000 * 45;
    const { transaction, outcome } = await new Promise((resolve, reject) => {
        let blockCallback;
        const subscribe = () => txEventEmitter.on('block', blockCallback);
        const unsubscribe = () => blockCallback && (txEventEmitter.off('block', blockCallback), blockCallback = null);
        setTimeout(() => (unsubscribe(), reject(new Error(`Taking more than ${SUBMIT_TOTAL_TIMEOUT}ms to submit transaction`))), SUBMIT_TOTAL_TIMEOUT);

        // NOTE: This is necessary in case transaction already landed before but we don't know
        setTimeout(async function checkStatus() {
            if (!blockCallback) return;

            const { transaction: { signerId } } = deserialize(BORSH_SCHEMA, SignedTransaction, transactionData);
            debug('Checking txStatus', txHash, signerId);
            try {
                const result = await txStatus(txHash, signerId);
                const { transaction, transaction_outcome } = result;
                debug('txStatus result:', result);

                if (blockCallback) {
                    unsubscribe();
                    resolve({ transaction, outcome: transaction_outcome });
                }
            } catch (error) {
                debug('txStatus error:', error);
                if (error.jsonRpcError) {
                    const { name, cause } = error.jsonRpcError;
                    if (name == 'HANDLER_ERROR' && cause.name ==  'UNKNOWN_TRANSACTION') {
                        setTimeout(checkStatus, SUBMIT_TX_STATUS_CHECK_TIMEOUT);
                        return;
                    }
                    unsubscribe();
                    reject(error);
                }
            }
        }, SUBMIT_TX_STATUS_CHECK_TIMEOUT);

        blockCallback = () => {
            if (txCache.has(txHash)) {
                unsubscribe();
                resolve(txCache.get(txHash));
            }
        };
        subscribe();
    });
    
    const result = {
        // TODO: Other fields
        transaction,
        transaction_outcome: outcome
    }
    console.log('result:', result);
    return result;
}

module.exports = { submitTransaction };