const fs = require('fs/promises');
const { readChangesFile, changeKey, changeValue } = require('./lake/changes-index');
const { readShardBlocks } = require('../source/lake');

class LakeStorage {

    dataDir = './lake-data/near-lake-data-mainnet';

    getLatestBlockHeight() {
        // TODO: Don't hardcode this
        return 110_999_999;
    }

    setLatestBlockHeight(blockHeight) {
        readOnly();
    }

    getBlockTimestamp(blockHeight) {
        // TODO: Real implementation
        return 0;
        // notImplemented();
    }

    setBlockTimestamp(blockHeight, timestamp) {
        readOnly();
    }

    async getLatestDataBlockHeight(compKey, blockHeight) {
        const { accountId, key } = parseCompKey(compKey);
        const shard = shardForAccount(accountId);

        let indexFile = `${this.dataDir}/${shard}/index/${accountId}.dat`;
        if (!await fileExists(indexFile)) {
            indexFile = `${this.dataDir}/${shard}/index/changes.dat`;
        }

        const changesStream = readChangesFile(indexFile, { accountId, keyPrefix: key, blockHeight });
        for await (const { key: k, changes } of changesStream) {
            if (k.equals(key)) {
                return changes.find(bh => bh <= blockHeight);
            }
        }
    }

    async getData(compKey, blockHeight) {
        const { accountId, key } = parseCompKey(compKey);
        const shard = shardForAccount(accountId);

        for await (const { data, blockHeight: currentHeight } of readShardBlocks({ dataDir: this.dataDir, shard, startBlockNumber: blockHeight, endBlockNumber: blockHeight + 1 })) {
            if (currentHeight !== blockHeight) {
                continue;
            }

            const { state_changes, chunk } = JSON.parse(data.toString('utf-8'));
            if (!chunk) {
                continue;
            }

            for (let { type, change } of state_changes) {
                const { account_id, ...changeData } = change;
                if (account_id !== accountId) {
                    continue;
                }

                const k = changeKey(type, changeData);
                if (k.equals(key)) {
                    return changeValue(type, changeData);
                }
            }
        }

        return null;
    }

    async getLatestData(compKey, blockHeight) {
        const dataBlockHeight = await this.getLatestDataBlockHeight(compKey, blockHeight);
        return dataBlockHeight && await this.getData(compKey, dataBlockHeight);
    }

    setData(batch, scope, accountId, storageKey, blockHeight, data) {
        readOnly();
    }

    deleteData(batch, scope, accountId, storageKey, blockHeight) {
        readOnly();
    }

    async getBlob(hash) {
        try {
            console.log('getBlob', hash.toString('hex'));
            return await fs.readFile(`${this.dataDir}/blob/${hash.toString('hex')}.wasm`);
        } catch (e) {
            if (e.code === 'ENOENT') {
                return null;
            }
            throw e;
        }
    }

    setBlob(batch, data) {
        readOnly();
    }

    cleanOlderData(batch, key, blockHeight) {
        readOnly();
    }

    scanAllKeys(iterator) {
        // TODO
        notImplemented();
    }

    scanDataKeys(accountId, blockHeight, keyPattern, iterator, limit) {
        // TODO
        notImplemented();
    }

    writeBatch(batch) {
        readOnly();
    }

    clearDatabase() {
        readOnly();
    }

    closeDatabase() {
        // Do nothing
        // TODO: If there is in memory cache, clear it?
    }
}

function readOnly() {
    throw new Error('LakeStorage is read only');
}

function notImplemented() {
    throw new Error('Not implemented');
}

function parseCompKey(compKey) {
    const type = compKey.subarray(0, 1);
    let offset = compKey.indexOf(':', 2);
    offset = offset === -1 ? compKey.length : offset;
    const accountId = compKey.toString('utf8', 2, offset);
    const key = Buffer.concat([type, compKey.slice(offset + 1)]);
    return { accountId, key, type };
}

function shardForAccount(accountId) {
    // TODO: Don't hardcode this
    // NOTE: This needs to match nearcore logic here: https://github.com/near/nearcore/blob/c6afdd71005a0f9b3e57244188ca02b97eeb0395/core/primitives/src/shard_layout.rs#L239
    const boundaryAccounts = ["aurora", "aurora-0", "kkuuue2akv_1630967379.near"];
    const index = boundaryAccounts.findIndex(boundaryAccount => accountId < boundaryAccount);
    return index < 0 ? boundaryAccounts.length : index;
}

async function fileExists(file) {
    try {
        await fs.access(file);
        return true;
    } catch (e) {
        return false;
    }
}

module.exports = { LakeStorage, parseCompKey };