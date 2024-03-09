const { open } = require('lmdb');
const bs58 = require('bs58');
const sha256 = require('../utils/sha256');
const { compositeKey, DATA_SCOPE } = require('../storage-keys');

const LMDB_PATH = process.env.FAST_NEAR_LMDB_PATH || './lmdb-data';

const MAX_STORAGE_KEY_SIZE = 1024;

const SCAN_COUNT = 1000;

const KEY_TYPE_STRING = 0;
const KEY_TYPE_BUFFER = 1;
const KEY_TYPE_CHANGE = 2;

class KeyEncoder {
    writeKey(key, targetBuffer, startPosition) {
        let offset = startPosition;
        if (key === null || key === undefined) {
            offset = startPosition;
        } else if (typeof key === 'string') {
            offset = targetBuffer.writeUInt8(KEY_TYPE_STRING, offset);
            offset += targetBuffer.write(key, offset);
        } else if (Buffer.isBuffer(key)) {
            offset = targetBuffer.writeUInt8(KEY_TYPE_BUFFER, offset);
            offset += key.copy(targetBuffer, offset);
        } else if (key.blockHeight !== undefined && key.compKey) {
            offset = targetBuffer.writeUInt8(KEY_TYPE_CHANGE, offset);
            offset += key.compKey.copy(targetBuffer, offset);
            offset = targetBuffer.writeUInt32BE(key.blockHeight, offset);
        } else {
            throw new Error(`Unsupported key type: ${typeof key}, key: ${JSON.stringify(key)}`);
        }

        return offset;
    }

    readKey(buffer, start, end) {
        if (start === end) {
            return null;
        }

        let offset = start;
        const type = buffer.readUInt8(offset);
        offset += 1;
        // NOTE: Buffer.from is used together with .subarray to make a copy of the buffer slice.
        // Otherwise it would be a view on the same buffer, which would be mutated by the next read.
        switch (type) {
            case KEY_TYPE_STRING:
                return buffer.toString('utf8', offset, end);
            case KEY_TYPE_BUFFER:
                return Buffer.from(buffer.subarray(offset, end));
            case KEY_TYPE_CHANGE:
                const compKey = Buffer.from(buffer.subarray(offset, end - 4));
                const blockHeight = buffer.readUInt32BE(end - 4);
                return { compKey, blockHeight };
            default:
                throw new Error('Unsupported key type: ' + type);
        }
    }
}

const keyEncoder = new KeyEncoder();

function truncatedKey(compKey) {
    if (compKey.length <= MAX_STORAGE_KEY_SIZE) {
        return compKey;
    }

    return Buffer.concat([compKey.subarray(0, MAX_STORAGE_KEY_SIZE), sha256(compKey)]);
}

class LMDBStorage {
    constructor({ path = LMDB_PATH }) {
        this.db = open({
            path,
            keyEncoder,
            noSync: true, // NOTE: YOLO, as all data is recoverable from the blockchain
            // compression: true, // TODO: Check if this is worth it
        });
    }

    async getLatestBlockHeight() {
        return this.db.get('latest_block_height');
    }

    async setLatestBlockHeight(blockHeight) {
        return this.db.put('latest_block_height', blockHeight);
    }

    async getBlockTimestamp(blockHeight) {
        return this.db.get(`t:${blockHeight}`);
    }

    async setBlockTimestamp(blockHeight, timestamp) {
        return this.db.put(`t:${blockHeight}`, timestamp);
    }

    async getLatestDataBlockHeight(compKey, blockHeight) {
        const key = truncatedKey(compKey);
        const [latest] = this.db.getKeys({
            start: { compKey: key, blockHeight },
            end: { compKey: key, blockHeight: 0 },
            limit: 1,
            reverse: true,
        }).asArray;

        if (latest) {
            return latest.blockHeight;
        }

        return null;
    }

    async getData(compKey, blockHeight) {
        const key = truncatedKey(compKey);
        const result = this.db.get({ compKey: key, blockHeight });
        return result && Buffer.from(result);
    }

    // TODO: Looks like this can resolve block height and get data at same read instead
    async getLatestData(compKey, blockHeight) {
        const dataBlockHeight = await this.getLatestDataBlockHeight(compKey, blockHeight);
        if (!dataBlockHeight) {
            return null;
        }

        return await this.getData(compKey, dataBlockHeight);
    }

    setData(batch, scope, accountId, storageKey, blockHeight, data) {
        const compKey = compositeKey(scope, accountId, storageKey);
        const key = truncatedKey(compKey);
        if (key.length > MAX_STORAGE_KEY_SIZE) {
            this.setBlob(batch, compKey);
        }
        this.db.put({ compKey: key, blockHeight }, data);
    }

    deleteData(batch, scope, accountId, storageKey, blockHeight) {
        const compKey = compositeKey(scope, accountId, storageKey);
        const key = truncatedKey(compKey);
        if (key.length > MAX_STORAGE_KEY_SIZE) {
            this.setBlob(batch, compKey);
        }
        // TODO: Garbage collect key blob for long keys?
        this.db.put({ compKey: key, blockHeight }, null);
    }

    getBlob(hash) {
        const bs58hash = bs58.encode(hash);
        const result = this.db.get(`b:${bs58hash}`);
        return result && Buffer.from(result);
    }

    setBlob(batch, data) {
        const hash = sha256(data);
        const bs58hash = bs58.encode(hash);
        this.db.put(`b:${bs58hash}`, data);
    }

    async cleanOlderData(batch, compKey, blockHeight) {
        const keysToRemove = await this.db.getKeys({
            start: { compKey, blockHeight: 0 },
            end: { compKey, blockHeight }
        }).asArray;
        for (const key of keysToRemove) {
            this.db.remove(key);
        }
    }

    async scanAllKeys(iterator) {
        iterator = iterator || '0';
        const limit = SCAN_COUNT;

        // TODO: Figure out how to start from first data key
        let start = null
        if (iterator != '0') {
            const buffer = Buffer.from(iterator, 'hex');
            start = keyEncoder.readKey(buffer, 0, buffer.length);
            // NOTE: Make sure we don't start from iterator key
            start.blockHeight += 1;
        }

        let data = await this.db.getKeys({
            start,
            end: { blockHeight: 0xffffffff, compKey: compositeKey(DATA_SCOPE, '\xff', '\xff') },
            limit
        }).asArray

        // TODO: Refactor with scanDataKeys
        if (data.length > 0) {
            // compute serialized key using writeKey
            const buffer = Buffer.alloc(2048); // 2048 is bigger than biggest default key size in lmdb
            const offset = keyEncoder.writeKey(data[data.length - 1].key, buffer, 0);
            const serializedKey = buffer.subarray(0, offset);
            iterator = serializedKey.toString('hex');
        }

        if (data.length < limit) {
            iterator = '0';
        }

        data = data.filter(key => !!key.blockHeight).map(key => key.compKey);

        // Deduplicate array of buffers in data
        // TODO: Less hacky way to do this?
        data = [...new Set(data.map(buffer => buffer.toString('hex')))].map(hex => Buffer.from(hex, 'hex'));

        return [
            iterator,
            data
        ];
    }

    async scanDataKeys(contractId, blockHeight, keyPattern, iterator, limit = SCAN_COUNT) {
        iterator = iterator || '0';
        // TODO: More robust pattern handling
        const keyPrefix = keyPattern.replace(/\*$/, '');

        let start;
        if (iterator === '0') {
            start = { blockHeight, compKey: compositeKey(DATA_SCOPE, contractId, keyPrefix) };
        } else {
            const buffer = Buffer.from(iterator, 'hex');
            start = keyEncoder.readKey(buffer, 0, buffer.length);
            // NOTE: Make sure we don't start from iterator key
            start.blockHeight += 1;
        }

        const data = await this.db.getRange({
            start,
            end: { blockHeight: 0xffffffff, compKey: compositeKey(DATA_SCOPE, contractId, keyPrefix + '\xff') },
            limit,
        }).asArray;

        if (data.length > 0) {
            // compute serialized key using writeKey
            const buffer = Buffer.alloc(2048); // 2048 is bigger than biggest default key size in lmdb
            const offset = keyEncoder.writeKey(data[data.length - 1].key, buffer, 0);
            const serializedKey = buffer.subarray(0, offset);
            iterator = serializedKey.toString('hex');
        }

        if (data.length < limit) {
            iterator = '0';
        }

        // Make sure to return only latest versions
        const latestData = [];
        for (const { key, value } of data) {
            if (latestData.length && key.compKey.equals(latestData[latestData.length - 1].key.compKey)) {
                latestData[latestData.length - 1] = { key, value };
            } else {
                latestData.push({ key, value });
            }
        }

        // TODO: Handle case when latest version is cut off by the limit

        return {
            iterator,
            data: latestData.map(({ key, value }) => {
                let { compKey } = key;
                if (compKey.length > MAX_STORAGE_KEY_SIZE) {
                    compKey = this.getBlob(compKey.subarray(compKey.length - 32, compKey.length));
                }
                const storageKey = compKey.slice(3 + contractId.length);
                return [
                    storageKey,
                    value,
                ];
            }),
        };
    }

    async writeBatch(fn) {
        // TODO: Is any real implementation needed?
        await this.db.transaction(() => {
            fn();
        });
    }

    async clearDatabase() {
        return this.db.drop();
    }

    async closeDatabase() {
        try {
            return await this.db.close();
        } catch (e) {
            if (/The environment is already closed/.test(e.message)) {
                return;
            }
            throw e;
        }
    }

    async printDatabase() {
        console.log('printDatabase');
        this.db.getRange().forEach(({ key, value }) => {
            if (key.compKey) {
                console.log(JSON.stringify(key.compKey.toString('utf8')), key.blockHeight, '->', value);
            } else {
                console.log(key, '->', value);
            }
        });
    }
}

module.exports = { LMDBStorage, KeyEncoder };