const sha256  = require('../utils/sha256');

const { parseCompKey } = require('./lake');

class ShardedStorage {
    constructor(shards, accountIdToShardId = hashShard(shards.length)) {
        this.shards = shards;
        this.accountIdToShardId = accountIdToShardId;
    }
    
    async getLatestBlockHeight() {
        return await this.shards[0].getLatestBlockHeight();
    }

    async setLatestBlockHeight(blockHeight) {
        await Promise.all(this.shards.map(shard => shard.setLatestBlockHeight(blockHeight)));
    }

    async getBlockTimestamp(blockHeight) {
        return await this.shards[0].getBlockTimestamp(blockHeight);
    }

    async setBlockTimestamp(blockHeight, timestamp) {
        await Promise.all(this.shards.map(shard => shard.setBlockTimestamp(blockHeight, timestamp)));
    }

    async getLatestDataBlockHeight(compKey, blockHeight) {
        const shardId = this.compKeyToShardId(compKey);
        return await this.shards[shardId].getLatestDataBlockHeight(compKey, blockHeight);
    }

    async getData(compKey, blockHeight) {
        const shardId = this.compKeyToShardId(compKey);
        return await this.shards[shardId].getData(compKey, blockHeight);
    }

    async getLatestData(compKey, blockHeight) {
        const shardId = this.compKeyToShardId(compKey);
        return await this.shards[shardId].getLatestData(compKey, blockHeight);
    }

    async setData(batch, scope, accountId, storageKey, blockHeight, data) {
        const shardId = this.accountIdToShardId(accountId);
        if (batch.shardId && batch.shardId != shardId) {
            return;
        }

        return await this.shards[shardId].setData(batch.batch, scope, accountId, storageKey, blockHeight, data);
    }

    async deleteData(batch, scope, accountId, storageKey, blockHeight) {
        const shardId = this.accountIdToShardId(accountId);
        if (batch.shardId && batch.shardId != shardId) {
            return;
        }

        return await this.shards[shardId].deleteData(batch, scope, accountId, storageKey, blockHeight);
    }

    async getBlob(hash) {
        const shardId = parseInt(hash.slice(0, 2), 16) % this.shards.length;
        return await this.shards[shardId].getBlob(hash);
    }

    async setBlob(batch, data) {
        // TODO: Refactor to avoid double hashing even in debug mode.
        const hash = sha256(data);

        const shardId = hash.readUInt32LE(0) % this.shards.length;
        if (batch.shardId && batch.shardId != shardId) {
            return;
        }

        return await this.shards[shardId].setBlob(hash, data);
    }

    async cleanOlderData(batch, key, blockHeight) {
        await Promise.all(this.shards.map(shard => shard.cleanOlderData(batch, key, blockHeight)));
    }

    async scanAllKeys(iterator) {
        // TODO: Iterate over all shards in sequence
        throw new Error('Not implemented');
    }
    
    async scanDataKeys(accountId, blockHeight, keyPattern, iterator, limit) {
        const shardId = this.accountIdToShardId(accountId);
        return await this.shards[shardId].scanDataKeys(accountId, blockHeight, keyPattern, iterator, limit);
    }

    async writeBatch(fn) {
        await Promise.all(this.shards.map((shard, shardId) => shard.writeBatch(batch => fn({ batch, shardId }))));
    }

    async clearDatabase() {
        await Promise.all(this.shards.map(shard => shard.clearDatabase()));
    }

    async closeDatabase() {
        await Promise.all(this.shards.map(shard => shard.closeDatabase()));
    }

    compKeyToShardId(compKey) {
        const { accountId } = parseCompKey(compKey);
        return this.accountIdToShardId(accountId);
    }
}

function hashShard(count) {
    return function (accountId) {
        const hash = sha256(Buffer.from(accountId));
        return hash.readUInt32LE(0) % count;
    };
}

module.exports = { ShardedStorage, hashShard };
    