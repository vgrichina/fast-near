// TODO: Allow to choose storage type via env variable

const { DebugStorage } = require('./debug-wrapper');
const { RedisStorage } = require('./redis');
const { LMDBStorage } = require('./lmdb-embedded');
const { LakeStorage } = require('./lake');
const { CachedStorage } = require('./cached');
const { ShardedStorage } = require('./sharded');

const storageType = process.env.FAST_NEAR_STORAGE_TYPE || 'redis';

const ENABLE_CACHE = ['no', 'false', '0'].indexOf((process.env.FAST_NEAR_ENABLE_CACHE || 'true').trim().toLowerCase()) === -1;

const debugStorage = new DebugStorage(createStorage(storageType));

function createStorage(storageType) {
    switch (storageType) {
        case 'lmdb':
            // return new LMDBStorage({ path: `./lmdb-data` });
            return new ShardedStorage([...Array(1)].map((_, i) => new LMDBStorage({ path: `./lmdb-data/${i}` })));
        case 'redis':
            return new RedisStorage();
        case 'lake':
            return new LakeStorage();
        default:
            throw new Error('Unknown storage type: ' + storageType);
    }
}

module.exports = 
    ENABLE_CACHE
        ? new CachedStorage(debugStorage)
        : debugStorage;