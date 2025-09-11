const fetch = require('node-fetch');
const getRawBody = require('raw-body');

const { runContract }  = require('./run-contract');
const storage = require('./storage');
const { FastNEARError } = require('./error');

const { Account, BORSH_SCHEMA } = require('./data-model');

const { deserialize } = require('borsh');
const bs58 = require('bs58');
const resolveBlockHeight = require('./resolve-block-height');
const { viewAccessKey } = require('./utils/view-access-key');
const { accountKey } = require('./storage-keys');
const debug = require('debug')('json-rpc');

// NOTE: This is JSON-RPC proxy needed to pretend we are actual nearcore
const NODE_URL = process.env.FAST_NEAR_NODE_URL || 'https://rpc.mainnet.near.org';
const ARCHIVAL_NODE_URL = process.env.FAST_NEAR_ARCHIVAL_NODE_URL || 'https://rpc.mainnet.internal.near.org';

const FAST_NEAR_BLOCK_DATA_URL = process.env.FAST_NEAR_BLOCK_DATA_URL || 'https://mainnet.neardata.xyz/v0';
const FAST_NEAR_BLOCK_SOURCE = process.env.FAST_NEAR_BLOCK_SOURCE || 'neardata';

const { readBlocks } = require(`./source/${FAST_NEAR_BLOCK_SOURCE}`);

const proxyJson = async (ctx, { archival = false } = {}) => {
    const nodeUrl = archival ? ARCHIVAL_NODE_URL : NODE_URL;
    const rawBody = ctx.request.body ? JSON.stringify(ctx.request.body) : await getRawBody(ctx.req);
    debug('proxyJson', ctx.request.method, ctx.request.path, nodeUrl, rawBody.toString('utf8'));

    ctx.type = 'json';
    const headers = {
        'Content-Type': 'application/json'
    };
    
    const authToken = process.env.FAST_NEAR_AUTH_TOKEN;
    if (authToken) {
        headers['Authorization'] = `Bearer ${authToken}`;
    }
    
    const response = await fetch(`${nodeUrl}${ctx.request.path}`, {
        method: ctx.request.method,
        headers,
        body: ctx.request.method != 'GET' ? rawBody : undefined
    });

    const contentType = response.headers.get('content-type');
    debug('response', response.status, response.statusText, contentType);
    if (!response.ok) {
        ctx.throw(502, `Backend returned ${response.status} ${response.statusText}`);
    }
    if (!contentType || !contentType.includes('application/json')) {
        ctx.throw(500, 'Invalid content type from backend: expected application/json');
    }

    ctx.status = response.status;
    ctx.body = Buffer.from(await response.arrayBuffer());
}

const viewCallError = ({ id, message }) => {
    return {
        jsonrpc: '2.0',
        result: {
            error: message,
            // TODO: block_height and block_hash
        },
        id
    };
}

const legacyError = ({ id, message, name, cause }) => {
    return {
        jsonrpc: '2.0',
        error: {
            name,
            cause,
            code: -32000,
            data: message,
            message: "Server error",
        },
        id
    };
}

const ALWAYS_PROXY = ['yes', 'true'].includes((process.env.FAST_NEAR_ALWAYS_PROXY || 'no').trim().toLowerCase());

const handleError = async ({ ctx, blockHeight, error }) => {
    debug('handleError', error);
    const { body } = ctx.request;
    const { id }  = body;
    const accountId = error.data?.accountId;

    // TODO: Match error handling? Structured errors? https://docs.near.org/docs/api/rpc/contracts#what-could-go-wrong-6
    const message = error.toString();
    if (/TypeError.* is not a function/.test(message)) {
        ctx.body = viewCallError({
            id,
            message: "wasm execution failed with error: FunctionCallError(MethodResolveError(MethodNotFound))"
        });
        return;
    }

    if (error.jsonRpcError) {
        ctx.body = {
            jsonrpc: '2.0',
            error: error.jsonRpcError,
            id
        };
        return;
    }

    switch (error.code) {
    case 'notImplemented':
        await proxyJson(ctx);
        return;
    case 'prohibitedInView':
        ctx.body = viewCallError({
            id,
            message: `wasm execution failed with error: HostError(ProhibitedInView { method_name: "${error.data.methodName}" })`,
        });
        return;
    case 'panic':
    case 'abort':
        ctx.body = viewCallError({
            id,
            message: `wasm execution failed with error: FunctionCallError(HostError(GuestPanic { panic_msg: ${JSON.stringify(error.message)}}))`
        });
        return;
    case 'codeNotFound':
        ctx.body = viewCallError({
            id,
            message: `wasm execution failed with error: FunctionCallError(CompilationError(CodeDoesNotExist { account_id: AccountId("${accountId}") }))`
        });
        return;
    case 'keyNotFound':
        ctx.body = viewCallError({
            id,
            message: `access key ${error.data.public_key} does not exist while viewing`,
        });
        return;
    case 'blockNotFound':
        ctx.body = legacyError({
            id,
            name: 'HANDLER_ERROR',
            cause: { info: {}, name: 'UNKNOWN_BLOCK' },
            message: `DB Not Found Error: BLOCK HEIGHT: ${error.data.blockHeight} \n Cause: Unknown`
        });
        return;
    case 'chunkNotFound':
        ctx.body = legacyError({
            id,
            name: 'HANDLER_ERROR',
            cause: { info: { shard_id: error.data.shard_id }, name: 'INVALID_SHARD_ID' },
            message: `Shard id ${error.data.shard_id} does not exist`
        });
        return;
    case 'blockHeightTooLow':
        await proxyJson(ctx, { archival: true });
        return;
    case 'blockHeightTooHigh':
        // TODO: Structured error in addition to legacy?
        ctx.body = legacyError({
            id: body.id,
            message: `DB Not Found Error: BLOCK HEIGHT: ${blockHeight} \n Cause: Unknown`
        });
        return;
    case 'accountNotFound':
        // TODO: Structured error in addition to legacy?
        ctx.body = legacyError({
            id: body.id,
            message: `account ${accountId} does not exist while viewing`
        });
        return;
    }

    ctx.throw(400, message);
}

const LRU = require('lru-cache');
const { submitTransaction } = require('./utils/submit-transaction');
const cache = new LRU({
    // TODO: Adjust cache size and max age
    max: 1000,
    // 1 second
    maxAge: 1000
});

const rpcResult = (id, result) => ({
    jsonrpc: '2.0',
    result,
    id
});

const withJsonRpcCache = async (ctx, next) => {
    const { body } = ctx.request;
    const cacheKey = JSON.stringify({ method: body.method, params: body.params });
    debug('cacheKey', cacheKey);
    let resultPromise = cache.get(cacheKey);
    let cacheHit = !!resultPromise;
    if (cacheHit) {
        debug('cache hit', cacheKey);
    }

    if (!resultPromise) {
        resultPromise = (async () => {
            await next();
            return ctx.body;
        })();
    }

    if (!cacheHit) {
        cache.set(cacheKey, resultPromise);
    }

    let resultBody = await resultPromise;
    if (!cacheHit) {
        ctx.type = 'json';
        ctx.body = resultBody;
        cache.set(cacheKey, resultBody);
    } else {
        if (Buffer.isBuffer(resultBody)) {
            resultBody = JSON.parse(resultBody.toString('utf8'));
        }

        const { result, error } = resultBody;
        ctx.body = { jsonrpc: '2.0', result, error, id: ctx.request.body.id };
    }
}

const parseJsonBody = async (ctx, next) => {
    ctx.request.body = JSON.parse((await getRawBody(ctx.req)).toString('utf8'));
    await next();
}

const handleJsonRpc = async ctx => {
    if (ALWAYS_PROXY) {
        return await proxyJson(ctx);
    }

    const { body } = ctx.request;
    try {
        switch (body?.method) {
            case 'query': {
                const { finality, block_id } = body.params;
                // TODO: Determine proper way to handle finality. Depending on what indexer can do maybe just redirect to nearcore if not final

                if (typeof block_id == 'string') {
                    // TODO: Maintain block hash -> block height mapping
                    // Fall back to proxying
                    break;
                } else {
                    const blockHeight = await resolveBlockHeight(block_id);
                    debug('blockHeight', blockHeight);

                    ctx.body = rpcResult(body.id, await handleQuery({ blockHeight, body }));
                }
                return;
            }
            case 'chunk': {
                const { block_id, shard_id } = body.params;
                if (shard_id !== undefined && typeof block_id === 'number') {
                    debug('get chunk', block_id, shard_id);
                    const blocks = await readBlocks({
                        baseUrl: FAST_NEAR_BLOCK_DATA_URL,
                        startBlockHeight: block_id,
                        endBlockHeight: block_id + 1
                    });
                    for await (const block of blocks) {
                        const shard = block.shards?.find(({ shard_id: s }) => s == shard_id);
                        if (shard?.chunk) {
                            debug('chunk found', block_id, shard_id);
                            ctx.body = rpcResult(body.id, shard?.chunk);
                            return;
                        }
                    }

                    throw new FastNEARError('chunkNotFound', `Chunk not found: ${block_id} ${shard_id}`, { block_id, shard_id });
                }
                // Fall back to proxying
                break;
            }
            case 'broadcast_tx_commit': {
                const result = await submitTransaction(Buffer.from(body.params[0], 'base64'));
                ctx.body = rpcResult(body.id, result);
                return;
            }
        }
    } catch (error) {
        await handleError({ ctx, blockHeight: null, error });
        return;
    }

    // Fall back to proxying
    await proxyJson(ctx);
};

async function handleQuery({ blockHeight, body }) {
    debug('handleQuery', body.params);

    switch (body?.params?.request_type) {
        case 'call_function': {
            const { account_id, method_name: methodName, args_base64 } = body.params;
            return await callViewFunction({ blockHeight, accountId: account_id, methodName, args: Buffer.from(args_base64, 'base64') });
        }
        case 'view_account': {
            const { account_id } = body.params;
            return await viewAccount({ blockHeight, accountId: account_id });
        }
        case 'view_access_key': {
            const { account_id, public_key } = body.params;
            const accessKey = await viewAccessKey({ blockHeight, accountId: account_id, publicKey: public_key });
            if (!accessKey) {
                throw new FastNEARError('keyNotFound', `Access key not found: ${public_key} for ${account_id}`, { account_id, public_key });
            }
            return accessKey;
        }
        default: {
            // NOTE: Legacy way to query
            if (body?.params?.length) {
                const query = body.params[0];
                if (query?.startsWith('account/')) {
                    const [, accountId] = query.split('/');
                    return await viewAccount({ blockHeight, accountId });
                }

                if (query?.startsWith('call/')) {
                    const [, accountId, methodName] = query.split('/');
                    const args = bs58.decode(body.params[1], 'base64');
                    return await callViewFunction({ blockHeight, accountId, methodName, args });
                }
            }
            throw new FastNEARError('notImplemented', `Not implemented: ${body.params.request_type}`, { body });
        }
    }
}

const callViewFunction = async ({ blockHeight, accountId, methodName, args }) => {
    const { result, logs, blockHeight: resolvedBlockHeight } = await runContract(accountId, methodName, args, blockHeight);
    const resultBuffer = Buffer.from(result);
    return {
        result: Array.from(resultBuffer),
        logs,
        block_height: parseInt(resolvedBlockHeight)
        // TODO: block_hash
    };
}

const viewAccount = async ({ blockHeight, accountId }) => {
    debug('find account data', accountId);
    const compKey = accountKey(accountId);
    const accountData = await storage.getLatestData(compKey, blockHeight);
    debug('account data loaded', accountId);
    if (!accountData) {
        throw new FastNEARError('accountNotFound', `Account not found: ${accountId} at ${blockHeight} block height`, { accountId, blockHeight });
    }

    const { amount, locked, code_hash, storage_usage } = deserialize(BORSH_SCHEMA, Account, accountData);
    return {
        amount: amount.toString(),
        locked: locked.toString(),
        code_hash: bs58.encode(code_hash),
        storage_usage: parseInt(storage_usage.toString()),
        block_height: parseInt(blockHeight)
        // TODO: block_hash
    };
}

module.exports = { parseJsonBody, withJsonRpcCache, handleJsonRpc, proxyJson };