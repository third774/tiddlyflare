import { HTTPException } from 'hono/http-exception';
import { CfEnv } from './durable-objects';
import { customAlphabet } from 'nanoid';

// No vowels to avoid "nice" words.
export const genId = customAlphabet('0123456789BCDFGHJKLMNPQRSTVWXZbcdfghjklmnpqrstvwxz', 32);

export function chunkify(bb: Uint8Array, chunkSz: number = 1_500_000): Uint8Array[] {
	// 2MB row size, so be comfortable at 1.5MB: https://developers.cloudflare.com/durable-objects/platform/limits/
	const chunks = new Array(Math.ceil(bb.length / chunkSz));
	let readidx = 0;
	for (let i = 0; i < chunks.length; i++) {
		const end = Math.min(readidx + chunkSz, bb.length);
		chunks[i] = bb.subarray(readidx, end);
		readidx = end;
	}
	return chunks;
}

export function mergeArrayBuffers(chunks: ArrayBuffer[]): Uint8Array {
	const arr = new Uint8Array(chunks.reduce((acc, b) => acc + b.byteLength, 0));
	let writeidx = 0;
	for (let i = 0; i < chunks.length; i++) {
		arr.set(new Uint8Array(chunks[i]), writeidx);
		writeidx += chunks[i].byteLength;
	}
	return arr;
}

export function apiKeyAuth(env: CfEnv, request: Request) {
	const authEnabled = env.VAR_API_AUTH_ENABLED;
	if (!authEnabled) {
		console.log('skipping auth like some monster!');
		return 'tiddlyflare-public';
	}

	// 1. Extra `Tiddlyflare-api-key` header.
	// 2. Extract tenantID and token from the header.
	// 3. Validate API KEY.
	// 4. Proceed or reject.
	const authKey = request.headers.get('Tiddlyflare-Api-Key')?.trim();
	if (!authKey) {
		throw new HTTPException(403, {
			message: 'Tiddlyflare-Api-Key header missing',
		});
	}

	// TODO Move this to Workers KV to allow multiple keys for multi-tenancy.
	const csvKeys = env.VAR_API_AUTH_ADMIN_KEYS_CSV?.split(',')
		.map((k) => k?.trim())
		.filter(Boolean);
	if (csvKeys.indexOf(authKey) < 0) {
		throw new HTTPException(403, {
			message: 'Tiddlyflare-Api-Key is invalid',
		});
	}

	// The key is `rf_key_<tenantID>_<token>`.
	const lastSepIdx = authKey.lastIndexOf('_');
	if (lastSepIdx < 0) {
		throw new HTTPException(403, {
			message: 'Tiddlyflare-Api-Key is malformed',
		});
	}
	const tenantId = authKey.slice('tf_key_'.length, lastSepIdx)?.trim();
	if (!tenantId) {
		throw new HTTPException(403, {
			message: 'Tiddlyflare-Api-Key is malformed',
		});
	}

	return tenantId;
}

export async function hash(s: string) {
	const utf8 = new TextEncoder().encode(s);
	const hashBuffer = await crypto.subtle.digest('SHA-256', utf8);
	const hashArray = Array.from(new Uint8Array(hashBuffer));
	const hashHex = hashArray.map((bytes) => bytes.toString(16).padStart(2, '0')).join('');
	return hashHex;
}

export async function hashToBigInt(s: string) {
	const hashHex = hash(s);
	return BigInt(`0x${(await hashHex).substring(0, 16)}`);
}
