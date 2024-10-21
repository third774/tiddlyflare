import { DurableObject } from 'cloudflare:workers';
import { WikiType } from './types';
import { SchemaMigration, SchemaMigrations } from './sql-migrations';
import { chunkify, mergeArrayBuffers } from './shared';

export interface CfEnv {
	TENANT: DurableObjectNamespace<TenantDO>;
	WIKI: DurableObjectNamespace<WikiDO>;

	ASSETS: Fetcher;

	VAR_API_AUTH_ENABLED: boolean;

	// TODO Move auth keys to Workers KV for multitenancy.
	VAR_API_AUTH_ADMIN_KEYS_CSV: string;
}

/////////////////////////////////////////////////////////////////
// Durable Objects
///////////////////

const TenantMigrations: SchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: 'initial version',
		sql: `
            CREATE TABLE IF NOT EXISTS tenant_info(
				tenantId TEXT PRIMARY KEY,
				dataJson TEXT
			);
            CREATE TABLE IF NOT EXISTS wikis (
				wikiId TEXT PRIMARY KEY,
				tenantId TEXT,
				name TEXT,
				wikiType TEXT
			);
        `,
	},
];

export class TenantDO extends DurableObject {
	env: CfEnv;
	sql: SqlStorage;
	tenantId: string = '';

	_migrations: SchemaMigrations;

	constructor(ctx: DurableObjectState, env: CfEnv) {
		super(ctx, env);
		this.env = env;
		this.sql = ctx.storage.sql;

		this._migrations = new SchemaMigrations({
			doStorage: ctx.storage,
			migrations: TenantMigrations,
		});

		ctx.blockConcurrencyWhile(async () => {
			const tableExists = this.sql.exec("SELECT name FROM sqlite_master WHERE name = 'tenant_info';").toArray().length > 0;
			this.tenantId = tableExists ? String(this.sql.exec('SELECT tenantId FROM tenant_info LIMIT 1').one().tenantId) : '';
		});
	}

	async _initTables(tenantId: string) {
		const rowsData = await this._migrations.runAll();
		if (rowsData.rowsRead || rowsData.rowsWritten) {
			console.info({ message: `TENANT: completed schema migrations`, rowsRead: rowsData.rowsRead, rowsWritten: rowsData.rowsWritten });
		}
		if (this.tenantId) {
			if (this.tenantId !== tenantId) {
				throw new Error(`wrong tenant ID [${tenantId}] on the wrong Tenant [${this.tenantId}]`);
			}
		} else {
			this.sql.exec('INSERT INTO tenant_info VALUES (?, ?) ON CONFLICT DO NOTHING;', tenantId, '{}');
			this.tenantId = tenantId;
		}
		return this.tenantId;
	}

	async createWiki(tenantId: string, name: string, wikiType: WikiType) {
		console.log({ message: 'TENANT: createWiki', tenantId, name, wikiType });
		await this._initTables(tenantId);

		const doId = this.env.WIKI.newUniqueId();

		// We use the DO ID stringified as our wiki ID to avoid the slowdown of `idFromName()`
		// that does a first round-trip to US to figure out the colo of the DO.
		// See https://developers.cloudflare.com/durable-objects/api/namespace/#newuniqueid
		const wikiId = doId.toString();
		const { redirectUrl } = await this.env.WIKI.get(doId).create(tenantId, wikiId, name, wikiType);

		this.sql.exec(`INSERT OR REPLACE INTO wikis VALUES (?, ?, ?, ?);`, wikiId, tenantId, name, wikiType);

		return { ok: true, redirectUrl };
	}

	// async delete(tenantId: string, ruleUrl: string): Promise<ApiListRedirectRulesResponse> {
	// 	// console.log('BOOM :: TENANT :: DELETE', tenantId, ruleUrl);
	// 	await this._initTables(tenantId);

	// 	await this.makeWikiStub(tenantId, ruleUrl).deleteAll();

	// 	this.sql.exec(`DELETE FROM rules WHERE rule_url = ? AND tenant_id = ?;`, ruleUrl, tenantId);

	// 	return this.list();
	// }

	// async list(): Promise<ApiListRedirectRulesResponse> {
	// 	// console.log('BOOM :: TENANT :: LIST', this.tenantId);
	// 	if (!this.tenantId) {
	// 		return {
	// 			data: {
	// 				rules: [],
	// 				stats: [],
	// 			},
	// 		} as ApiListRedirectRulesResponse;
	// 	}

	// 	const data: ApiListRedirectRulesResponse['data'] = {
	// 		rules: this.sql
	// 			.exec('SELECT * FROM rules;')
	// 			.toArray()
	// 			.map((row) => ({
	// 				tenantId: String(row.tenant_id),
	// 				ruleUrl: String(row.rule_url),
	// 				responseStatus: Number(row.response_status),
	// 				responseLocation: String(row.response_location),
	// 				responseHeaders: JSON.parse(row.response_headers as string) as string[2][],
	// 			})),

	// 		stats: this.sql
	// 			.exec('SELECT * FROM url_visits_stats_agg')
	// 			.toArray()
	// 			.map((row) => ({
	// 				tenantId: String(row.tenant_id),
	// 				ruleUrl: String(row.rule_url),
	// 				tsHourMs: Number(row.ts_hour_ms),
	// 				totalVisits: Number(row.total_visits),
	// 			})),
	// 	};
	// 	return { data };
	// }
}

const WikiMigrations: SchemaMigration[] = [
	{
		idMonotonicInc: 1,
		description: 'initial version',
		sql: `
            CREATE TABLE IF NOT EXISTS wiki_info (
                wikiId TEXT PRIMARY KEY,
                tenantId TEXT,
                name TEXT,
                wikiType TEXT
            );

            CREATE TABLE IF NOT EXISTS wiki_versions (
				wikiId TEXT,
				tsMs INTEGER,
				src BLOB,
				chunkIdx INTEGER,
				chunksTotal INTEGER,

				PRIMARY KEY (wikiId, tsMs, chunkIdx)
			);
        `,
	},
];

const RETAINED_VERSIONS_NUM = 10;
// 1.5MB per chunk.
const CHUNK_SIZE_MAX = 1_500_000;
// anything under 5MB is handled without streaming.
const BYTE_SIZE_ATONCE_THRESHOLD = 5_000_000;

export class WikiDO extends DurableObject {
	env: CfEnv;
	storage: DurableObjectStorage;
	sql: SqlStorage;

	fileSrc: Uint8Array | null = null;

	_migrations: SchemaMigrations;

	wikiId: string = '';
	tenantId: string = '';

	constructor(ctx: DurableObjectState, env: CfEnv) {
		super(ctx, env);
		this.env = env;
		this.storage = ctx.storage;
		this.sql = ctx.storage.sql;

		this._migrations = new SchemaMigrations({
			doStorage: ctx.storage,
			migrations: WikiMigrations,
		});

		// The WikiDO is referenced straight from the eyeball worker visiting URLs
		// so in the constructor do not write anything to storage until we know it's a valid request.

		const tableExists = this.sql.exec("SELECT name FROM sqlite_master WHERE name = 'wiki_info';").toArray().length > 0;
		if (tableExists) {
			const { tenantId, wikiId } = this.sql
				.exec<{ tenantId: string; wikiId: string }>('SELECT tenantId, wikiId FROM wiki_info LIMIT 1')
				.one();
			this.tenantId = tenantId;
			this.wikiId = wikiId;
		}
	}

	async create(tenantId: string, wikiId: string, name: string, wikiType: WikiType) {
		// Fetch the content of the wiki based on the wikiType.
		switch (wikiType) {
			case 'tw5':
				// 2.43MB.
				this.fileSrc = await (await this.env.ASSETS.fetch('https://this-will-not-be-used/ui/static/tw/empty.html')).bytes();
				break;
			default:
				throw new Error('invalid wikiType specified: ' + wikiType);
		}
		const chunks = chunkify(this.fileSrc, CHUNK_SIZE_MAX);

		const tsMs = Date.now();

		await this._migrations.runAll();

		this.storage.transactionSync(() => {
			this.sql.exec(`INSERT OR REPLACE INTO wiki_info VALUES (?, ?, ?, ?);`, wikiId, tenantId, name, wikiType);

			for (let i = 0; i < chunks.length; i++) {
				this.sql.exec(`INSERT OR REPLACE INTO wiki_versions VALUES (?, ?, ?, ?, ?);`, wikiId, tsMs, chunks[i], i + 1, chunks.length);
			}
		});

		this.tenantId = tenantId;
		this.wikiId = wikiId;

		return {
			ok: true,
			redirectUrl: `/w/${wikiId}/${name}`,
		};
	}

	async upsert(wikiId: string, bytesStream: ReadableStream, contentLength?: number) {
		await this._migrations.runAll();

		const tsMs = Date.now();

		// Reset the in-memory cache to avoid inconsistencies if streaming is used.
		this.fileSrc = null;

		try {
			// If we had contentLength provided and is less than our threshold then
			// don't bother with streaming and do it all in memory.
			if (contentLength && contentLength < BYTE_SIZE_ATONCE_THRESHOLD) {
				const bytes = await new Response(bytesStream).bytes();
				const chunks = chunkify(bytes, CHUNK_SIZE_MAX);

				this.storage.transactionSync(() => {
					for (let i = 0; i < chunks.length; i++) {
						const chunkIdx = i+1;
						const { rowsRead, rowsWritten } = this.sql.exec(
							`INSERT OR REPLACE INTO wiki_versions VALUES (?, ?, ?, ?, ?);`,
							wikiId,
							tsMs,
							chunks[i],
							chunkIdx,
							chunks.length
						);
						console.log({ message: 'WIKI: INSERT INTO wiki_versions', wikiId, tsMs, chunkIdx, rowsWritten, rowsRead });
					}
				});

				this.fileSrc = bytes;
			} else {
				////////////////////
				// Streaming dance!

				let chunkIdx = 0;
				const reader = bytesStream.getReader();
				let bufferArraysTotal = 0;
				const bufferArrays = [];
				while (true) {
					// In tests with wrangler this returns 4KB each time. So we need to buffer it
					// to avoid writing a row per 4KB.
					//
					// TODO Switch to BYOB to avoid extra allocations for the returned buffer?
					// - https://developer.mozilla.org/en-US/docs/Web/API/ReadableStreamBYOBReader/read
					// - https://developers.cloudflare.com/workers/runtime-apis/streams/readablestreambyobreader/
					//
					const { value, done } = await reader.read();
					// console.log('Received', done, value?.byteLength);
					if (!done) {
						if (!(value instanceof Uint8Array)) {
							throw new Error('unexpected body chunk type:' + typeof value);
						}
						bufferArrays.push(value);
						bufferArraysTotal += value.byteLength;
						if (bufferArraysTotal < CHUNK_SIZE_MAX) {
							continue;
						}
					}

					// we will write to SQLite now so merge all the arrays we have.
					const chunk = mergeArrayBuffers(bufferArrays);
					bufferArrays.length = 0;
					bufferArraysTotal = 0;

					chunkIdx += 1;

					// Write the current chunk. It's OK if we fail without completing all the chunks.
					// We only consider a batch of chunks complete if the last chunk with `chunkIdx == chunksTotal` exists.
					const { rowsRead, rowsWritten } = this.sql.exec(
						`INSERT OR REPLACE INTO wiki_versions(wikiId, tsMs, src, chunkIdx) VALUES (?, ?, ?, ?);`,
						wikiId,
						tsMs,
						chunk,
						chunkIdx
						// We do not write the `chunksTotal` column since we don't know how many exist.
						// chunksTotal
					);
					console.log({
						message: 'WIKI: INSERT INTO wiki_versions',
						wikiId,
						tsMs,
						chunkIdx,
						chunkSz: chunk.byteLength,
						rowsWritten,
						rowsRead,
					});

					if (done) {
						break;
					}
				}

				// VERY IMPORTANT: Write a last row with empty `src` to signal the end of the streamed chunks.
				// Our reading flow will use the condition `chunkIdx === chunksTotal` to know that a certain
				// timestamp has all its chunks.
				// This is because we can fail at any point above while writing chunks, therefore
				// we do not want to serve incomplete files. They will be cleaned up eventually after N upserts.
				chunkIdx += 1;
				const { rowsRead, rowsWritten } = this.sql.exec(
					`INSERT OR REPLACE INTO wiki_versions(wikiId, tsMs, src, chunkIdx, chunksTotal) VALUES (?, ?, ?, ?, ?);`,
					wikiId,
					tsMs,
					new Uint8Array(),
					chunkIdx,
					chunkIdx
				);
				console.log({ message: 'WIKI: INSERT INTO wiki_versions', wikiId, tsMs, chunkIdx, rowsWritten, rowsRead });
			}

			// Retain only the latest 10 versions, otherwise we would hit the DO storage limit of 1GB fast.
			this.storage.transactionSync(() => {
				const tss = this.sql
					.exec(
						'SELECT DISTINCT tsMs FROM wiki_versions WHERE wikiId = ? AND chunkIdx = chunksTotal ORDER BY tsMs DESC LIMIT ?',
						wikiId,
						RETAINED_VERSIONS_NUM
					)
					.toArray()
					.map((row) => Number(row.tsMs));
				if (tss.length < RETAINED_VERSIONS_NUM) {
					console.log({ message: 'WIKI: skipping delete due to small number of versions' });
					return;
				}

				const { rowsRead, rowsWritten } = this.sql.exec(
					`DELETE FROM wiki_versions WHERE wikiId = ? AND tsMs < ?;`,
					wikiId,
					tss[tss.length - 1]
				);
				console.log({ message: 'WIKI: DELETE FROM wiki_versions', wikiId, tsMs, rowsWritten, rowsRead });
			});

			console.log({ message: 'WIKI: database size', wikiId, tsMs, databaseSizeBytes: this.sql.databaseSize });
		} catch (e) {
			console.error({
				message: 'WIKI: failed to persist file upsert',
				wikiId,
				tsMs,
				error: e,
			});
			throw e;
		}

		return { ok: true };
	}

	async getFileSrc(wikiId: string): Promise<Response> {
		if (!this.wikiId || this.wikiId != wikiId) {
			// We do not have a valid wikiId, so this must be a random request.
			return new Response('_|_', { status: 400 });
		}

		if (this.fileSrc) {
			return this._makeStreamResponse(wikiId, this.fileSrc);
		}

		await this._migrations.runAll();

		// FIXME Handle cases where somehow the file was not saved into SQLite.
		// 		For this, in case there isn't any chunk written, we read the wiki_info row
		//		and call the this.create() method, then recurse on the `getFileSrc()` again.

		// Find which chunks to read. We need complete files, so make sure to pick
		// the latest timestamp for which we have the last chunk!
		const chunksInfo = this.sql
			.exec<{
				tsMs: number;
				chunksTotal: number;
			}>(
				`
				SELECT tsMs, chunksTotal
				FROM wiki_versions
				WHERE wikiId = ? AND chunkIdx = chunksTotal
				ORDER BY tsMs DESC
				LIMIT 1;`,
				wikiId
			)
			.one();
		console.log({ message: 'WIKI: chunks info', wikiId, chunksInfo });

		// Get a cursor to the chunks, and we decide later if we will read all of them
		// at once, or stream them out gradually.
		const chunksCursor = this.sql.exec<{ src: ArrayBuffer }>(
			`SELECT src FROM wiki_versions WHERE wikiId = ? AND tsMs = ? ORDER BY chunkIdx ASC;`,
			wikiId,
			chunksInfo.tsMs
		);

		// Optimization: If we have less than N chunks, then keep the whole file in-memory,
		// otherwise we always go back to the database to read the chunks.
		if (chunksInfo.chunksTotal * CHUNK_SIZE_MAX < BYTE_SIZE_ATONCE_THRESHOLD) {
			this.fileSrc = mergeArrayBuffers(chunksCursor.toArray().map((c) => c.src));
			return this._makeStreamResponse(wikiId, this.fileSrc);
		}
		// Fallback to streaming each chunk without storing it all in memory.
		return this._makeStreamResponse(wikiId, chunksCursor);
	}

	async deleteAll() {
		this.fileSrc = null;

		await this.storage.deleteAll();

		// Reset the migrations to apply next run.
		this._migrations = new SchemaMigrations({
			doStorage: this.ctx.storage,
			migrations: WikiMigrations,
		});
	}

	async findTenantId() {
		return String(this.sql.exec('SELECT tenantId FROM wiki_info LIMIT 1;').one().tenantId);
	}

	_makeStreamResponse(
		wikiId: string,
		chunksCursor:
			| Uint8Array
			| SqlStorageCursor<{
					src: ArrayBuffer;
			  }>
	): Response {
		let cancelled = false;
		let chunksLen = 0;
		let totalBytes = 0;
		return new Response(
			new ReadableStream({
				start(controller) {
					if (chunksCursor instanceof Uint8Array) {
						controller.enqueue(chunksCursor);
						chunksLen = 1;
						totalBytes = chunksCursor.byteLength;
					} else {
						for (const chunk of chunksCursor) {
							if (cancelled) {
								break;
							}
							const arr = new Uint8Array(chunk.src);
							controller.enqueue(arr);
							chunksLen += 1;
							totalBytes += arr.byteLength;
						}
					}
					controller.close();
					console.log({ message: 'WIKI: _makeStreamResponse', wikiId, chunksLen, totalBytes });
				},
				cancel() {
					// This is called if the reader cancels,
					// so we should stop generating strings
					cancelled = true;
					console.log({ message: 'WIKI: _makeStreamResponse cancelled', wikiId, chunksLen, totalBytes });
				},
			}),
			{
				headers: {
					'Content-Type': 'text/html',
				},
			}
		);
	}
}

/////////////////////////////////////////////////////////////////
// Utils
/////////

/////////////////////////////////////////////////////////////////
// API Handlers
////////////////

export async function routeCreateWiki(env: CfEnv, tenantId: string, name: string, wikiType: WikiType) {
	try {
		let id: DurableObjectId = env.TENANT.idFromName(tenantId);
		let tenantStub = env.TENANT.get(id);
		const resp = await tenantStub.createWiki(tenantId, name, wikiType);
		return Response.json(resp, { status: 201 });
	} catch (e) {
		return Response.json({ ok: false }, { status: 500 });
	}
}

// export async function routeListUrlRedirects(request: Request, env: CfEnv, tenantId: string): Promise<ApiListRedirectRulesResponse> {
// 	let id: DurableObjectId = env.TENANT.idFromName(tenantId);
// 	let tenantStub = env.TENANT.get(id);

// 	return tenantStub.list();
// }

export async function routeWikiRequest(env: CfEnv, wikiId: string, _name: string) {
	// Convert the hex ID back to the correct Durable Object ID.
	let id: DurableObjectId = env.WIKI.idFromString(wikiId);
	let stub = env.WIKI.get(id);
	return stub.getFileSrc(wikiId);
}

export async function routeUpsertWiki(env: CfEnv, wikiId: string, bytes: ReadableStream, contentLength?: number) {
	let id: DurableObjectId = env.WIKI.idFromString(wikiId);
	let wikiStub = env.WIKI.get(id);
	try {
		const { ok } = await wikiStub.upsert(wikiId, bytes, contentLength);
		if (!ok) {
			throw new Error('could not save wiki');
		}
	} catch (e) {
		console.error('WIKI failed to upsert:', e);
		throw new Error('WIKI failed to save your updates');
	}
}

// export async function routeDeleteUrlRedirect(request: Request, env: CfEnv, tenantId: string): Promise<ApiListRedirectRulesResponse> {
// 	interface Params {
// 		ruleUrl: string;
// 	}
// 	const params = (await request.json()) as Params;

// 	let id: DurableObjectId = env.TENANT.idFromName(tenantId);
// 	let tenantStub = env.TENANT.get(id);

// 	return tenantStub.delete(tenantId, params.ruleUrl);
// }
