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

	_migrations?: SchemaMigrations;

	constructor(ctx: DurableObjectState, env: CfEnv) {
		super(ctx, env);
		this.env = env;
		this.sql = ctx.storage.sql;

		ctx.blockConcurrencyWhile(async () => {
			this._migrations = new SchemaMigrations({
				doStorage: ctx.storage,
				migrations: TenantMigrations,
			});

			const tableExists = this.sql.exec("SELECT name FROM sqlite_master WHERE name = 'tenant_info';").toArray().length > 0;
			this.tenantId = tableExists ? String(this.sql.exec('SELECT tenantId FROM tenant_info LIMIT 1').one().tenantId) : '';
		});
	}

	async _initTables(tenantId: string) {
		const rowsData = await this._migrations!.runAll();
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

	async create(tenantId: string, name: string, wikiType: WikiType) {
		console.log('BOOM :: TENANT :: CREATE', tenantId, name, wikiType);
		await this._initTables(tenantId);

		const doId = this.env.WIKI.newUniqueId();

		// We use the DO ID stringified as our wiki ID to avoid the slowdown of `idFromName()`
		// that does a first round-trip to US to figure out the colo of the DO.
		// See https://developers.cloudflare.com/durable-objects/api/namespace/#newuniqueid
		const wikiId = doId.toString();

		this.sql.exec(`INSERT OR REPLACE INTO wikis VALUES (?, ?, ?, ?);`, wikiId, tenantId, name, wikiType);

		const { redirectUrl } = await this.env.WIKI.get(doId).create(tenantId, wikiId, name, wikiType);

		return { redirectUrl };
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

export class WikiDO extends DurableObject {
	env: CfEnv;
	storage: DurableObjectStorage;
	sql: SqlStorage;

	fileSrc: Uint8Array | null = null;

	_migrations?: SchemaMigrations;

	constructor(ctx: DurableObjectState, env: CfEnv) {
		super(ctx, env);
		this.env = env;
		this.storage = ctx.storage;
		this.sql = ctx.storage.sql;

		ctx.blockConcurrencyWhile(async () => {
			this._migrations = new SchemaMigrations({
				doStorage: ctx.storage,
				migrations: WikiMigrations,
			});

			await this._migrations.runAll();
		});
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

		const tsMs = Date.now();

		// I upsert here, to avoid failures, and allow retries.
		// Since each DO is identified uniquely for each wiki, it's safe to overwrite things.
		// Store the info first, so that even if the following operations fail, we can recreate
		// it on next visit.
		this.sql.exec(`INSERT OR REPLACE INTO wiki_info VALUES (?, ?, ?, ?);`, wikiId, tenantId, name, wikiType);

		const chunks = chunkify(this.fileSrc);

		this.storage.transactionSync(() => {
			for (let i = 0; i < chunks.length; i++) {
				this.sql.exec(`INSERT OR REPLACE INTO wiki_versions VALUES (?, ?, ?, ?, ?);`, wikiId, tsMs, chunks[i], i + 1, chunks.length);
			}
		});

		return {
			ok: true,
			redirectUrl: `/w/${wikiId}/${name}`,
		};
	}

	async upsert(wikiId: string, bytesStream: ReadableStream) {
		const tsMs = Date.now();

		try {
			// TODO Do chunking and storing in SQLite directly, to avoid buffering the whole payload!
			// https://github.com/lambrospetrou/tiddlyflare/issues/2
			const bytes = await new Response(bytesStream).bytes();
			const chunks = chunkify(bytes);

			this.storage.transactionSync(() => {
				for (let i = 0; i < chunks.length; i++) {
					const { rowsRead, rowsWritten } = this.sql.exec(
						`INSERT OR REPLACE INTO wiki_versions VALUES (?, ?, ?, ?, ?);`,
						wikiId,
						tsMs,
						chunks[i],
						i + 1,
						chunks.length
					);
					console.log({ message: 'WIKI: INSERT INTO wiki_versions', rowsWritten, rowsRead });
				}
			});

			this.fileSrc = bytes;

			// Retain only the latest 10 versions, otherwise we would hit the DO storage limit of 1GB fast.
			this.storage.transactionSync(() => {
				const tss = this.sql
					.exec('SELECT DISTINCT tsMs FROM wiki_versions WHERE wikiId = ? ORDER BY tsMs DESC LIMIT ?', wikiId, RETAINED_VERSIONS_NUM)
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
				console.log({ message: 'WIKI: DELETE FROM wiki_versions', rowsWritten, rowsRead });

				console.log({ message: 'WIKI: database size', databaseSizeBytes: this.sql.databaseSize });
			});
		} catch (e) {
			console.error({
				message: 'failed to persist file upsert',
				error: e,
			});
			throw e;
		}

		return { ok: true };
	}

	async getFileSrc(wikiId: string): Promise<Response> {
		if (this.fileSrc) {
			return this._makeStreamResponse(this.fileSrc);
		}

		// TODO Handle cases where somehow the file was not saved into SQLite.

		// Find which chunks to read.
		const chunksInfo = this.sql
			.exec<{
				tsMs: number;
				chunksLen: number;
			}>(
				`
				SELECT MAX(tsMs) as tsMs, COUNT(1) as chunksLen
				FROM wiki_versions WHERE wikiId = ?
				GROUP BY tsMs
				ORDER BY tsMs DESC
				LIMIT 1;`,
				wikiId
			)
			.one();
		console.log({ message: 'WIKI: chunks info', chunksInfo });

		// Get a cursor to the chunks, and we decide later if we will read all of them
		// at once, or stream them out gradually.
		const chunksCursor = this.sql.exec<{ src: ArrayBuffer }>(
			`SELECT src FROM wiki_versions WHERE wikiId = ? AND tsMs = ? ORDER BY tsMs ASC;`,
			wikiId,
			chunksInfo.tsMs
		);

		// Optimization: If we have less than 5 chunks (<10MB), then keep the whole file in-memory,
		// otherwise we always go back to the database to read the chunks.
		if (chunksInfo.chunksLen < 5) {
			this.fileSrc = mergeArrayBuffers(chunksCursor.toArray().map((c) => c.src));
			return this._makeStreamResponse(this.fileSrc);
		}
		// Fallback to streaming each chunk without storing it all in memory.
		return this._makeStreamResponse(chunksCursor);
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
					console.log({ message: 'WIKI: _makeStreamResponse', chunksLen, totalBytes });
				},
				cancel() {
					// This is called if the reader cancels,
					// so we should stop generating strings
					cancelled = true;
					console.log({ message: 'WIKI: _makeStreamResponse cancelled', chunksLen, totalBytes });
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
	let id: DurableObjectId = env.TENANT.idFromName(tenantId);
	let tenantStub = env.TENANT.get(id);

	return tenantStub.create(tenantId, name, wikiType);
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

export async function routeUpsertWiki(env: CfEnv, wikiId: string, _name: string, bytes: ReadableStream) {
	let id: DurableObjectId = env.WIKI.idFromString(wikiId);
	let wikiStub = env.WIKI.get(id);
	try {
		const { ok } = await wikiStub.upsert(wikiId, bytes);
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
