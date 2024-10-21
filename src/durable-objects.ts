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
		const { redirectUrl } = await this.env.WIKI.get(doId).create(tenantId, wikiId, name, wikiType);

		this.sql.exec(`INSERT OR REPLACE INTO wikis VALUES (?, ?, ?, ?);`, wikiId, tenantId, name, wikiType);

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

		const chunks = chunkify(this.fileSrc);

		this.storage.transactionSync(() => {
			// I upsert here, to avoid failures, and allow retries.
			// Since each DO is identified uniquely for each wiki, it's safe to overwrite things.
			this.sql.exec(`INSERT OR REPLACE INTO wiki_info VALUES (?, ?, ?, ?);`, wikiId, tenantId, name, wikiType);
			for (let i = 0; i < chunks.length; i++) {
				this.sql.exec(`INSERT OR REPLACE INTO wiki_versions VALUES (?, ?, ?, ?, ?);`, wikiId, tsMs, chunks[i], i + 1, chunks.length);
			}
		});

		return {
			ok: true,
			redirectUrl: `${tenantId}/${wikiId}/${name}`,
		};
	}

	async upsert(_tenantId: string, wikiId: string, bytesStream: ReadableStream) {
		const tsMs = Date.now();

		const bytes = await new Response(bytesStream).bytes();
		const chunks = chunkify(bytes);

		try {
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
			});
		} catch (e) {
			console.error({
				message: 'failed to persist file upsert',
				error: e,
			});
			throw e;
		}

		this.fileSrc = bytes;

		return { ok: true };
	}

	async getFileSrc(wikiId: string, _tenantId: string): Promise<Response> {
		if (this.fileSrc) {
			return this._makeStreamResponse(this.fileSrc);
		}

		const row = this.sql.exec(`SELECT * FROM wiki_versions WHERE wikiId = ? ORDER BY tsMs DESC LIMIT 1;`, wikiId).one();
		const sz = Number(row.chunksTotal);
		const chunks = new Array<ArrayBuffer>(sz);
		for (let i = 0; i < sz; i++) {
			if (i === row.chunkIdx) {
				chunks[row.chunkIdx as number] = row.src as ArrayBuffer;
			} else {
				chunks[i] = this.sql
					.exec(`SELECT * FROM wiki_versions WHERE wikiId = ? AND tsMs = ? AND chunkIdx = ?;`, wikiId, row.tsMs, i + 1)
					.one().src as ArrayBuffer;
			}
		}

		this.fileSrc = mergeArrayBuffers(chunks);

		return this._makeStreamResponse(this.fileSrc);
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

	_makeStreamResponse(b: Uint8Array): Response {
		return new Response(
			new ReadableStream({
				start(controller) {
					controller.enqueue(b);
					controller.close();
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

export async function routeWikiRequest(env: CfEnv, tenantId: string, wikiId: string, name: string) {
	// Convert the hex ID back to the correct Durable Object ID.
	let id: DurableObjectId = env.WIKI.idFromString(wikiId);
	let stub = env.WIKI.get(id);
	return stub.getFileSrc(wikiId, tenantId);
}

// export async function routeListUrlRedirects(request: Request, env: CfEnv, tenantId: string): Promise<ApiListRedirectRulesResponse> {
// 	let id: DurableObjectId = env.TENANT.idFromName(tenantId);
// 	let tenantStub = env.TENANT.get(id);

// 	return tenantStub.list();
// }

export async function routeCreateWiki(env: CfEnv, tenantId: string, name: string, wikiType: WikiType) {
	let id: DurableObjectId = env.TENANT.idFromName(tenantId);
	let tenantStub = env.TENANT.get(id);

	return tenantStub.create(tenantId, name, wikiType);
}

export async function routeUpsertWiki(env: CfEnv, tenantId: string, wikiId: string, name: string, bytes: ReadableStream) {
	let id: DurableObjectId = env.WIKI.idFromString(wikiId);
	let wikiStub = env.WIKI.get(id);
	try {
		const { ok } = await wikiStub.upsert(tenantId, wikiId, bytes);
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
