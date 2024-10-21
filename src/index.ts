import { Hono } from 'hono/tiny';
import { HTTPException } from 'hono/http-exception';
import { requestId } from 'hono/request-id';
import { logger } from 'hono/logger';
import { cors } from 'hono/cors';

import { CfEnv, routeWikiRequest, routeCreateWiki, routeUpsertWiki } from './durable-objects';
export { TenantDO, WikiDO } from './durable-objects';

import { uiAbout, uiAdmin } from './ui';
import { RequestVars } from './types';
import { apiKeyAuth } from './shared';

const app = new Hono<{ Bindings: CfEnv; Variables: RequestVars }>();
export default app;

app.onError((e, c) => {
	if (e instanceof HTTPException) {
		// Get the custom response
		return e.getResponse();
	}
	console.error('failed to handle the request: ', e);
	return new Response('failed to handle the request: ' + e.message, {
		status: 500,
		statusText: e.name,
	});
});

app.use('*', requestId());
app.use(logger());

// CORS swallows the OPTIONS requests for the PUT Saver, so restrict it to the API.
app.use(
	'/-_-/v1/*',
	cors({
		origin: '*',
		allowHeaders: ['Allow', 'If-Match'],
		allowMethods: ['HEAD', 'PUT', 'POST', 'GET', 'OPTIONS'],
		exposeHeaders: ['dav', 'X-Powered-By'],
		maxAge: 900,
		credentials: true,
	})
);
app.use('/-_-/v1/*', async (c, next) => {
	const tenantId = apiKeyAuth(c.env, c.req.raw);
	c.set('tenantId', tenantId);
	return next();
});

// app.get('/-_-/v1/redirects.List', async (c) => {
// 	const respData = await routeListUrlRedirects(c.req.raw, c.env, c.var.tenantId);
// 	return Response.json(respData);
// });

app.post('/-_-/v1/wikis.Create', async (c) => {
	interface Params {
		name: string;
		wikiType: string;
	}

	const params = (await c.req.raw.json()) as Params;
	if (params.wikiType != 'tw5') {
		return Response.json(
			{ error: new Error('invalid wikiType') },
			{
				status: 400,
			}
		);
	}
	if (!params.name?.trim()) {
		return Response.json(
			{ error: new Error('invalid name') },
			{
				status: 400,
			}
		);
	}

	const respData = await routeCreateWiki(c.env, c.var.tenantId, params.name, params.wikiType);
	c.res.headers.set('X-Powered-By', 'Tiddlyflare');
	return c.json(respData, 201);
});

// app.post('/-_-/v1/redirects.Delete', async (c) => {
// 	const respData = await routeDeleteUrlRedirect(c.req.raw, c.env, c.var.tenantId);
// 	return Response.json(respData);
// });

app.route('/', uiAdmin);
app.route('/', uiAbout);

app.get("/w/:wikiId/favicon.ico", async (c) => {
	// Without this route, we fallback to the route below that always returns the full
	// wiki which is crazy expensive for a favicon :)
	// https://css-tricks.com/emoji-as-a-favicon/
	c.res.headers.set("Content-Type", "image/svg+xml");
	return c.body(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><text y=".9em" font-size="90">✏️</text></svg>`);
});

app.get('/w/:wikiId/:name', async (c) => {
	const { wikiId, name } = c.req.param();
	console.log('GET ::', wikiId, name, c.req.method);

	c.res.headers.set('X-Powered-By', 'Tiddlyflare');

	// Hono handles HEAD methods as part of GET!

	// Returning the stream causes issues with the runtime since nobody consumes it.
	// Error: Uncaught (in promise) Error: Network connection lost.
	// TODO Return ETag if we want.
	if (c.req.method.toUpperCase() === 'HEAD') {
		return c.text('');
	}
	return routeWikiRequest(c.env, wikiId, name);
});

app.put('/w/:wikiId/:name', async (c) => {
	const { wikiId, name } = c.req.param();
	console.log('PUT ::', wikiId, name);

	try {
		// const bytes = await c.req.raw.bytes();
		await routeUpsertWiki(
			c.env,
			wikiId,
			name,
			// Pass the body stream directly!
			c.req.raw.body!,
		);

		// TODO Add ETag.
		c.res.headers.set('X-Powered-By', 'Tiddlyflare');
		return c.text('ok');
	} catch (e) {
		console.error({
			message: 'PUT.Saver::failed to persist file save',
			error: e,
		});
		return c.text('failed to save due to internal DO error', { status: 500 });
	}
});

app.options('/w/:wikiId/:name', async (c) => {
	// const { wikiId, name } = c.req.param();
	// console.log("OPTIONS ::", wikiId, name);
	// Satisfy the PUT Saver: https://github.com/TiddlyWiki/TiddlyWiki5/blob/646f5ae7cf2a46ccd298685af3228cfd14760e25/core/modules/savers/put.js#L58
	return c.text('ok', {
		headers: {
			'X-Powered-By': 'Tiddlyflare',
			Allow: 'GET,HEAD,POST,OPTIONS,CONNECT,PUT,DAV,dav',
			dav: 'tw5/put',
		},
	});
});

app.all('/*', async (c) => {
	return c.text('_|_', {
		headers: {
			'X-Powered-By': 'Tiddlyflare',
		},
	});
});
