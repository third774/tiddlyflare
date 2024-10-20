import { DurableObject } from 'cloudflare:workers';

import { Hono } from 'hono/tiny';
import { HTTPException } from 'hono/http-exception';
import { requestId } from 'hono/request-id';
import { logger } from 'hono/logger';
import { bodyLimit } from 'hono/body-limit';
import { cors } from 'hono/cors';

import { CfEnv, routeWikiRequest, routeCreateWiki } from './durable-objects';
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
app.use(async function poweredBy(c, next) {
	await next();
	c.res.headers.set('X-Powered-By', 'Tiddlyflare');
});
app.use(logger());
app.use(
	cors({
		origin: '*',
		// allowHeaders: ['Upgrade-Insecure-Requests'],
		allowMethods: ['POST', 'GET', 'OPTIONS'],
		// maxAge: 600,
		credentials: true,
	})
);
app.use(
	bodyLimit({
		maxSize: 10 * 1024, // 10kb
		onError: (c) => {
			return c.text('overflow :(', 413);
		},
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
		name: string,
		wikiType: string;
	}

	const params = (await c.req.raw.json()) as Params;
	if (params.wikiType != "tw5") {
		return Response.json({error: new Error("invalid wikiType")}, {
			status: 400
		})
	}
	if (!params.name?.trim()) {
		return Response.json({error: new Error("invalid name")}, {
			status: 400
		})
	}

	const respData = await routeCreateWiki(c.env, c.var.tenantId, params.name, params.wikiType);
	return Response.json(respData);
});

// app.post('/-_-/v1/redirects.Delete', async (c) => {
// 	const respData = await routeDeleteUrlRedirect(c.req.raw, c.env, c.var.tenantId);
// 	return Response.json(respData);
// });

app.route('/', uiAdmin);
app.route('/', uiAbout);

app.get('/:tenantId/:wikiId/:name', async (c) => {
	return routeWikiRequest(c.env, c.req.param("tenantId"), c.req.param("wikiId"), c.req.param("name"));
});
