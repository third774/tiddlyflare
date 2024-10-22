export interface RequestVars {
	tenantId: string;
}

export type WikiType = "tw5";

export interface ApiWiki {
	name: string;
	wikiId: string;
	wikiUrl: string;
	wikiType: string;

	tenantId: string;
}

export interface ApiListWikisResponse {
	data: {
		wikis: ApiWiki[];
	};
}
