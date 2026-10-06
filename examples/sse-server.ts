// A document page that live-updates over SSE, using only web-standard Request/Response.
//
//   deno serve --port 8000 examples/sse-server.ts
//   bun --port 8000 examples/sse-server.ts
//
//   curl -N localhost:8000/documents/42/updates      (watch one document)
//   curl -N localhost:8000/documents/updates         (watch the list)
//   curl -X POST localhost:8000/documents/42 -d 'new body'
//
// The event format is Datastar's; any SSE client can read it.
import { EventHub } from "../src/index.ts";

type DocumentChanged = { documentID: number };

const hub = new EventHub<{ document: DocumentChanged }>();
const documentBodiesByID = new Map<number, string>([[42, "first draft"]]);

const escapeHtml = (text: string) =>
	text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll('"', "&quot;");

const renderDocument = (documentID: number) =>
	`<article id="document">${escapeHtml(documentBodiesByID.get(documentID) ?? "(deleted)")}</article>`;

const renderDocumentList = () =>
	`<ul id="documents">${[...documentBodiesByID.keys()].map((documentID) => `<li>${documentID}</li>`).join("")}</ul>`;

const patchElementsEvent = (html: string) => `event: datastar-patch-elements\ndata: elements ${html}\n\n`;

// One SSE response per watcher. `request.signal` aborts when the client disconnects, which ends the
// `for await` loop and removes the subscription; nothing else needs cleaning up.
function streamUpdates(request: Request, render: () => string, key?: number): Response {
	const encoder = new TextEncoder();
	const body = new ReadableStream<Uint8Array>({
		async start(controller) {
			controller.enqueue(encoder.encode(patchElementsEvent(render())));
			try {
				for await (const _change of hub.stream("document", { key, signal: request.signal })) {
					controller.enqueue(encoder.encode(patchElementsEvent(render())));
				}
			} finally {
				try {
					controller.close();
				} catch {
					// Already closed by the runtime after the client left.
				}
			}
		},
	});
	return new Response(body, {
		headers: { "content-type": "text/event-stream", "cache-control": "no-cache" },
	});
}

export default {
	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);

		if (request.method === "GET" && url.pathname === "/documents/updates") {
			return streamUpdates(request, renderDocumentList);
		}

		const updatesMatch = url.pathname.match(/^\/documents\/(\d+)\/updates$/);
		if (request.method === "GET" && updatesMatch) {
			const documentID = Number(updatesMatch[1]);
			return streamUpdates(request, () => renderDocument(documentID), documentID);
		}

		const documentMatch = url.pathname.match(/^\/documents\/(\d+)$/);
		if (request.method === "POST" && documentMatch) {
			const documentID = Number(documentMatch[1]);
			documentBodiesByID.set(documentID, await request.text());
			// In the real app this sits right after the DB commit (or in a Postgres LISTEN handler).
			hub.publish("document", { documentID }, { key: documentID });
			return new Response(null, { status: 204 });
		}

		if (url.pathname === "/stats") return Response.json(hub.stats());

		return new Response("not found", { status: 404 });
	},
};
