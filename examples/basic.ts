import { EventHub, StopSubscription } from "../src/index.ts";

type DocumentEvent = { documentID: string };
type UserEvent = { userID: string; action: "login" | "logout" };

const hub = new EventHub<{ document: DocumentEvent; user: UserEvent }>();

const fakeDatabase = {
	async get(documentID: string) {
		await new Promise((resolve) => setTimeout(resolve, 5));
		return { id: documentID, body: `contents of ${documentID}` };
	},
};

// 1. Callback style, as in your sketch. The request's AbortSignal does the cleanup.
const firstConnection = new AbortController();
hub.subscribe(
	"document",
	async (event) => {
		const newDocument = await fakeDatabase.get(event.documentID);
		console.log("[callback] morph", newDocument.body);
	},
	{ signal: firstConnection.signal },
);

// 2. Throwing StopSubscription leaves without an error log.
let eventsSeenBySecondSubscriber = 0;
hub.subscribe("document", () => {
	eventsSeenBySecondSubscriber += 1;
	if (eventsSeenBySecondSubscriber === 2) throw new StopSubscription();
	console.log("[stopper] got event", eventsSeenBySecondSubscriber);
});

// 3. `using` is TypeScript's `defer subscription.unregister()`.
{
	using scopedSubscription = hub.subscribe("user", (event) => console.log("[scoped]", event.action));
	hub.publish("user", { userID: "u1", action: "login" });
	await new Promise((resolve) => setTimeout(resolve, 1));
}
hub.publish("user", { userID: "u1", action: "logout" }); // not printed: scope ended

// 4. Channel-style loop, the shape an SSE handler usually wants.
const secondConnection = new AbortController();
const streamLoopFinished = (async () => {
	for await (const event of hub.stream("document", { signal: secondConnection.signal })) {
		const newDocument = await fakeDatabase.get(event.documentID);
		console.log("[stream] morph", newDocument.body);
	}
	console.log("[stream] connection closed, loop ended");
})();

hub.publish("document", { documentID: "doc-1" });
hub.publish("document", { documentID: "doc-2" });
await new Promise((resolve) => setTimeout(resolve, 50));

firstConnection.abort();
secondConnection.abort();
await streamLoopFinished;

hub.publish("document", { documentID: "doc-3" }); // nobody left listening
await new Promise((resolve) => setTimeout(resolve, 20));
console.log("done");
