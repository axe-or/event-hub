// Runs under `node --test`, `deno test` and plain `bun` (all three support node:test).
// The GC tests need --expose-gc (Deno: --v8-flags=--expose-gc) and are skipped without it.
import { test } from "node:test";
import assert from "node:assert/strict";
import { EventHub, StopSubscription } from "../src/index.ts";

type DocumentEvent = { documentID: number };
type Topics = { document: DocumentEvent; user: { userID: string } };

const emptyStats = { topicCount: 0, keyCount: 0, subscriberCount: 0 };

// Lets queued microtasks (handler drains, generator resumptions) and one macrotask turn complete.
const settle = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
const silent = () => {};

// ---------------------------------------------------------------- delivery semantics

test("keyed subscribers get their key, topic-wide subscribers get everything, unkeyed publish broadcasts", async () => {
	const hub = new EventHub<Topics>();
	const viewerOf7: number[] = [];
	const viewerOf8: number[] = [];
	const listView: number[] = [];
	hub.subscribe("document", (event) => void viewerOf7.push(event.documentID), { key: 7 });
	hub.subscribe("document", (event) => void viewerOf8.push(event.documentID), { key: 8 });
	hub.subscribe("document", (event) => void listView.push(event.documentID));

	hub.publish("document", { documentID: 7 }, { key: 7 });
	hub.publish("document", { documentID: 9 }, { key: 9 });
	hub.publish("document", { documentID: 0 });
	await settle();

	assert.deepEqual(viewerOf7, [7, 0]);
	assert.deepEqual(viewerOf8, [0]);
	assert.deepEqual(listView, [7, 9, 0]);
});

test("a full mailbox drops the oldest events, keeping the latest", async () => {
	const hub = new EventHub<Topics>();
	const keepThree: number[] = [];
	const keepOne: number[] = [];
	hub.subscribe("document", (event) => void keepThree.push(event.documentID), { maxQueuedEvents: 3 });
	hub.subscribe("document", (event) => void keepOne.push(event.documentID), { maxQueuedEvents: 1 });

	for (let documentID = 1; documentID <= 10; documentID++) hub.publish("document", { documentID });
	await settle();

	assert.deepEqual(keepThree, [8, 9, 10]);
	assert.deepEqual(keepOne, [10]);
});

test("async handlers process one event at a time, in order", async () => {
	const hub = new EventHub<Topics>();
	const trace: string[] = [];
	let handlersRunning = 0;
	hub.subscribe("document", async (event) => {
		handlersRunning++;
		assert.equal(handlersRunning, 1, "handler overlapped with itself");
		trace.push(`start ${event.documentID}`);
		await new Promise((resolve) => setTimeout(resolve, 2));
		trace.push(`end ${event.documentID}`);
		handlersRunning--;
	});

	hub.publish("document", { documentID: 1 });
	hub.publish("document", { documentID: 2 });
	await new Promise((resolve) => setTimeout(resolve, 20));

	assert.deepEqual(trace, ["start 1", "end 1", "start 2", "end 2"]);
});

test("publish never runs handler code synchronously", () => {
	const hub = new EventHub<Topics>();
	let handlerRan = false;
	hub.subscribe("document", () => void (handlerRan = true));
	hub.publish("document", { documentID: 1 });
	assert.equal(handlerRan, false);
});

// ---------------------------------------------------------------- robustness across runtimes and package copies

test("a non-native thenable returned by a handler is awaited before the next event", async () => {
	const hub = new EventHub<Topics>();
	const trace: string[] = [];
	hub.subscribe("document", (event) => {
		trace.push(`start ${event.documentID}`);
		// Shaped like a promise from another realm or a promise library: has .then, isn't a Promise.
		const foreignThenable = {
			then(onFulfilled: () => void) {
				setTimeout(() => {
					trace.push(`end ${event.documentID}`);
					onFulfilled();
				}, 2);
			},
		};
		return foreignThenable as unknown as PromiseLike<void>;
	});
	hub.publish("document", { documentID: 1 });
	hub.publish("document", { documentID: 2 });
	await new Promise((resolve) => setTimeout(resolve, 20));
	assert.deepEqual(trace, ["start 1", "end 1", "start 2", "end 2"]);
});

test("a StopSubscription from a duplicate copy of the package is still recognized", async () => {
	const hub = new EventHub<Topics>();
	const reportedErrors: unknown[] = [];
	// What another installed copy's StopSubscription looks like to this copy: not our class, same brand.
	const makeForeignStopSubscription = () =>
		Object.assign(new Error("from another copy"), { [Symbol.for("eventhub.StopSubscription")]: true });
	hub.subscribe(
		"document",
		() => {
			throw makeForeignStopSubscription();
		},
		{ onError: (error) => reportedErrors.push(error) },
	);
	hub.publish("document", { documentID: 1 });
	await settle();
	assert.deepEqual(hub.stats(), emptyStats);
	assert.equal(reportedErrors.length, 0);
});

test("an onError that throws does not wedge the hub", async () => {
	const hub = new EventHub<Topics>();
	hub.subscribe(
		"document",
		() => {
			throw new Error("handler failed");
		},
		{
			onError: () => {
				throw new Error("error reporter failed too");
			},
		},
	);
	const healthyViewer: number[] = [];
	hub.subscribe("document", (event) => void healthyViewer.push(event.documentID));

	hub.publish("document", { documentID: 1 });
	hub.publish("document", { documentID: 2 });
	await settle();
	assert.equal(hub.stats().subscriberCount, 1);
	assert.deepEqual(healthyViewer, [1, 2]);
});

test("Subscription supports `using` where the runtime has Symbol.dispose, and nothing odd otherwise", () => {
	const hub = new EventHub<Topics>();
	const subscription = hub.subscribe("document", silent);
	assert.equal("undefined" in subscription, false);
	assert.equal(typeof subscription[Symbol.dispose], "function");
	subscription[Symbol.dispose]();
	assert.equal(subscription.closed, true);
});

// ---------------------------------------------------------------- every exit path leaves the hub empty

test("unregister() removes the subscriber, its key and its topic; calling it twice is harmless", () => {
	const hub = new EventHub<Topics>();
	const keyed = hub.subscribe("document", silent, { key: 1 });
	const topicWide = hub.subscribe("document", silent);
	assert.deepEqual(hub.stats(), { topicCount: 1, keyCount: 1, subscriberCount: 2 });

	keyed.unregister();
	assert.deepEqual(hub.stats(), { topicCount: 1, keyCount: 0, subscriberCount: 1 });
	keyed.unregister();
	topicWide.unregister();
	assert.deepEqual(hub.stats(), emptyStats);
	assert.equal(keyed.closed, true);
});

test("a key entry survives until its last subscriber leaves", () => {
	const hub = new EventHub<Topics>();
	const firstViewer = hub.subscribe("document", silent, { key: 5 });
	const secondViewer = hub.subscribe("document", silent, { key: 5 });

	firstViewer.unregister();
	assert.deepEqual(hub.stats(), { topicCount: 1, keyCount: 1, subscriberCount: 1 });
	secondViewer.unregister();
	assert.deepEqual(hub.stats(), emptyStats);
});

test("aborting the signal unregisters, including a signal that was already aborted", () => {
	const hub = new EventHub<Topics>();
	const connection = new AbortController();
	const subscription = hub.subscribe("document", silent, { key: 1, signal: connection.signal });
	connection.abort();
	assert.equal(subscription.closed, true);
	assert.deepEqual(hub.stats(), emptyStats);

	const lateSubscription = hub.subscribe("document", silent, { signal: AbortSignal.abort() });
	assert.equal(lateSubscription.closed, true);
	assert.deepEqual(hub.stats(), emptyStats);
});

test("throwing StopSubscription unregisters without reporting an error", async () => {
	const hub = new EventHub<Topics>();
	const reportedErrors: unknown[] = [];
	hub.subscribe(
		"document",
		() => {
			throw new StopSubscription();
		},
		{ key: 1, onError: (error) => reportedErrors.push(error) },
	);
	hub.publish("document", { documentID: 1 }, { key: 1 });
	await settle();
	assert.deepEqual(hub.stats(), emptyStats);
	assert.equal(reportedErrors.length, 0);
});

test("any other throw, sync or async, unregisters and reports the error", async () => {
	const hub = new EventHub<Topics>();
	const reportedErrors: unknown[] = [];
	const onError = (error: unknown) => reportedErrors.push(error);
	hub.subscribe(
		"document",
		() => {
			throw new Error("sync boom");
		},
		{ onError },
	);
	hub.subscribe(
		"document",
		async () => {
			throw new Error("async boom");
		},
		{ onError },
	);

	hub.publish("document", { documentID: 1 });
	await settle();
	assert.deepEqual(hub.stats(), emptyStats);
	assert.deepEqual(
		reportedErrors.map((error) => (error as Error).message).sort(),
		["async boom", "sync boom"],
	);
});

test("events still queued when a handler leaves are discarded, not delivered", async () => {
	const hub = new EventHub<Topics>();
	const received: number[] = [];
	hub.subscribe("document", (event) => {
		received.push(event.documentID);
		if (event.documentID === 2) throw new StopSubscription();
	});
	for (let documentID = 1; documentID <= 5; documentID++) hub.publish("document", { documentID });
	await settle();
	assert.deepEqual(received, [1, 2]);
});

test("`using` unregisters at the end of the scope", () => {
	const hub = new EventHub<Topics>();
	{
		using _subscription = hub.subscribe("document", silent, { key: 1 });
		assert.equal(hub.stats().subscriberCount, 1);
	}
	assert.deepEqual(hub.stats(), emptyStats);
});

test("unregistering from inside the handler stops later events", async () => {
	const hub = new EventHub<Topics>();
	const received: number[] = [];
	const subscription = hub.subscribe("document", (event) => {
		received.push(event.documentID);
		subscription.unregister();
	});
	hub.publish("document", { documentID: 1 });
	hub.publish("document", { documentID: 2 });
	await settle();
	assert.deepEqual(received, [1]);
	assert.deepEqual(hub.stats(), emptyStats);
});

// ---------------------------------------------------------------- stream() exit paths

async function collectStream(
	hub: EventHub<Topics>,
	options: { key?: number; signal?: AbortSignal },
	bodyForEachEvent: (documentID: number) => "continue" | "break" | "throw" | "return",
) {
	const received: number[] = [];
	const finished = (async () => {
		for await (const event of hub.stream("document", options)) {
			received.push(event.documentID);
			const action = bodyForEachEvent(event.documentID);
			if (action === "break") break;
			if (action === "return") return;
			if (action === "throw") throw new Error("loop body threw");
		}
	})();
	await settle(); // generator bodies only run (and subscribe) once iteration starts
	return { received, finished };
}

test("stream: aborting while the loop is waiting ends the loop and cleans up", async () => {
	const hub = new EventHub<Topics>();
	const connection = new AbortController();
	const { received, finished } = await collectStream(hub, { key: 3, signal: connection.signal }, () => "continue");
	assert.equal(hub.stats().subscriberCount, 1);

	hub.publish("document", { documentID: 3 }, { key: 3 });
	await settle();
	connection.abort();
	await finished;

	assert.deepEqual(received, [3]);
	assert.deepEqual(hub.stats(), emptyStats);
});

for (const exitAction of ["break", "return", "throw"] as const) {
	test(`stream: \`${exitAction}\` out of the loop unsubscribes`, async () => {
		const hub = new EventHub<Topics>();
		const { finished } = await collectStream(hub, { key: 3 }, () => exitAction);
		hub.publish("document", { documentID: 3 }, { key: 3 });
		await (exitAction === "throw" ? assert.rejects(finished) : finished);
		assert.deepEqual(hub.stats(), emptyStats);
	});
}

test("stream: an already-aborted signal yields nothing and leaves nothing behind", async () => {
	const hub = new EventHub<Topics>();
	const { received, finished } = await collectStream(hub, { signal: AbortSignal.abort() }, () => "continue");
	await finished;
	assert.deepEqual(received, []);
	assert.deepEqual(hub.stats(), emptyStats);
});

test("stream: a generator that is created but never iterated never subscribes", () => {
	const hub = new EventHub<Topics>();
	hub.stream("document", { key: 1 });
	assert.deepEqual(hub.stats(), emptyStats);
});

// ---------------------------------------------------------------- many connections churning

test("10k connections opening and closing in random order leave the hub empty", async () => {
	const hub = new EventHub<Topics>();
	const connections: AbortController[] = [];
	for (let index = 0; index < 10_000; index++) {
		const connection = new AbortController();
		connections.push(connection);
		const key = index % 7 === 0 ? undefined : index % 500;
		if (index % 2 === 0) {
			hub.subscribe("document", silent, { key, signal: connection.signal });
		} else {
			void (async () => {
				for await (const _event of hub.stream("document", { key, signal: connection.signal }));
			})();
		}
	}
	await settle();
	assert.equal(hub.stats().subscriberCount, 10_000);

	for (let documentID = 0; documentID < 500; documentID++) hub.publish("document", { documentID }, { key: documentID });

	// Fisher-Yates with a fixed seed so a failure is reproducible.
	let seed = 42;
	const random = () => ((seed = (seed * 1103515245 + 12345) % 2 ** 31) / 2 ** 31);
	for (let index = connections.length - 1; index > 0; index--) {
		const swapIndex = Math.floor(random() * (index + 1));
		[connections[index], connections[swapIndex]] = [connections[swapIndex], connections[index]];
	}
	for (const connection of connections) connection.abort();
	await settle();

	assert.deepEqual(hub.stats(), emptyStats);
});

// ---------------------------------------------------------------- garbage collection

// V8 runtimes (Node, Deno) expose gc() with --expose-gc; Bun (JavaScriptCore) has Bun.gc(true) built in.
const host = globalThis as { gc?: () => void; Bun?: { gc(synchronous: boolean): void } };
const garbageCollect: (() => void) | undefined = host.gc ?? (host.Bun ? () => host.Bun!.gc(true) : undefined);
const skipWithoutGc = garbageCollect === undefined ? "needs --expose-gc (Node: --expose-gc, Deno: --v8-flags=--expose-gc)" : false;

// The handler closure captures a large buffer, standing in for an SSE connection and its page state.
// If anything in the hub still references the subscriber, the closure (and the buffer) can't be freed.
function makeConnectionState() {
	const pageState = new Uint8Array(1024);
	const handler = () => void pageState[0]++;
	return { handler, handlerRef: new WeakRef(handler) };
}

async function assertCollected(weakRefs: WeakRef<object>[], label: string) {
	for (let attempt = 0; attempt < 10 && weakRefs.some((ref) => ref.deref() !== undefined); attempt++) {
		await settle();
		garbageCollect!();
	}
	const leaked = weakRefs.filter((ref) => ref.deref() !== undefined).length;
	assert.ok(
		leaked <= conservativeGcTolerance,
		`${label}: ${leaked}/${weakRefs.length} handler closures still reachable`,
	);
}

// V8's GC is precise, so anything still reachable there is a real reference. JavaScriptCore scans the
// native stack conservatively: a stale word that looks like a pointer pins an object. Measured on Bun:
// 0-2 of 1,000 pinned at arbitrary indexes, and 0 of 10k/50k, so the count does not grow with load.
// A real leak pins all of them (1000/1000 with the abort-listener cleanup removed), so this still catches it.
const conservativeGcTolerance = host.gc === undefined && host.Bun !== undefined ? 5 : 0;

// Setup lives in plain sync functions: a suspended async test function can keep its last loop
// iteration's locals alive in a register (V8) or as a stale stack word that JavaScriptCore's
// conservative stack scan treats as a pointer (Bun). Either shows up as a false "1 leaked".
function subscribeAndUnregister(hub: EventHub<Topics>, count: number, signal?: AbortSignal) {
	const weakRefs: WeakRef<object>[] = [];
	for (let index = 0; index < count; index++) {
		const { handler, handlerRef } = makeConnectionState();
		weakRefs.push(handlerRef);
		hub.subscribe("document", handler, { key: index, signal }).unregister();
	}
	return weakRefs;
}

function subscribeHandlersThatStop(hub: EventHub<Topics>, count: number) {
	const weakRefs: WeakRef<object>[] = [];
	for (let index = 0; index < count; index++) {
		const pageState = new Uint8Array(1024);
		const handler = () => {
			pageState[0]++;
			throw new StopSubscription();
		};
		weakRefs.push(new WeakRef(handler));
		hub.subscribe("document", handler, { key: index });
	}
	return weakRefs;
}

test("GC: unregistered subscribers are freed while the hub lives on", { skip: skipWithoutGc }, async () => {
	const hub = new EventHub<Topics>();
	const weakRefs = subscribeAndUnregister(hub, 1_000);
	await assertCollected(weakRefs, "unregister");
	assert.deepEqual(hub.stats(), emptyStats);
});

test(
	"GC: a long-lived signal does not keep unregistered subscribers alive",
	{ skip: skipWithoutGc },
	async () => {
		// e.g. one server-wide shutdown signal passed to every subscription. If unregister() forgot to
		// remove its abort listener, the signal would hold every subscriber ever created.
		const hub = new EventHub<Topics>();
		const serverShutdown = new AbortController();
		const weakRefs = subscribeAndUnregister(hub, 1_000, serverShutdown.signal);
		await assertCollected(weakRefs, "long-lived signal");
		assert.equal(serverShutdown.signal.aborted, false);
	},
);

test("GC: subscribers that threw are freed", { skip: skipWithoutGc }, async () => {
	const hub = new EventHub<Topics>();
	const weakRefs = subscribeHandlersThatStop(hub, 1_000);
	for (let index = 0; index < 1_000; index++) hub.publish("document", { documentID: index }, { key: index });
	await assertCollected(weakRefs, "throw");
});

function openStreams(hub: EventHub<Topics>, count: number) {
	const weakRefs: WeakRef<object>[] = [];
	const connections: AbortController[] = [];
	for (let index = 0; index < count; index++) {
		const connection = new AbortController();
		connections.push(connection);
		const { handler, handlerRef } = makeConnectionState();
		weakRefs.push(handlerRef);
		void (async () => {
			for await (const _event of hub.stream("document", { key: index, signal: connection.signal })) handler();
		})();
	}
	return { weakRefs, closeAll: () => connections.splice(0).forEach((connection) => connection.abort()) };
}

test("GC: aborted streams and their loop bodies are freed", { skip: skipWithoutGc }, async () => {
	const hub = new EventHub<Topics>();
	const { weakRefs, closeAll } = openStreams(hub, 1_000);
	await settle();
	closeAll();
	await assertCollected(weakRefs, "stream abort");
	assert.deepEqual(hub.stats(), emptyStats);
});
