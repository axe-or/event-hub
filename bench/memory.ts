// Node: node --expose-gc · Deno: deno run --v8-flags=--expose-gc · Bun: bun --expose-gc
import { EventHub } from "../src/index.ts";

type Topics = { document: { documentID: number } };

// V8 runtimes (Node, Deno) expose gc() with --expose-gc; Bun (JavaScriptCore) has Bun.gc(true) built in.
const host = globalThis as { gc?: () => void; Bun?: { gc(synchronous: boolean): void } };
const garbageCollect: (() => void) | undefined = host.gc ?? (host.Bun ? () => host.Bun!.gc(true) : undefined);
if (garbageCollect === undefined) throw new Error("run with --expose-gc (Deno: --v8-flags=--expose-gc; Bun needs nothing)");

const settle = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

async function heapAfterFullGc(): Promise<number> {
	// Several rounds: freeing one generation (e.g. generator frames) can make the next one collectable.
	for (let round = 0; round < 4; round++) {
		await settle();
		garbageCollect!();
	}
	return process.memoryUsage().heapUsed;
}

const listenerCount = 10_000;
const documentCount = 2_000;
const kilobytes = (bytes: number) => `${(bytes / 1024).toFixed(1)} KB`;

type Scenario = {
	label: string;
	open: (hub: EventHub<Topics>, signal: AbortSignal, index: number) => void;
	// Optional traffic so mailboxes and stream buffers actually hold or have held events.
	traffic?: (hub: EventHub<Topics>) => void;
};

// Handlers are empty: this measures the hub's own cost, not your page state.
const scenarios: Scenario[] = [
	{
		label: "subscribe, keyed, no signal",
		open: (hub, _signal, index) => hub.subscribe("document", () => {}, { key: index % documentCount }),
	},
	{
		label: "subscribe, topic-wide",
		open: (hub, signal) => hub.subscribe("document", () => {}, { signal }),
	},
	{
		label: "subscribe, keyed (2k docs)",
		open: (hub, signal, index) => hub.subscribe("document", () => {}, { key: index % documentCount, signal }),
	},
	{
		label: "subscribe, keyed, unique key each",
		open: (hub, signal, index) => hub.subscribe("document", () => {}, { key: index, signal }),
	},
	{
		label: "stream, keyed (2k docs)",
		open: (hub, signal, index) => {
			void (async () => {
				for await (const _event of hub.stream("document", { key: index % documentCount, signal }));
			})();
		},
	},
	{
		label: "subscribe, keyed, 64 events queued",
		open: (hub, signal, index) =>
			hub.subscribe("document", () => new Promise<void>(() => {}), { key: index % documentCount, signal }),
		// The first event blocks forever, so the next 64 sit in each mailbox at its maximum size.
		traffic: (hub) => {
			for (let round = 0; round < 65; round++) {
				for (let documentID = 0; documentID < documentCount; documentID++) {
					hub.publish("document", { documentID }, { key: documentID });
				}
			}
		},
	},
];

console.log(`${listenerCount} listeners per scenario, empty handlers (hub overhead only)\n`);
console.log(`${"scenario".padEnd(36)} ${"per listener".padStart(13)} ${"total".padStart(10)} ${"left after close".padStart(17)}`);

for (const scenario of scenarios) {
	let hub: EventHub<Topics> | null = new EventHub<Topics>();
	let connections: AbortController[] = [];
	// Created up front so the controllers themselves aren't counted as hub cost.
	for (let index = 0; index < listenerCount; index++) connections.push(new AbortController());

	const heapBefore = await heapAfterFullGc();
	for (let index = 0; index < listenerCount; index++) scenario.open(hub, connections[index].signal, index);
	scenario.traffic?.(hub);
	const heapOpen = await heapAfterFullGc();

	const subscriptionsWithoutSignal = hub.stats().subscriberCount > 0 && scenario.label.includes("no signal");
	for (const connection of connections) connection.abort();
	if (subscriptionsWithoutSignal) hub = new EventHub<Topics>(); // no signal to close them, so drop the hub
	const statsAfterClose = hub.stats();
	// An aborted signal keeps its reason (a DOMException with a stack), so release the controllers
	// before measuring, or they'd be miscounted as hub leftovers.
	connections = [];
	const heapClosed = await heapAfterFullGc();
	if (statsAfterClose.subscriberCount !== 0) throw new Error(`${scenario.label}: dangling subscribers`);

	console.log(
		`${scenario.label.padEnd(36)} ${`${((heapOpen - heapBefore) / listenerCount).toFixed(0)} B`.padStart(13)} ` +
			`${kilobytes(heapOpen - heapBefore).padStart(10)} ${kilobytes(heapClosed - heapBefore).padStart(17)}`,
	);
	hub = null;
}

// Sustained churn: if anything leaks per connection, the heap climbs round over round.
console.log(`\nChurn: 30 rounds × ${listenerCount} connections opened, sent 2k events, then closed`);
const churnHub = new EventHub<Topics>();
let heapAtStart = 0;
const samples: string[] = [];
for (let round = 1; round <= 30; round++) {
	const connections: AbortController[] = [];
	for (let index = 0; index < listenerCount; index++) {
		const connection = new AbortController();
		connections.push(connection);
		const key = index % 10 === 0 ? undefined : index % documentCount;
		if (index % 2 === 0) {
			churnHub.subscribe("document", () => {}, { key, signal: connection.signal });
		} else {
			void (async () => {
				for await (const _event of churnHub.stream("document", { key, signal: connection.signal }));
			})();
		}
	}
	await settle();
	for (let documentID = 0; documentID < documentCount; documentID++) {
		churnHub.publish("document", { documentID }, { key: documentID });
	}
	await settle();
	for (const connection of connections) connection.abort();
	connections.length = 0;
	// Round 1 is warm-up (JIT code, hidden classes, V8 internal tables); measure growth from there.
	if (round === 1) heapAtStart = await heapAfterFullGc();
	if (round % 5 === 0) {
		const heapNow = await heapAfterFullGc();
		samples.push(`round ${String(round).padStart(2)}: ${kilobytes(heapNow - heapAtStart).padStart(9)} above round 1`);
	}
}
for (const sample of samples) console.log(`  ${sample}`);
console.log(`  stats after churn: ${JSON.stringify(churnHub.stats())}`);
