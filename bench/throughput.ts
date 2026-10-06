import { EventEmitter, setMaxListeners } from "node:events";
import { EventHub } from "../src/index.ts";

type DocumentEvent = { documentID: number };

const settle = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

async function measure(label: string, totalDeliveries: number, run: () => Promise<number>) {
	// Warm-up lets V8 tier the hot paths up to optimized code before we time them.
	for (let warmupRound = 0; warmupRound < 3; warmupRound++) await run();
	(globalThis as { gc?: () => void }).gc?.();

	const heapBefore = process.memoryUsage().heapUsed;
	const startTime = performance.now();
	const deliveredCount = await run();
	const elapsedMilliseconds = performance.now() - startTime;
	const heapDeltaMegabytes = (process.memoryUsage().heapUsed - heapBefore) / 1e6;

	if (deliveredCount !== totalDeliveries) throw new Error(`${label}: delivered ${deliveredCount}/${totalDeliveries}`);
	const nanosecondsPerDelivery = (elapsedMilliseconds * 1e6) / totalDeliveries;
	console.log(
		`${label.padEnd(34)} ${elapsedMilliseconds.toFixed(1).padStart(8)} ms  ` +
			`${nanosecondsPerDelivery.toFixed(0).padStart(6)} ns/delivery  heap Δ ${heapDeltaMegabytes.toFixed(1)} MB`,
	);
}

async function runScenario(title: string, subscriberCount: number, eventCount: number, awaitBetweenPublishes: boolean) {
	const totalDeliveries = subscriberCount * eventCount;
	console.log(`\n## ${title}: ${subscriberCount} subscribers × ${eventCount} events = ${totalDeliveries} deliveries`);

	// Each publish is either fired back-to-back (burst) or followed by letting subscribers finish (steady trickle).
	const publishAll = async (publishOne: (eventIndex: number) => void) => {
		for (let eventIndex = 0; eventIndex < eventCount; eventIndex++) {
			publishOne(eventIndex);
			if (awaitBetweenPublishes) await settle();
		}
		await settle();
	};

	await measure("plain Set<callback> (sync)", totalDeliveries, async () => {
		let deliveredCount = 0;
		const callbacks = new Set<(event: DocumentEvent) => void>();
		for (let index = 0; index < subscriberCount; index++) callbacks.add(() => void deliveredCount++);
		await publishAll((eventIndex) => {
			for (const callback of callbacks) callback({ documentID: eventIndex });
		});
		return deliveredCount;
	});

	await measure("node EventEmitter (sync)", totalDeliveries, async () => {
		let deliveredCount = 0;
		const emitter = new EventEmitter();
		emitter.setMaxListeners(0);
		for (let index = 0; index < subscriberCount; index++) emitter.on("document", () => void deliveredCount++);
		await publishAll((eventIndex) => emitter.emit("document", { documentID: eventIndex }));
		return deliveredCount;
	});

	await measure("EventTarget (sync, web standard)", totalDeliveries, async () => {
		let deliveredCount = 0;
		const target = new EventTarget();
		setMaxListeners(0, target);
		for (let index = 0; index < subscriberCount; index++) target.addEventListener("document", () => void deliveredCount++);
		await publishAll(() => target.dispatchEvent(new Event("document")));
		return deliveredCount;
	});

	const runHub = async (subscribeOne: (hub: EventHub<{ document: DocumentEvent }>, countDelivery: () => void) => void) => {
		let deliveredCount = 0;
		const hub = new EventHub<{ document: DocumentEvent }>();
		for (let index = 0; index < subscriberCount; index++) subscribeOne(hub, () => void deliveredCount++);
		await publishAll((eventIndex) => hub.publish("document", { documentID: eventIndex }));
		return deliveredCount;
	};

	await measure("hub.subscribe, sync handler", totalDeliveries, () =>
		runHub((hub, countDelivery) => hub.subscribe("document", countDelivery, { maxQueuedEvents: eventCount })),
	);

	await measure("hub.subscribe, async handler", totalDeliveries, () =>
		runHub((hub, countDelivery) =>
			hub.subscribe("document", async () => countDelivery(), { maxQueuedEvents: eventCount }),
		),
	);

	await measure("hub.stream (for await)", totalDeliveries, () =>
		runHub((hub, countDelivery) => {
			void (async () => {
				for await (const _event of hub.stream("document", { maxQueuedEvents: eventCount })) countDelivery();
			})();
		}),
	);
}

await runScenario("Burst", 100, 10_000, false);
await runScenario("Fan-out trickle (SSE-like)", 2_000, 200, true);
await runScenario("Big backlog (large maxQueuedEvents)", 1, 200_000, false);
