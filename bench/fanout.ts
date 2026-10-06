import { EventHub } from "../src/index.ts";

type DocumentEvent = { documentID: number };

const documentCount = 2_000;
const viewersPerDocument = 5;
const listViewerCount = 500;
const totalListeners = documentCount * viewersPerDocument + listViewerCount;
const changeCount = 2_000;
const expectedDeliveries = changeCount * (viewersPerDocument + listViewerCount);

// setImmediate runs after the microtask queue is empty, i.e. after every handler for this publish finished.
const allHandlersDone = () => new Promise<void>((resolve) => setImmediate(resolve));

type Setup = (countRelevantDelivery: () => void) => {
	publishChange: (documentID: number) => void;
	shutdown: () => void;
};

async function measure(label: string, setup: Setup) {
	const runOnce = async () => {
		let relevantDeliveries = 0;
		const { publishChange, shutdown } = setup(() => void relevantDeliveries++);
		await allHandlersDone();

		const latenciesMicroseconds: number[] = [];
		for (let changeIndex = 0; changeIndex < changeCount; changeIndex++) {
			const startTime = performance.now();
			publishChange(changeIndex % documentCount);
			await allHandlersDone();
			latenciesMicroseconds.push((performance.now() - startTime) * 1000);
		}
		shutdown();
		await allHandlersDone();
		if (relevantDeliveries !== expectedDeliveries) {
			throw new Error(`${label}: ${relevantDeliveries}/${expectedDeliveries} relevant deliveries`);
		}
		return latenciesMicroseconds.sort((left, right) => left - right);
	};

	await runOnce();
	const sortedLatencies = await runOnce();
	const percentile = (fraction: number) => sortedLatencies[Math.floor(fraction * (sortedLatencies.length - 1))];
	console.log(
		`${label.padEnd(40)} p50 ${percentile(0.5).toFixed(0).padStart(6)} µs   p99 ${percentile(0.99).toFixed(0).padStart(6)} µs`,
	);
}

function forEachViewer(visit: (watchedDocumentID: number | null) => void) {
	for (let documentID = 0; documentID < documentCount; documentID++) {
		for (let viewer = 0; viewer < viewersPerDocument; viewer++) visit(documentID);
	}
	for (let viewer = 0; viewer < listViewerCount; viewer++) visit(null);
}

console.log(
	`${totalListeners} listeners (${documentCount} docs × ${viewersPerDocument} viewers + ${listViewerCount} list views), ` +
		`${changeCount} changes, latency per publish:\n`,
);

await measure("baseline: plain Set, filter in callback", (countRelevantDelivery) => {
	const callbacks = new Set<(event: DocumentEvent) => void>();
	forEachViewer((watchedDocumentID) =>
		callbacks.add((event) => {
			if (watchedDocumentID === null || event.documentID === watchedDocumentID) countRelevantDelivery();
		}),
	);
	return {
		publishChange: (documentID) => {
			for (const callback of callbacks) callback({ documentID });
		},
		shutdown: () => callbacks.clear(),
	};
});

await measure("hub: one topic, filter in handler", (countRelevantDelivery) => {
	const hub = new EventHub<{ document: DocumentEvent }>();
	const connections = new AbortController();
	forEachViewer((watchedDocumentID) =>
		hub.subscribe(
			"document",
			(event) => {
				if (watchedDocumentID === null || event.documentID === watchedDocumentID) countRelevantDelivery();
			},
			{ signal: connections.signal },
		),
	);
	return {
		publishChange: (documentID) => hub.publish("document", { documentID }),
		shutdown: () => connections.abort(),
	};
});

await measure("hub: keyed subscribe", (countRelevantDelivery) => {
	const hub = new EventHub<{ document: DocumentEvent }>();
	const connections = new AbortController();
	forEachViewer((watchedDocumentID) =>
		hub.subscribe("document", () => countRelevantDelivery(), {
			key: watchedDocumentID ?? undefined,
			signal: connections.signal,
		}),
	);
	return {
		publishChange: (documentID) => hub.publish("document", { documentID }, { key: documentID }),
		shutdown: () => connections.abort(),
	};
});

await measure("hub: keyed stream (for await)", (countRelevantDelivery) => {
	const hub = new EventHub<{ document: DocumentEvent }>();
	const connections = new AbortController();
	forEachViewer((watchedDocumentID) => {
		void (async () => {
			const events = hub.stream("document", {
				key: watchedDocumentID ?? undefined,
				signal: connections.signal,
			});
			for await (const _event of events) countRelevantDelivery();
		})();
	});
	return {
		publishChange: (documentID) => hub.publish("document", { documentID }, { key: documentID }),
		shutdown: () => connections.abort(),
	};
});

await measure("hub: unkeyed stream, filter in loop", (countRelevantDelivery) => {
	const hub = new EventHub<{ document: DocumentEvent }>();
	const connections = new AbortController();
	forEachViewer((watchedDocumentID) => {
		void (async () => {
			for await (const event of hub.stream("document", { signal: connections.signal })) {
				if (watchedDocumentID === null || event.documentID === watchedDocumentID) countRelevantDelivery();
			}
		})();
	});
	return {
		publishChange: (documentID) => hub.publish("document", { documentID }),
		shutdown: () => connections.abort(),
	};
});
