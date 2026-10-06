// Simple, self-contained, typed event hub.
//
// Semantics are modelled on a buffered Go channel per subscriber:
//   - publish() never runs handler code inline; it only enqueues (like `ch <- ev` on a buffered chan).
//   - each subscriber drains its own mailbox one event at a time, awaiting async handlers,
//     so events for one subscriber are never processed concurrently or out of order.
//   - a slow subscriber cannot block publishers or other subscribers; when its mailbox is full
//     the oldest event is dropped (for "re-render the page" style consumers, only the latest matters).
//
// Uses only ECMAScript plus queueMicrotask and AbortSignal, which every current runtime provides
// (Node, Deno, Bun, browsers, workers, edge runtimes).

import { BoundedQueue } from "./bounded-queue.ts";

export type EventHandler<Event> = (event: Event) => void | PromiseLike<void>;

export type SubscriptionKey = string | number;

export interface SubscribeOptions {
	/**
	 * When aborted, the subscription is removed. Pass the HTTP request's signal so a closed
	 * SSE connection unsubscribes without the handler having to notice.
	 */
	signal?: AbortSignal;
	/**
	 * Only receive events published with this key, like the NATS subject `document.42`.
	 * Omit it to receive every event on the topic, like `document.*` (e.g. for a list view).
	 */
	key?: SubscriptionKey;
	/** Mailbox size. When it is full, the oldest queued event is dropped. Default 64. */
	maxQueuedEvents?: number;
	/** Called when a handler throws anything but {@link StopSubscription}. Default: `console.error`. */
	onError?: (error: unknown) => void;
}

export type StreamOptions = Pick<SubscribeOptions, "signal" | "key" | "maxQueuedEvents">;

export interface PublishOptions {
	/**
	 * Delivered to subscribers of this key plus the topic-wide subscribers.
	 * Omit it to broadcast to every subscriber of the topic, keyed or not.
	 */
	key?: SubscriptionKey;
}

/** Returned by {@link EventHub.subscribe}. Usable with `using` where the runtime has `Symbol.dispose`. */
export interface Subscription extends Disposable {
	readonly closed: boolean;
	/** Removes the subscription and discards queued events. Safe to call more than once. */
	unregister(): void;
}

export interface EventHubStats {
	topicCount: number;
	keyCount: number;
	subscriberCount: number;
}

// Symbol.for gives the same symbol in every realm and every copy of this package, so a
// StopSubscription thrown by one installed copy is still recognized by the hub from another
// (duplicate installs are common in monorepos, and `instanceof` fails across them).
const stopSubscriptionBrand = Symbol.for("eventhub.StopSubscription");

/**
 * Throw from a handler to unsubscribe without the error being reported.
 * Any other thrown error also unsubscribes, but goes to `onError`.
 */
export class StopSubscription extends Error {
	readonly [stopSubscriptionBrand] = true;

	constructor() {
		super("subscription stopped by handler");
		this.name = "StopSubscription";
	}
}

function isStopSubscription(error: unknown): boolean {
	return typeof error === "object" && error !== null && (error as Record<symbol, unknown>)[stopSubscriptionBrand] === true;
}

// Checks for a thenable rather than `instanceof Promise`, which misses promises from other realms
// (iframes, vm contexts) and from promise libraries; those would otherwise go un-awaited.
function isThenable(value: unknown): value is PromiseLike<unknown> {
	return typeof value === "object" && value !== null && typeof (value as PromiseLike<unknown>).then === "function";
}

const defaultOnError = (error: unknown) => {
	console.error("eventhub: handler threw; subscription removed:", error);
};

class Subscriber<Event> implements Subscription {
	private pendingEvents: BoundedQueue<Event>;
	private draining = false;
	private isClosed = false;

	// Installed on the prototype below, only where the runtime has Symbol.dispose.
	declare [Symbol.dispose]: () => void;

	constructor(
		private readonly handler: EventHandler<Event>,
		maxQueuedEvents: number,
		private readonly onError: (error: unknown) => void,
		private readonly detachFromHub: () => void,
	) {
		this.pendingEvents = new BoundedQueue(maxQueuedEvents);
	}

	get closed(): boolean {
		return this.isClosed;
	}

	deliver(event: Event): void {
		if (this.isClosed) return;

		this.pendingEvents.push(event);

		if (!this.draining) {
			this.draining = true;
			// Deferred so the publisher's call stack never contains subscriber code (no reentrancy surprises).
			queueMicrotask(() => void this.drain());
		}
	}

	private async drain(): Promise<void> {
		while (!this.isClosed && this.pendingEvents.length > 0) {
			const event = this.pendingEvents.shift()!;
			try {
				const handlerResult = this.handler(event);
				// Awaiting a non-promise still costs a microtask hop per event; sync handlers skip it.
				if (isThenable(handlerResult)) await handlerResult;
			} catch (error) {
				if (!isStopSubscription(error)) this.reportError(error);
				this.unregister();
			}
		}
		this.draining = false;
	}

	private reportError(error: unknown): void {
		try {
			this.onError(error);
		} catch {
			// A throwing onError must not wedge the drain loop with `draining` stuck at true.
		}
	}

	unregister(): void {
		if (this.isClosed) return;
		this.isClosed = true;
		this.pendingEvents.clear();
		this.detachFromHub();
	}
}

// Defined conditionally because a computed `[Symbol.dispose]` method on a runtime without the symbol
// would silently create a method named "undefined". Without it, `using` is unavailable but
// unregister() and signals work the same.
if (typeof Symbol.dispose === "symbol") {
	Object.defineProperty(Subscriber.prototype, Symbol.dispose, {
		value: function dispose(this: Subscriber<unknown>) {
			this.unregister();
		},
		writable: true,
		configurable: true,
	});
}

class TopicSubscribers {
	readonly topicWide = new Set<Subscriber<any>>();
	readonly byKey = new Map<SubscriptionKey, Set<Subscriber<any>>>();

	get isEmpty(): boolean {
		return this.topicWide.size === 0 && this.byKey.size === 0;
	}
}

/**
 * In-process pub/sub. Each subscriber has its own bounded mailbox and handles one event at a time.
 *
 * `TopicEvents` maps each topic name to its event type, so publish and subscribe are checked per topic:
 *
 * ```ts
 * const hub = new EventHub<{ document: { documentID: number } }>();
 * hub.publish("document", { documentID: 42 }, { key: 42 });
 * ```
 */
export class EventHub<TopicEvents extends Record<string, unknown>> {
	private subscribersByTopic = new Map<keyof TopicEvents, TopicSubscribers>();

	/** Current counts. Cheap enough for a metrics endpoint; a steadily growing `subscriberCount` means a leak. */
	stats(): EventHubStats {
		let keyCount = 0;
		let subscriberCount = 0;
		for (const topicSubscribers of this.subscribersByTopic.values()) {
			subscriberCount += topicSubscribers.topicWide.size;
			keyCount += topicSubscribers.byKey.size;
			for (const keyedSubscribers of topicSubscribers.byKey.values()) subscriberCount += keyedSubscribers.size;
		}
		return { topicCount: this.subscribersByTopic.size, keyCount, subscriberCount };
	}

	/** Enqueues the event for matching subscribers and returns immediately; no handler runs inside this call. */
	publish<Topic extends keyof TopicEvents>(
		topic: Topic,
		event: TopicEvents[Topic],
		options: PublishOptions = {},
	): void {
		const topicSubscribers = this.subscribersByTopic.get(topic);
		if (topicSubscribers === undefined) return;

		// No snapshot needed: deliver() only enqueues, so no subscriber code can add or remove
		// subscribers while these loops run.
		for (const subscriber of topicSubscribers.topicWide) subscriber.deliver(event);

		if (options.key === undefined) {
			for (const keyedSubscribers of topicSubscribers.byKey.values()) {
				for (const subscriber of keyedSubscribers) subscriber.deliver(event);
			}
		} else {
			const keyedSubscribers = topicSubscribers.byKey.get(options.key);
			if (keyedSubscribers === undefined) return;
			for (const subscriber of keyedSubscribers) subscriber.deliver(event);
		}
	}

	/**
	 * Calls `handler` for each event, one at a time, awaiting it if it returns a promise.
	 * Ends when `signal` aborts, `unregister()` is called, or the handler throws.
	 */
	subscribe<Topic extends keyof TopicEvents>(
		topic: Topic,
		handler: EventHandler<TopicEvents[Topic]>,
		options: SubscribeOptions = {},
	): Subscription {
		const { signal, key, maxQueuedEvents = 64, onError = defaultOnError } = options;

		let topicSubscribers = this.subscribersByTopic.get(topic);
		if (topicSubscribers === undefined) {
			topicSubscribers = new TopicSubscribers();
			this.subscribersByTopic.set(topic, topicSubscribers);
		}
		const owningTopic = topicSubscribers;

		let owningSet: Set<Subscriber<any>>;
		if (key === undefined) {
			owningSet = owningTopic.topicWide;
		} else {
			let keyedSubscribers = owningTopic.byKey.get(key);
			if (keyedSubscribers === undefined) {
				keyedSubscribers = new Set();
				owningTopic.byKey.set(key, keyedSubscribers);
			}
			owningSet = keyedSubscribers;
		}

		const unregisterOnAbort = () => subscriber.unregister();

		const subscriber = new Subscriber<TopicEvents[Topic]>(handler, maxQueuedEvents, onError, () => {
			owningSet.delete(subscriber);
			// Empty sets are removed so documents nobody watches anymore don't pile up as dead map entries.
			if (key !== undefined && owningSet.size === 0 && owningTopic.byKey.get(key) === owningSet) {
				owningTopic.byKey.delete(key);
			}
			if (owningTopic.isEmpty && this.subscribersByTopic.get(topic) === owningTopic) {
				this.subscribersByTopic.delete(topic);
			}
			// A long-lived signal (e.g. server shutdown) would otherwise keep every past subscriber alive.
			signal?.removeEventListener("abort", unregisterOnAbort);
		});

		owningSet.add(subscriber);

		if (signal?.aborted) {
			subscriber.unregister();
		} else {
			signal?.addEventListener("abort", unregisterOnAbort, { once: true });
		}

		return subscriber;
	}

	/**
	 * Channel-style consumption, like Go's `for event := range ch`:
	 *
	 * ```ts
	 * for await (const event of hub.stream("document", { key: documentID, signal: request.signal })) { ... }
	 * ```
	 *
	 * Ends when the signal aborts. Leaving the loop (break, return or throw) unsubscribes.
	 * Nothing is subscribed until iteration starts.
	 */
	async *stream<Topic extends keyof TopicEvents>(
		topic: Topic,
		options: StreamOptions = {},
	): AsyncGenerator<TopicEvents[Topic], void, undefined> {
		const { signal, key, maxQueuedEvents = 64 } = options;
		const bufferedEvents = new BoundedQueue<TopicEvents[Topic]>(maxQueuedEvents);
		let wakeConsumer: (() => void) | null = null;

		const wake = () => {
			const resolve = wakeConsumer;
			wakeConsumer = null;
			resolve?.();
		};

		const subscription = this.subscribe(
			topic,
			(event) => {
				bufferedEvents.push(event);
				wake();
			},
			// The inner mailbox must be as large as our buffer, or a burst is dropped before it reaches us.
			{ signal, key, maxQueuedEvents },
		);
		signal?.addEventListener("abort", wake, { once: true });

		try {
			while (!subscription.closed) {
				if (bufferedEvents.length === 0) {
					await new Promise<void>((resolve) => {
						wakeConsumer = resolve;
					});
					continue;
				}
				yield bufferedEvents.shift()!;
			}
		} finally {
			subscription.unregister();
			signal?.removeEventListener("abort", wake);
		}
	}
}
