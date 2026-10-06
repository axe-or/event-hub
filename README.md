# @mmf/eventhub

A typed, in-process event hub for fan-out to many listeners, such as SSE connections watching documents.
It has zero dependencies and runs in Node, Deno, Bun, browsers, workers and edge runtimes.

Each subscriber gets its own bounded mailbox and handles one event at a time, like a buffered Go
channel with a goroutine draining it. Subscribers can be keyed, like NATS subjects (`document.42`
vs `document.*`), so a publish only wakes the listeners that care.

```ts
import { EventHub } from "@mmf/eventhub";

type DocumentChanged = { documentID: number };
const hub = new EventHub<{ document: DocumentChanged }>();

// One document's viewers: like subscribing to "document.42"
for await (const event of hub.stream("document", { key: documentID, signal: request.signal })) {
	sse.patchElements(DocumentPage(await database.get(event.documentID)));
}

// After the DB write commits:
hub.publish("document", { documentID: 42 }, { key: 42 });
```

## Install

```sh
npm install @mmf/eventhub        # Node, Bun, bundlers
deno add npm:@mmf/eventhub       # Deno, via npm
deno add jsr:@mmf/eventhub       # Deno, from TypeScript source (if published to JSR)
```

It ships as ES modules only. CommonJS code can still `require()` it on Node ≥ 20.19 / 22.12.
There's no dual CJS build on purpose: with two copies of the module, code that `import`s the hub and code that `require`s it
would get two different `EventHub` classes and two separate sets of subscribers (the "dual package hazard").

## Usage

### Topics are typed

`TopicEvents` maps each topic to its event type. Publishing the wrong shape, or to an unknown topic, is a compile error.

```ts
const hub = new EventHub<{
	document: { documentID: number };
	user: { userID: string; action: "login" | "logout" };
}>();
```

### Keys: only wake the listeners that care

```ts
hub.subscribe("document", render, { key: 42 }); // only document 42 ("document.42")
hub.subscribe("document", renderList);           // every document ("document.*"), e.g. a list view

hub.publish("document", event, { key: 42 }); // → key 42 subscribers + topic-wide subscribers
hub.publish("document", event);              // no key → broadcast to every subscriber of the topic
```

Keys are `string | number`. With 10k listeners spread over 2k documents, keyed publishing is about 20–30× faster
than having every listener filter by `documentID` itself (see [Performance](#performance)).

### Two ways to consume

**Channel style** reads like `for event := range ch`, which suits SSE handlers:

```ts
for await (const event of hub.stream("document", { key: documentID, signal: request.signal })) {
	// ...
}
// Reached when the client disconnects. break, return and throw also unsubscribe.
```

**Callback style** is cheaper per event, at about 1/4 of the cost of `stream()` at 10k listeners:

```ts
hub.subscribe("document", async (event) => {
	sse.patchElements(DocumentPage(await database.get(event.documentID)));
}, { key: documentID, signal: request.signal });
```

### Cleanup

A subscription ends, and is fully removed from the hub, when any of these happens:

| How | Notes |
|---|---|
| `signal` aborts | Pass the request's `AbortSignal` and disconnects clean themselves up. |
| Handler throws `StopSubscription` | Ends quietly. |
| Handler throws anything else | Also ends, and the error goes to `onError` (default `console.error`). |
| `subscription.unregister()` | Safe to call more than once. |
| End of a `using` block | `using sub = hub.subscribe(...)`, like Go's `defer`. Needs `Symbol.dispose` (see below). |
| Leaving a `for await` over `stream()` | break, return, throw, or the signal aborting. |

Reusing one long-lived signal across many subscriptions (e.g. a server shutdown signal) doesn't leak:
each subscription removes its abort listener when it ends.

### Metrics

```ts
hub.stats(); // { topicCount, keyCount, subscriberCount }
```

A `subscriberCount` that keeps growing while your connection count is stable means something isn't being cleaned up.

## Semantics

- **`publish()` never runs handler code.** It only enqueues and returns, so handlers can't re-enter the publisher.
- **One event at a time per subscriber.** If a handler returns a promise (or any thenable), the next event waits for it.
  Different subscribers run independently.
- **Bounded mailboxes, newest wins.** Each subscriber holds up to `maxQueuedEvents` (default 64).
  When full, the oldest queued event is dropped. That suits "re-render the current state" consumers, where only the latest event matters.
  If you need every event, raise the bound and make sure handlers keep up.
- **Order is preserved** per subscriber.
- **A slow subscriber never blocks** the publisher or other subscribers.
- **Events queued when a subscription ends are discarded.**

## Runtime requirements

Beyond ECMAScript 2022, the library uses exactly three host APIs: `queueMicrotask`, `AbortSignal`
and `console.error`. That list is enforced: the build type-checks `src/` against
[`types/host-globals.d.ts`](types/host-globals.d.ts) only, with no DOM or Node typings, so using
anything else fails the build.

| Environment | Status |
|---|---|
| Node ≥ 18 | Tested on 22 (ESM `import` and `require()`). |
| Deno 2 | Tested on 2.9 (npm package and TS source). |
| Bun | Tested on 1.4 (JavaScriptCore). |
| Browsers, workers, edge runtimes | Bundled with esbuild `--platform=browser` and run in a bare realm that has only the three APIs above. |

**`using` needs `Symbol.dispose`.** It exists in current Node, Deno, Bun and Chromium. Where it's missing,
subscriptions don't get the dispose method, and `unregister()` and signals still work.

**TypeScript consumers** need a `Disposable` type in scope, because `Subscription` extends it.
It comes with `@types/node`, Deno, Bun, `"lib": ["esnext"]` or `"lib": [..., "esnext.disposable"]`. A browser
project with `"lib": ["es2022", "dom"]` and `skipLibCheck: false` is the one setup that will complain.
Add `"esnext.disposable"` to `lib`, or use `skipLibCheck: true`, which is the default in `tsc --init` and Vite.

**Duplicate installs are fine.** `StopSubscription` is recognized by a `Symbol.for` brand rather than `instanceof`,
so one thrown from another installed copy of this package (common in monorepos) still ends the subscription quietly.

## Performance

Measured on Node 22, Deno 2.9 and Bun 1.4 (all on the same Linux x86-64 machine), from `bench/fanout.ts`: 10,500
listeners (2,000 documents × 5 viewers + 500 list views), with each change published to one document.
Latency is per publish, until every relevant handler has run.

| | Node p50 | Deno p50 | Bun p50 |
|---|---|---|---|
| Plain `Set` of callbacks, filtering (sync floor) | 36 µs | 41 µs | 51 µs |
| Hub, one topic, every listener filters | 2.3 ms | 1.4 ms | 1.4 ms |
| **Hub, keyed `subscribe`** | **81 µs** | **51 µs** | **63 µs** |
| **Hub, keyed `stream`** | **359 µs** | **234 µs** | **268 µs** |

Memory per listener (hub overhead only, empty handlers), from `bench/memory.ts`:

| | V8 (Node/Deno) | JSC (Bun) |
|---|---|---|
| `subscribe`, keyed, with signal | ~1.1 KB | ~0.7 KB |
| `stream`, keyed | ~2.8 KB | ~1.6 KB |
| `subscribe` with a full 64-event mailbox | ~2.0 KB | ~1.6 KB |

10k viewers cost roughly 7–28 MB in the hub, which is small next to the sockets themselves.
Opening and closing 10k mixed connections for 30 rounds leaves the heap flat in all three runtimes.

`Array.prototype.shift` isn't used for queues: V8 makes it O(n) per call past a few thousand items,
so mailboxes use a ring buffer that grows lazily.

## Development

```sh
npm install
npm run typecheck      # library, tests, benches, examples
npm run build          # src/ → dist/ (ESM + .d.ts + source maps)
npm test               # Node, including GC leak tests
npm run test:deno      # same suite on Deno
npm run test:bun       # same suite on Bun
npm run bench:fanout   # also bench:throughput, bench:memory
```

The test suite uses `node:test`, which all three runtimes support. Alongside semantics, it checks that every way a
subscription can end leaves `stats()` at zero, and it uses `WeakRef`s with forced GC to check that subscribers are
actually freed rather than just removed from the hub's maps. To confirm those GC tests catch real leaks, a deliberately planted leak was
run against them, and they failed with 1,000 of 1,000 subscribers retained.

On Bun the GC tests allow up to 5 of 1,000 objects to stay reachable. JavaScriptCore scans the stack conservatively,
so a stale stack word can pin an object or two. The count doesn't grow with load (0 of 50,000), while a real leak pins
all of them. On V8, which has a precise GC, the tolerance is zero.

`examples/sse-server.ts` is a runnable live-updating document server using web-standard `Request`/`Response`
(`deno serve examples/sse-server.ts` or `bun examples/sse-server.ts`).
