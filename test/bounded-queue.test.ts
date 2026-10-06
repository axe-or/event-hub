import { test } from "node:test";
import assert from "node:assert/strict";
import { BoundedQueue } from "../src/bounded-queue.ts";

function drain<Item>(queue: BoundedQueue<Item>): Item[] {
	const items: Item[] = [];
	while (queue.length > 0) items.push(queue.shift()!);
	return items;
}

test("FIFO order below capacity", () => {
	const queue = new BoundedQueue<number>(10);
	for (let value = 1; value <= 5; value++) queue.push(value);
	assert.deepEqual(drain(queue), [1, 2, 3, 4, 5]);
	assert.equal(queue.shift(), undefined);
});

test("growing while the ring is wrapped around keeps order", () => {
	const queue = new BoundedQueue<number>(64);
	// Fill the initial 4 slots, consume 3 so head sits mid-array, then push past the old capacity.
	for (let value = 1; value <= 4; value++) queue.push(value);
	assert.deepEqual([queue.shift(), queue.shift(), queue.shift()], [1, 2, 3]);
	for (let value = 5; value <= 20; value++) queue.push(value);
	assert.deepEqual(drain(queue), Array.from({ length: 17 }, (_, index) => index + 4));
});

test("at capacity, the oldest item is dropped", () => {
	const queue = new BoundedQueue<number>(5);
	for (let value = 1; value <= 12; value++) queue.push(value);
	assert.equal(queue.length, 5);
	assert.deepEqual(drain(queue), [8, 9, 10, 11, 12]);
});

test("non-power-of-two capacities and interleaved push/shift", () => {
	const queue = new BoundedQueue<number>(7);
	const expectedContents: number[] = [];
	for (let value = 0; value < 1000; value++) {
		queue.push(value);
		expectedContents.push(value);
		if (expectedContents.length > 7) expectedContents.shift();
		if (value % 3 === 0) assert.equal(queue.shift(), expectedContents.shift());
		assert.equal(queue.length, expectedContents.length);
	}
	assert.deepEqual(drain(queue), expectedContents);
});

test("capacities below 1 behave as 1", () => {
	for (const requestedCapacity of [0, -5, 0.5]) {
		const queue = new BoundedQueue<string>(requestedCapacity);
		queue.push("old");
		queue.push("new");
		assert.deepEqual(drain(queue), ["new"]);
	}
});

test("clear() empties the queue and it stays usable", () => {
	const queue = new BoundedQueue<number>(16);
	for (let value = 0; value < 10; value++) queue.push(value);
	queue.clear();
	assert.equal(queue.length, 0);
	queue.push(42);
	assert.deepEqual(drain(queue), [42]);
});
