// Ring buffer that drops its oldest item when full.
//
// Why not an array with push/shift: V8's Array.prototype.shift is cheap only for small arrays and
// degrades to O(n) per call past a few thousand items. Why grow lazily: with 10k mostly-idle
// subscribers, preallocating maxItems slots each would waste memory for queues that rarely hold
// more than one event.
export class BoundedQueue<Item> {
	private slots: (Item | undefined)[];
	private headIndex = 0;
	private itemCount = 0;
	private readonly maxItems: number;

	constructor(maxItems: number) {
		this.maxItems = Math.max(1, Math.floor(maxItems));
		this.slots = new Array(Math.min(4, this.maxItems));
	}

	get length(): number {
		return this.itemCount;
	}

	push(item: Item): void {
		if (this.itemCount === this.slots.length) {
			if (this.slots.length < this.maxItems) {
				this.grow();
			} else {
				this.shift();
			}
		}
		this.slots[(this.headIndex + this.itemCount) % this.slots.length] = item;
		this.itemCount++;
	}

	shift(): Item | undefined {
		if (this.itemCount === 0) return undefined;
		const item = this.slots[this.headIndex];
		// Without clearing, the slot would keep the event reachable for as long as the queue lives.
		this.slots[this.headIndex] = undefined;
		this.headIndex = (this.headIndex + 1) % this.slots.length;
		this.itemCount--;
		return item;
	}

	clear(): void {
		this.slots = new Array(Math.min(4, this.maxItems));
		this.headIndex = 0;
		this.itemCount = 0;
	}

	private grow(): void {
		const newCapacity = Math.min(this.slots.length * 2, this.maxItems);
		const newSlots: (Item | undefined)[] = new Array(newCapacity);
		// Unrolls the wrapped-around ring so the oldest item lands at index 0.
		for (let offset = 0; offset < this.itemCount; offset++) {
			newSlots[offset] = this.slots[(this.headIndex + offset) % this.slots.length];
		}
		this.slots = newSlots;
		this.headIndex = 0;
	}
}
