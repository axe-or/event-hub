// The complete list of host APIs the library may use, beyond ECMAScript itself.
// Only the build sees this file (types: [] and no DOM lib), so touching anything else, like
// `process`, `Deno`, `window` or `setTimeout`, fails the build instead of breaking some runtime later.
// Every listed API exists in Node >= 15, Deno, Bun, browsers, web workers and edge runtimes.

interface AbortSignal {
	readonly aborted: boolean;
	addEventListener(type: "abort", listener: () => void, options?: { once?: boolean }): void;
	removeEventListener(type: "abort", listener: () => void): void;
}

declare function queueMicrotask(callback: () => void): void;

declare var console: {
	error(...data: unknown[]): void;
};
