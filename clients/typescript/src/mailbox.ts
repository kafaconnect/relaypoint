// Single-consumer async queue bridging push delivery to an AsyncIterable.
export class Mailbox<T> implements AsyncIterable<T> {
  private readonly queue: T[] = [];
  private readonly waiters: Array<{
    resolve: (r: IteratorResult<T>) => void;
    reject: (e: unknown) => void;
  }> = [];
  private done = false;
  private failure: unknown;
  private iterating = false;

  push(item: T): void {
    if (this.done) return;
    const w = this.waiters.shift();
    if (w) w.resolve({ value: item, done: false });
    else this.queue.push(item);
  }

  error(e: unknown): void {
    if (this.done) return;
    this.failure = e;
    this.done = true;
    const w = this.waiters.shift();
    if (w) w.reject(e);
  }

  close(): void {
    if (this.done) return;
    this.done = true;
    for (const w of this.waiters.splice(0)) w.resolve({ value: undefined, done: true });
  }

  // One active consumer at a time (a second would silently split the ordered stream) — but the
  // flag is released when a consumer stops (break/return/throw) so the next call to events() can
  // resume; it is NOT a permanent lock.
  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    if (this.iterating) throw new Error("Mailbox already has an active consumer (single-consumer)");
    this.iterating = true;
    try {
      for (;;) {
        if (this.queue.length > 0) {
          yield this.queue.shift() as T;
          continue;
        }
        if (this.done) {
          if (this.failure !== undefined) throw this.failure;
          return;
        }
        const result = await new Promise<IteratorResult<T>>((resolve, reject) => {
          this.waiters.push({ resolve, reject });
        });
        if (result.done) return;
        yield result.value;
      }
    } finally {
      this.iterating = false;
    }
  }
}
