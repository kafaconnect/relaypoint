// A single-consumer async queue bridging push-style delivery to an AsyncIterable. Used by the
// delivery plane to hand ordered `.log` facts to `events()` consumers.
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

  // Single-consumer: a second concurrent iterator would silently split the ordered stream, so
  // it is refused. The interaction handle exposes one ordered `.log` consumer; fan a stream out
  // in app code if multiple consumers are needed.
  async *[Symbol.asyncIterator](): AsyncIterator<T> {
    if (this.iterating) throw new Error("Mailbox already has an active consumer (single-consumer)");
    this.iterating = true;
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
  }
}
