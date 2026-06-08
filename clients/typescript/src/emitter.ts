export class Emitter<Events extends Record<string, (...args: never[]) => void>> {
  private readonly handlers = new Map<keyof Events, Set<(...args: never[]) => void>>();

  on<E extends keyof Events>(event: E, cb: Events[E]): () => void {
    let set = this.handlers.get(event);
    if (!set) {
      set = new Set();
      this.handlers.set(event, set);
    }
    set.add(cb as (...args: never[]) => void);
    return () => set!.delete(cb as (...args: never[]) => void);
  }

  emit<E extends keyof Events>(event: E, ...args: Parameters<Events[E]>): void {
    const set = this.handlers.get(event);
    if (!set) return;
    for (const cb of [...set]) (cb as (...a: Parameters<Events[E]>) => void)(...args);
  }
}
