// Typed errors the SDK rejects/throws with — consumers discriminate on `.name`.

export class CommandRejectedError extends Error {
  override readonly name = "CommandRejectedError";
  constructor(
    readonly commandId: string,
    readonly reason: string,
  ) {
    super(`command ${commandId} rejected: ${reason}`);
  }
}

// Fatal: getToken() kept failing past the SDK's backoff retries (auth_failed). The client
// transitions to "failed" and stays disconnected — it does not silently loop forever.
export class AuthFailedError extends Error {
  override readonly name = "AuthFailedError";
  constructor(override readonly cause?: unknown) {
    super("getToken() failed after retries — auth_failed");
  }
}

// The `.log` replay could not fill a detected sequence gap after retries. The missing facts
// are NOT dropped and the stream never resumes live over the gap.
export class DeliveryFailedError extends Error {
  override readonly name = "DeliveryFailedError";
  constructor(
    readonly missingFrom: number,
    override readonly cause?: unknown,
  ) {
    super(`log replay could not fill the gap from sequence ${missingFrom}`);
  }
}
