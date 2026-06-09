export class CommandRejectedError extends Error {
  override readonly name = "CommandRejectedError";
  constructor(
    readonly commandId: string,
    readonly reason: string,
  ) {
    super(`command ${commandId} rejected: ${reason}`);
  }
}

export class AuthFailedError extends Error {
  override readonly name = "AuthFailedError";
  constructor(override readonly cause?: unknown) {
    super("getToken() failed after retries — auth_failed");
  }
}

export class DeliveryFailedError extends Error {
  override readonly name = "DeliveryFailedError";
  constructor(
    readonly missingFrom: number,
    override readonly cause?: unknown,
  ) {
    super(`log replay could not fill the gap from sequence ${missingFrom}`);
  }
}
