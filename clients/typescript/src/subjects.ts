export const cmdSubject = (tenantId: string, interactionId: string): string =>
  `tenant.${tenantId}.interaction.${interactionId}.cmd`;

export const logSubject = (tenantId: string, interactionId: string): string =>
  `tenant.${tenantId}.interaction.${interactionId}.log`;

export const signalSubject = (tenantId: string, interactionId: string, selfUserId: string): string =>
  `tenant.${tenantId}.interaction.${interactionId}.signal.${selfUserId}`;
