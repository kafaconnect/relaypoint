// Code generated from contracts/asyncapi/participation.yaml by scripts/generate_participation_addresses.sh. DO NOT EDIT.

const uuidv7 = /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

export function mutateDesiredParticipationAddress(tenant: string, interaction: string): string {
  return participationAddress("rpc.corex.participation-mutate.v1", tenant, interaction);
}

export function participationCommandAddress(tenant: string, interaction: string): string {
  return participationAddress("corex.participation.commands.v1", tenant, interaction);
}

export function replayParticipationAddress(tenant: string, interaction: string): string {
  return participationAddress("rpc.corex.participation-replay.v1", tenant, interaction);
}

export function desiredParticipationSnapshotAddress(tenant: string, interaction: string): string {
  return participationAddress("rpc.corex.participation-snapshot.v1", tenant, interaction);
}

function participationAddress(prefix: string, tenant: string, interaction: string): string {
  if (!uuidv7.test(tenant) || !uuidv7.test(interaction)) throw new Error("invalid participation address");
  return `${prefix}.${tenant}.${interaction}`;
}
