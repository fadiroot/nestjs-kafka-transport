import type {
  ClusterMetadata,
  ExtendedGroupProtocolSubscription,
  GroupAssignment,
  GroupPartitionsAssignments,
} from '@platformatic/kafka';

/**
 * Partition assigner used by the client's reply consumer.
 *
 * Every client instance must own at least one partition of each reply topic it subscribed to,
 * because the server produces the reply straight to the partition named in the request
 * (`kafka_replyPartition`). The assigner therefore spreads the partitions of every topic over
 * the group members round-robin, sorted by member id so all members compute the same result.
 *
 * This mirrors `KafkaReplyPartitionAssigner` of the built-in transport. Stickiness across
 * rebalances is not needed: the client re-reads its assignments after each join and stamps the
 * current partition on outgoing requests.
 */
export function replyPartitionAssigner(
  _current: string,
  members: Map<string, ExtendedGroupProtocolSubscription>,
  topics: Set<string>,
  metadata: ClusterMetadata,
): GroupPartitionsAssignments[] {
  const memberIds = [...members.keys()].sort();
  const assignments = new Map<string, Map<string, GroupAssignment>>();
  for (const memberId of memberIds) {
    assignments.set(memberId, new Map());
  }

  for (const topic of [...topics].sort()) {
    const partitionsCount = metadata.topics.get(topic)?.partitionsCount ?? 0;
    for (let partition = 0; partition < partitionsCount; partition++) {
      const memberId = memberIds[partition % memberIds.length];
      if (memberId === undefined) {
        break;
      }
      const memberAssignments = assignments.get(memberId);
      if (!memberAssignments) {
        continue;
      }
      const existing = memberAssignments.get(topic);
      if (existing) {
        existing.partitions.push(partition);
      } else {
        memberAssignments.set(topic, { topic, partitions: [partition] });
      }
    }
  }

  return memberIds.map((memberId) => ({
    memberId,
    assignments: assignments.get(memberId) ?? new Map<string, GroupAssignment>(),
  }));
}
