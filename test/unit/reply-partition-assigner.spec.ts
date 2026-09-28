import type { ClusterMetadata, ExtendedGroupProtocolSubscription } from '@platformatic/kafka';
import { describe, expect, it } from 'vitest';

import { replyPartitionAssigner } from '../../src/client/reply-partition-assigner.js';

function metadataWith(partitions: Record<string, number>): ClusterMetadata {
  const topics = new Map(
    Object.entries(partitions).map(([topic, count]) => [
      topic,
      { id: topic, partitions: [], partitionsCount: count, lastUpdate: 0 },
    ]),
  );
  return { id: 'cluster', brokers: new Map(), controllerId: 1, topics, lastUpdate: 0 };
}

function members(...ids: string[]): Map<string, ExtendedGroupProtocolSubscription> {
  return new Map(ids.map((memberId) => [memberId, { memberId, version: 1 }]));
}

describe('replyPartitionAssigner', () => {
  it('gives every member at least one partition of every reply topic when partitions >= members', () => {
    const result = replyPartitionAssigner(
      'b',
      members('b', 'a'),
      new Set(['x.reply', 'y.reply']),
      metadataWith({ 'x.reply': 3, 'y.reply': 2 }),
    );
    expect(result.map((r) => r.memberId)).toEqual(['a', 'b']);
    expect(result[0]?.assignments.get('x.reply')?.partitions).toEqual([0, 2]);
    expect(result[1]?.assignments.get('x.reply')?.partitions).toEqual([1]);
    expect(result[0]?.assignments.get('y.reply')?.partitions).toEqual([0]);
    expect(result[1]?.assignments.get('y.reply')?.partitions).toEqual([1]);
  });

  it('is deterministic regardless of the member iteration order', () => {
    const topics = new Set(['x.reply']);
    const meta = metadataWith({ 'x.reply': 4 });
    const a = replyPartitionAssigner('m1', members('m1', 'm2', 'm3'), topics, meta);
    const b = replyPartitionAssigner('m3', members('m3', 'm1', 'm2'), topics, meta);
    expect(a).toEqual(b);
  });

  it('leaves members without partitions when a topic has fewer partitions than members', () => {
    const result = replyPartitionAssigner(
      'a',
      members('a', 'b', 'c'),
      new Set(['x.reply']),
      metadataWith({ 'x.reply': 2 }),
    );
    expect(result[2]?.assignments.size).toBe(0);
    expect(result[0]?.assignments.get('x.reply')?.partitions).toEqual([0]);
    expect(result[1]?.assignments.get('x.reply')?.partitions).toEqual([1]);
  });

  it('ignores topics missing from the metadata', () => {
    const result = replyPartitionAssigner(
      'a',
      members('a'),
      new Set(['gone.reply']),
      metadataWith({}),
    );
    expect(result[0]?.assignments.size).toBe(0);
  });
});
