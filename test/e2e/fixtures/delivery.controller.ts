import { Controller } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  KafkaRetriableException,
  MessagePattern,
  Payload,
  RpcException,
} from '@nestjs/microservices';

import type { KafkaTransportContext } from '../../../src/index.js';

export const delivery: { handled: unknown[]; committed: string[] } = { handled: [], committed: [] };

/** Handlers for the commit-mode and dead-letter e2e suite. */
@Controller()
export class DeliveryController {
  @EventPattern('delivery.manual')
  async manual(
    @Payload() data: { id: string; commit: boolean },
    @Ctx() ctx: KafkaTransportContext,
  ): Promise<void> {
    delivery.handled.push(data);
    if (data.commit) {
      await ctx.commit();
      delivery.committed.push(data.id);
    }
  }

  @EventPattern('delivery.broken')
  broken(@Payload() data: { id: string }): never {
    delivery.handled.push(data);
    // Nest's RPC exception filter forwards an RpcException's error; any other exception becomes
    // "Internal server error" (and is logged), so the dead letter would carry only that.
    throw new RpcException(`cannot process ${data.id}`);
  }

  @MessagePattern('delivery.flaky')
  flaky(@Payload() data: { id: string }): never {
    delivery.handled.push(data);
    throw new KafkaRetriableException(`still failing ${data.id}`);
  }
}
