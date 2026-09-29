import { Controller } from '@nestjs/common';
import {
  Ctx,
  EventPattern,
  KafkaContext,
  KafkaRetriableException,
  MessagePattern,
  Payload,
  RpcException,
} from '@nestjs/microservices';
import { Observable, from } from 'rxjs';

import type { KafkaTransportContext } from '../../../src/index.js';

export const received: { events: unknown[]; contexts: KafkaContext[]; retriable: number } = {
  events: [],
  contexts: [],
  retriable: 0,
};

/** Handlers mirroring the built-in transport's e2e fixtures (`integration/microservices/src/kafka`). */
@Controller()
export class MathController {
  @MessagePattern('math.sum.sync.kafka.message')
  sumFromRecord(@Payload() data: { numbers: number[] }, @Ctx() ctx: KafkaContext): number {
    received.contexts.push(ctx);
    return data.numbers.reduce((a, b) => a + b, 0);
  }

  @MessagePattern('math.sum.sync.array')
  sumArray(@Payload() data: number[]): number {
    return data.reduce((a, b) => a + b, 0);
  }

  @MessagePattern('math.sum.sync.string')
  sumString(@Payload() data: string): number {
    return data
      .split(',')
      .map(Number)
      .reduce((a, b) => a + b, 0);
  }

  @MessagePattern('math.sum.sync.number')
  sumNumber(@Payload() data: string): number {
    return Number(data) * 2;
  }

  @MessagePattern('math.stream')
  stream(@Payload() data: { count: number }): Observable<number> {
    return from(Array.from({ length: data.count }, (_, i) => i + 1));
  }

  @MessagePattern('math.fail')
  fail(): never {
    throw new RpcException('boom');
  }

  @MessagePattern(/^math\.sum\.sync\.regex\..+$/)
  sumRegex(@Payload() data: { numbers: number[] }): number {
    return data.numbers.reduce((a, b) => a + b, 0) * 10;
  }

  @MessagePattern('math.operation')
  operation(
    @Payload() _data: unknown,
    @Ctx() ctx: KafkaTransportContext,
  ): { operationId: string | null } {
    return { operationId: ctx.getOperationId() ?? null };
  }

  @MessagePattern('math.slow')
  async slow(@Payload() data: { ms: number }): Promise<string> {
    await new Promise((resolve) => setTimeout(resolve, data.ms));
    return 'late';
  }

  @EventPattern('notify')
  notify(@Payload() data: unknown, @Ctx() ctx: KafkaContext): void {
    received.events.push(data);
    received.contexts.push(ctx);
  }

  @EventPattern('notify.retriable')
  retriable(@Payload() data: { failUntil: number }): void {
    received.retriable += 1;
    if (received.retriable <= data.failUntil) {
      throw new KafkaRetriableException('try again');
    }
    received.events.push({ retriable: received.retriable });
  }
}
