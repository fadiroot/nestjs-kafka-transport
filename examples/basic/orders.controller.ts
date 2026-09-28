import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext, MessagePattern, Payload } from '@nestjs/microservices';

export interface Order {
  id: string;
  items: { sku: string; price: number }[];
}

@Controller()
export class OrdersController {
  /** Request-reply: the caller gets the total back. */
  @MessagePattern('order.total')
  total(@Payload() order: Order, @Ctx() ctx: KafkaContext): number {
    console.log(`order.total from ${ctx.getTopic()}[${String(ctx.getPartition())}]`);
    return order.items.reduce((sum, item) => sum + item.price, 0);
  }

  /** Event: nobody waits for a reply. */
  @EventPattern('order.created')
  created(@Payload() order: Order): void {
    console.log(`order ${order.id} created with ${String(order.items.length)} items`);
  }
}
