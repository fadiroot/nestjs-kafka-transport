import { firstValueFrom, lastValueFrom } from 'rxjs';

import { KafkaTransportClient } from '../../src/index.js';
import type { Order } from './orders.controller.js';

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

const client = new KafkaTransportClient({
  client: { clientId: 'gateway', bootstrapBrokers: brokers },
  consumer: { groupId: 'gateway' },
});
client.subscribeToResponseOf('order.total');
await client.connect();

const order: Order = {
  id: 'o-1',
  items: [
    { sku: 'a', price: 10 },
    { sku: 'b', price: 5.5 },
  ],
};
const total = await firstValueFrom(client.send<number>('order.total', order));
console.log('total', total); // 15.5 (a number, not "15.5")

await lastValueFrom(client.emit('order.created', { key: order.id, value: order }));
await client.close();
