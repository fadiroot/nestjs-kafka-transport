import { Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';

import { KafkaTransportServer } from '../../src/index.js';
import { OrdersController } from './orders.controller.js';

/* eslint-disable @typescript-eslint/no-extraneous-class -- Nest modules are declarative */
@Module({ controllers: [OrdersController] })
class AppModule {}
/* eslint-enable @typescript-eslint/no-extraneous-class */

const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

const app = await NestFactory.createMicroservice(AppModule, {
  strategy: new KafkaTransportServer({
    client: { clientId: 'orders', bootstrapBrokers: brokers },
    consumer: { groupId: 'orders' },
  }),
});
await app.listen();
console.log('orders service listening');
