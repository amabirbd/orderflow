import amqp from "amqplib";
import { randomUUID } from "node:crypto";

const RABBITMQ_URL =
  process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

const EXCHANGE = "order.events";

let connection;
let channel;

export async function connectRabbitMQ() {
  connection = await amqp.connect(RABBITMQ_URL);

  // A confirm channel lets us await broker confirmations.
  channel = await connection.createConfirmChannel();

  // Durable exchange survives broker restarts.
  await channel.assertExchange(EXCHANGE, "topic", {
    durable: true,
  });

  console.log("Connected to RabbitMQ");
}

export async function publishOrderCreated(order) {
  if (!channel) {
    throw new Error("RabbitMQ publisher is not initialized");
  }

  // Define a stable event contract.
  const event = {
    eventId: randomUUID(),
    eventType: "order.created",
    schemaVersion: 1,
    occurredAt: new Date().toISOString(),
    data: {
      orderId: order.id,
      customerId: order.customerId,
      items: order.items,
      status: order.status,
      createdAt: order.createdAt,
    },
  };

  const message = Buffer.from(JSON.stringify(event));

  channel.publish(EXCHANGE, "order.created", message, {
    persistent: true,
    contentType: "application/json",
    messageId: event.eventId,
    type: event.eventType,
    timestamp: Date.now(),
  });

  // Wait until RabbitMQ confirms the outstanding publishes.
  await channel.waitForConfirms();

  return event;
}
