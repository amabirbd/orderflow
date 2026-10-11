import amqp from "amqplib";

const RABBITMQ_URL =
  process.env.RABBITMQ_URL ?? "amqp://guest:guest@localhost:5672";

const EXCHANGE = "order.events";
const ROUTING_KEY = "order.created";
const QUEUE = "notification.order-created";

// Dead-letter infrastructure for messages that fail processing.
const DLX = "order.events.dlx";
const DLQ = "notification.order-created.dlq";
const DLQ_ROUTING_KEY = "order.created.failed";

const connection = await amqp.connect(RABBITMQ_URL);
const channel = await connection.createChannel();

// Declare the exchanges and dead-letter queue first.
await channel.assertExchange(EXCHANGE, "topic", {
  durable: true,
});

await channel.assertExchange(DLX, "direct", {
  durable: true,
});

await channel.assertQueue(DLQ, {
  durable: true,
});

await channel.bindQueue(DLQ, DLX, DLQ_ROUTING_KEY);

// Messages rejected without requeue go to the DLQ.
await channel.assertQueue(QUEUE, {
  durable: true,
  arguments: {
    "x-dead-letter-exchange": DLX,
    "x-dead-letter-routing-key": DLQ_ROUTING_KEY,
  },
});

await channel.bindQueue(QUEUE, EXCHANGE, ROUTING_KEY);

// Allow only one unacknowledged message at a time.
await channel.prefetch(1);

console.log("Notification Service is listening");

channel.consume(
  QUEUE,
  (message) => {
    if (message === null) {
      return;
    }

    try {
      const event = JSON.parse(message.content.toString());

      // Validate the event contract.
      if (
        event.eventType !== "order.created" ||
        !event.eventId ||
        typeof event.data?.orderId !== "string" ||
        typeof event.data?.customerId !== "string"
      ) {
        throw new Error("Invalid order.created event");
      }

      console.log("Received event:", event.eventId);

      // Simulate sending a notification.
      console.log(
        `[NOTIFICATION] Order received: ${event.data.orderId}, customer: ${event.data.customerId}`,
      );

      // Acknowledge only after processing succeeds.
      channel.ack(message);
    } catch (error) {
      console.error("Event processing failed:", error);

      // Reject without requeue; the queue's DLX
      // routes this message to the dead-letter queue.
      channel.nack(message, false, false);
    }
  },
  {
    noAck: false,
  },
);
