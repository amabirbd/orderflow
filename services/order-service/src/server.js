import express from "express";
import { randomUUID } from "node:crypto";

import { connectRabbitMQ, publishOrderCreated } from "./rabbitmq.js";

const app = express();
const PORT = process.env.PORT || 3001;

const INVENTORY_URL = process.env.INVENTORY_URL || "http://localhost:3002";

app.use(express.json());

// Temporary in-memory storage
const orders = new Map();

// Health endpoint
app.get("/health", (req, res) => {
  res.json({
    service: "order-service",
    status: "UP",
  });
});

app.get("/orders", (req, res) => {
  const orderList = Array.from(orders.values());
  res.json(orderList);
});

// Create order

app.post("/orders", async (req, res) => {
  const { customerId, items } = req.body ?? {};

  // Step 1: Validate incoming data
  if (
    typeof customerId !== "string" ||
    !customerId.trim() ||
    !Array.isArray(items) ||
    items.length === 0 ||
    !items.every(
      (item) =>
        item &&
        typeof item.productId === "string" &&
        item.productId.trim() &&
        Number.isInteger(item.quantity) &&
        item.quantity > 0,
    )
  ) {
    return res.status(400).json({
      message: "Invalid order data",
    });
  }

  try {
    // Step 2: Call Inventory Service
    const response = await fetch(`${INVENTORY_URL}/inventory/check`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ items }),
      signal: AbortSignal.timeout(1500),
    });

    // Step 3: Handle upstream HTTP failures
    if (!response.ok) {
      console.error("Inventory Service returned:", response.status);

      return res.status(503).json({
        message: "Inventory Service is unavailable",
      });
    }

    // Step 4: Process inventory result
    const inventoryResult = await response.json();

    if (typeof inventoryResult.available !== "boolean") {
      return res.status(503).json({
        message: "Invalid inventory service response",
      });
    }

    if (!inventoryResult.available) {
      return res.status(409).json({
        message: "Insufficient inventory",
        details: inventoryResult.items,
      });
    }

    // Step 5: Create the order
    const order = {
      id: randomUUID(),
      customerId,
      items: items.map(({ productId, quantity }) => ({
        productId,
        quantity,
      })),
      status: "PENDING",
      createdAt: new Date().toISOString(),
    };

    orders.set(order.id, order);

    try {
      const event = await publishOrderCreated(order);

      console.log(`Published ${event.eventType}: ${event.eventId}`);

      return res.status(201).json(order);
    } catch (error) {
      console.error("Order event publication failed:", error);

      return res.status(503).json({
        message:
          "Order was created in memory, but event publication could not be confirmed. Retrying may create a duplicate.",
        orderId: order.id,
      });
    }
  } catch (error) {
    console.error("Inventory request failed:", error);

    if (error.name === "TimeoutError" || error.name === "AbortError") {
      return res.status(504).json({
        message: "Inventory Service timed out",
      });
    }

    return res.status(503).json({
      message: "Could not communicate with Inventory Service",
    });
  }
});

// Get order by ID
app.get("/orders/:id", (req, res) => {
  const order = orders.get(req.params.id);

  if (!order) {
    return res.status(404).json({
      message: "Order not found",
    });
  }

  res.json(order);
});

async function start() {
  await connectRabbitMQ();

  app.listen(PORT, () => {
    console.log(`Order Service running on port ${PORT}`);
  });
}

start().catch((error) => {
  console.error("Failed to start Order Service:", error);
  process.exitCode = 1;
});
