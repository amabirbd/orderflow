import express from "express";

const app = express();
const PORT = process.env.PORT || 3002;

app.use(express.json());

console.log(process.env.INVENTORY_DELAY_MS);

// Simulated inventory database
const inventory = new Map([
  ["laptop-101", 5],
  ["mouse-102", 10],
  ["keyboard-103", 3],
]);

app.get("/health", (req, res) => {
  res.json({
    service: "inventory-service",
    status: "UP",
  });
});

app.post("/inventory/check", async (req, res) => {
  const { items } = req.body ?? {};

  // Validate the request
  if (
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
      message: "Invalid inventory request",
    });
  }

  // Optional delay for failure simulation
  const delayMs = Number(process.env.INVENTORY_DELAY_MS ?? 0);

  if (Number.isFinite(delayMs) && delayMs > 0) {
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }

  // Combine quantities for duplicate product IDs
  const requested = new Map();

  for (const item of items) {
    requested.set(
      item.productId,
      (requested.get(item.productId) ?? 0) + item.quantity,
    );
  }

  const results = Array.from(requested, ([productId, requestedQuantity]) => {
    const availableQuantity = inventory.get(productId) ?? 0;

    return {
      productId,
      requestedQuantity,
      availableQuantity,
      available: availableQuantity >= requestedQuantity,
    };
  });

  res.json({
    available: results.every((item) => item.available),
    items: results,
  });
});

app.listen(PORT, () => {
  console.log(`Inventory Service running on port ${PORT}`);
});
