import { getPostgresConnection } from "./db.mjs";

const db = await getPostgresConnection();

process.on("message", async (msg) => {
  if (!msg || typeof msg !== "object") return;
  const { id, items } = msg;
  if (!id) return;

  try {
    if (!Array.isArray(items)) {
      process.send({ id, error: "items must be an array" });
      return;
    }

    const normalized = items.map((row) => {
      const out = {};
      for (const k of Object.keys(row)) {
        const key = String(k).trim();
        const val = row[k] == null ? null : String(row[k]).trim();
        out[key] = val === "" ? null : val;
      }
      return out;
    });

    const toInsert = normalized.filter(
      (r) => r.registro_car && r.registro_car !== "NULL"
    );
    const skippedCount = normalized.length - toInsert.length;

    if (toInsert.length > 0) {
      await db.car_gov.insertMany(toInsert);
    }

    process.send({ id, processedCount: toInsert.length, skippedCount });
  } catch (err) {
    console.error("Child insertion error:", err);
    process.send({ id, error: String(err?.message ?? err) });
    if (String(err?.message ?? "").includes("too many clients")) {
      process.exit(1);
    }
  }
});
