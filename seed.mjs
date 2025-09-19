import { getPostgresConnection } from "./db.mjs";

async function seedPostegres() {
  const db = await getPostgresConnection();
  console.log("creating table car_gov if not exists");
  await db.car_gov.createTable();
  console.log("table car_gov created with success");
  await db.client.end();
}
await seedPostegres();
