import fs from "fs";
import csv from "csv-parser";
import { initialize } from "./cluster.mjs";
import { getPostgresConnection } from "./db.mjs";
import cliProgress from "cli-progress";
import { setTimeout } from "node:timers/promises";
import os from "os";

const ITEMS_PER_PAGE = 4000;
const DEFAULT_CLUSTER = Math.max(
  1,
  Math.min(Number(process.env.CLUSTER_SIZE || os.cpus().length), 8)
);
const CLUSTER_SIZE = DEFAULT_CLUSTER;
const TASK_FILE = new URL("./background-task.mjs", import.meta.url).pathname;
const FILE_PATH = "./data-to-import/planilha.CSV";
const MAX_IN_FLIGHT_BATCHES = CLUSTER_SIZE * 2;

async function countCsvLines(filePath) {
  return new Promise((resolve, reject) => {
    let count = 0;
    fs.createReadStream(filePath)
      .pipe(csv({ mapHeaders: ({ header }) => header.trim() }))
      .on("data", () => count++)
      .on("end", () => resolve(count))
      .on("error", reject);
  });
}

async function* getAllPagedData(filePath, itemsPerPage) {
  const pipeline = fs
    .createReadStream(filePath)
    .pipe(csv({ mapHeaders: ({ header }) => header.trim() }));

  let page = [];

  for await (const value of pipeline) {
    page.push(value);
    if (page.length === itemsPerPage) {
      yield page;
      page = [];
    }
  }

  if (page.length > 0) yield page;
}

async function main() {
  const postgresDB = await getPostgresConnection();

  await postgresDB.car_gov.deleteAll();

  const total = await countCsvLines(FILE_PATH);
  console.log(`total items in CSV: ${total}`);

  const progress = new cliProgress.SingleBar(
    {
      format: "progress [{bar}] {percentage}% | {value}/{total} | {duration}s",
      clearOnComplete: false,
    },
    cliProgress.Presets.shades_classic
  );
  progress.start(total, 0);

  let totalProcessed = 0;

  const cluster = initialize({
    backgroundTaskFile: TASK_FILE,
    clusterSize: CLUSTER_SIZE,
  });

  await setTimeout(1000);

  const inFlight = new Set();

  try {
    for await (const page of getAllPagedData(FILE_PATH, ITEMS_PER_PAGE)) {
      while (inFlight.size >= MAX_IN_FLIGHT_BATCHES) {
        await Promise.race(inFlight);
      }

      const promise = cluster
        .sendToChild(page)
        .then((result) => {
          const processed = Number(result.processedCount ?? 0);
          totalProcessed += processed;
          progress.update(totalProcessed);
          if (result.error) {
            console.error("batch error:", result.error);
          }
          return result;
        })
        .catch((err) => {
          console.error("Batch failed:", err);
          throw err;
        })
        .finally(() => {
          inFlight.delete(promise);
        });

      inFlight.add(promise);
    }

    await Promise.all(inFlight);

    progress.stop();

    const insertedOnPostgres = await postgresDB.car_gov.count();
    console.log(
      `done. total in CSV ${total} and total in Postgres ${insertedOnPostgres}`
    );
  } catch (err) {
    console.error("Fatal error in master:", err);
  } finally {
    cluster.killAll();
    process.exit(0);
  }
}

await main();
