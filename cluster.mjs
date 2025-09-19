import { fork } from "child_process";

function roundRobin(array) {
  let index = 0;
  return function () {
    if (index >= array.length) index = 0;
    return array[index++];
  };
}

export function initialize({ backgroundTaskFile, clusterSize = 2 }) {
  const processes = [];
  const pending = new Map();
  const idToChild = new Map();

  for (let i = 0; i < clusterSize; i++) {
    const child = fork(backgroundTaskFile);

    child.on("exit", (code, signal) => {
      // rejeita promessas pendentes atribuídas a esse child
      for (const [id, entry] of pending.entries()) {
        if (entry.child === child) {
          entry.reject(
            new Error(
              `child (pid=${child.pid}) exited before finishing id=${id}`
            )
          );
          pending.delete(id);
          idToChild.delete(id);
        }
      }
      // remove da lista
      const idx = processes.indexOf(child);
      if (idx >= 0) processes.splice(idx, 1);
    });

    child.on("error", (err) => {
      console.error("child process error:", err);
    });

    child.on("message", (msg) => {
      if (!msg || typeof msg !== "object") return;
      const { id, processedCount, error } = msg;
      if (!id) return;
      const entry = pending.get(id);
      if (!entry) return;
      if (error) entry.reject(new Error(error));
      else entry.resolve({ id, processedCount });
      pending.delete(id);
      idToChild.delete(id);
    });

    processes.push(child);
  }

  const getProcess = roundRobin(processes);
  let currentId = 1;

  function sendToChild(items) {
    return new Promise((resolve, reject) => {
      if (processes.length === 0)
        return reject(new Error("no child processes available"));

      const id = String(currentId++);
      const child = getProcess();

      if (!child || !child.connected) {
        return reject(new Error("chosen child is not connected"));
      }

      pending.set(id, { resolve, reject, child });
      idToChild.set(id, child);

      try {
        child.send({ id, items }, (err) => {
          if (err) {
            if (pending.has(id)) {
              pending.get(id).reject(err);
              pending.delete(id);
              idToChild.delete(id);
            }
          }
        });
      } catch (err) {
        if (pending.has(id)) {
          pending.get(id).reject(err);
          pending.delete(id);
          idToChild.delete(id);
        }
      }

      const TIMEOUT_MS = Number(process.env.BATCH_TIMEOUT_MS || 5 * 60 * 1000);
      const timer = setTimeout(() => {
        if (pending.has(id)) {
          pending.get(id).reject(new Error("batch timed out"));
          pending.delete(id);
          idToChild.delete(id);
        }
      }, TIMEOUT_MS);

      // wrap resolve/reject para limpar timer
      const orig = pending.get(id);
      pending.set(id, {
        child,
        resolve: (res) => {
          clearTimeout(timer);
          resolve(res);
        },
        reject: (err) => {
          clearTimeout(timer);
          reject(err);
        },
      });
    });
  }

  function killAll() {
    processes.forEach((c) => {
      try {
        c.kill();
      } catch (_) {}
    });
  }

  return { sendToChild, killAll };
}
