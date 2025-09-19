import pg from "pg";
const { Pool } = pg;

const DEFAULT_POOL_MAX = Number(process.env.DB_POOL_MAX || 1);

async function getPostgresConnection() {
  const pool = new Pool({
    user: process.env.DB_USER || "admin",
    host: process.env.DB_HOST || "localhost",
    database: process.env.DB_NAME || "postgres",
    password: process.env.DB_PASS || "admin",
    port: Number(process.env.DB_PORT || 5432),
    max: DEFAULT_POOL_MAX,
  });

  await pool.query("SELECT 1");

  const COLUMNS = [
    "registro_car",
    "uf",
    "municipio",
    "codigo_ibge",
    "area_do_imovel",
    "situacao_cadastro",
    "condicao_cadastro",
    "latitude",
    "longitude",
    "tipo_imovel_rural",
    "modulos_fiscais",
    "origem",
    "descricao",
    "data_processamento",
    "area_de_conflito",
    "percentual",
  ];

  const shutdown = async () => {
    try {
      await pool.end();
    } catch (_) {}
  };
  process.once("exit", shutdown);
  process.once("SIGINT", () => {
    shutdown().then(() => process.exit(0));
  });

  return {
    pool,
    car_gov: {
      async insert(item) {
        const values = COLUMNS.map((c) => item[c] ?? null);
        const placeholders = COLUMNS.map((_, i) => `$${i + 1}`).join(", ");
        const query = `INSERT INTO car_gov (${COLUMNS.join(
          ", "
        )}) VALUES (${placeholders})`;
        await pool.query(query, values);
      },

      async insertMany(items) {
        if (!Array.isArray(items) || items.length === 0) return 0;
        const cols = COLUMNS;
        const values = [];
        const rowPlaceholders = items
          .map((row) => {
            const placeholders = cols.map((col) => {
              values.push(row[col] ?? null);
              return `$${values.length}`;
            });
            return `(${placeholders.join(",")})`;
          })
          .join(",");

        const sql = `INSERT INTO car_gov (${cols.join(
          ","
        )}) VALUES ${rowPlaceholders}`;
        await pool.query(sql, values);
        return items.length;
      },

      async count() {
        const r = await pool.query("SELECT COUNT(*) as total FROM car_gov");
        return Number(r.rows[0].total);
      },

      async deleteAll() {
        await pool.query("TRUNCATE TABLE car_gov");
      },

      async createTable() {
        const createStudentsTableQuery = `
          CREATE TABLE IF NOT EXISTS car_gov (
            id SERIAL PRIMARY KEY,
            registro_car TEXT NOT NULL, uf TEXT NOT NULL, municipio TEXT NOT NULL,
            codigo_ibge TEXT NOT NULL, area_do_imovel TEXT, situacao_cadastro TEXT,
            condicao_cadastro TEXT, latitude TEXT, longitude TEXT, tipo_imovel_rural TEXT,
            modulos_fiscais TEXT, origem TEXT, descricao TEXT, data_processamento TEXT,
            area_de_conflito TEXT, percentual TEXT
          );`;
        await pool.query(createStudentsTableQuery);
      },
    },
  };
}

export { getPostgresConnection };
