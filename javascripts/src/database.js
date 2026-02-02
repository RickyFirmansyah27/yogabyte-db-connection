import pg from 'pg';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import { GetLogger } from './logger.js';

const { Pool } = pg;
const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Load .env from project root
dotenv.config({ path: path.join(__dirname, '../.env') });

let pool;

export const InitDB = async () => {
    const logger = GetLogger();

    const dbConfig = {
        host: process.env.DB_HOST,
        port: process.env.DB_PORT,
        database: process.env.DB_NAME,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        ssl: {
            rejectUnauthorized: true,
            ca: fs.readFileSync(path.join(__dirname, '../config/root.crt')).toString(),
        },
        connectionTimeoutMillis: 30000,
        max: 25, // Max connections (Go: 25)
        min: 5,  // Min connections (Go: 5)
        idleTimeoutMillis: 30 * 60 * 1000, // 30 min
    };

    pool = new Pool(dbConfig);

    // Initial connection test
    try {
        const client = await pool.connect();
        const result = await client.query('SELECT version()');
        client.release();

        logger.info("Database connection successful", {
            version: result.rows[0].version,
            max_conns: dbConfig.max,
            min_conns: dbConfig.min
        });
    } catch (err) {
        logger.error("Failed to connect to database", { error: err.message });
        throw err;
    }
};

export const GetPool = () => pool;

export const Connect = InitDB; // Alias

export const CloseDB = async () => {
    if (pool) {
        await pool.end();
    }
};

const maskSensitiveParams = (params) => {
    if (!params) return [];
    return params.map(p => {
        if (typeof p === 'string' && p.length > 8) {
            return p.substring(0, 3) + '***';
        }
        return p;
    });
};

export const ExecuteSQLWithParams = async (sql, params = []) => {
    const logger = GetLogger();
    logger.info("Executing SQL Query", { query: sql, params: maskSensitiveParams(params) }); // Matches Go format logic

    const client = await pool.connect();
    try {
        const result = await client.query(sql, params);
        // In Go, we returned rows. In JS pg, result.rows is the data array.
        // To strictly match "rows" object concept, we return the result object which has .rows
        // But the consuming code might expect just the array of rows if adapted from previous JS.
        // However, the previous JS returned result.rows.
        // The Go code returns `pgx.Rows` which needs closing.
        // JS `pg` uses `client` which needs releasing.
        // If we return just data, we release client here.
        // The Go refactor `ExecuteSQLWithParams` returns `rows, err` and expects user to close rows?
        // Wait, `pgxpool.Query` returns rows that must be closed.
        // In JS `pg`, `pool.query` is auto-managed? No, we used `client = pool.connect()`.
        // If we use `pool.query()`, it handles release automatically.
        // Let's use `pool.query()` for simplest non-transactional execution, which is "Connection Pooling" best practice for single queries.

        // However, the Go code returns `rows` to iterate.
        // JS `pg` `pool.query` returns a Result object with all rows already fetched (buffered).
        // So we can return it.

        return result;
    } catch (err) {
        logger.error("Query Failed", { error: err.message });
        throw err;
    } finally {
        client.release();
    }
};

// For transactions
export const StartTransaction = async () => {
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        return client;
    } catch (err) {
        client.release();
        throw err;
    }
};

export const ExecuteSQLTransaction = async (client, sql, params = []) => {
    // client is the 'tx'
    try {
        const result = await client.query(sql, params);
        return result;
    } catch (err) {
        throw err;
    }
};

export const CommitTransaction = async (client) => {
    try {
        await client.query('COMMIT');
    } finally {
        client.release();
    }
};

export const RollbackTransaction = async (client) => {
    try {
        await client.query('ROLLBACK');
    } finally {
        client.release();
    }
};
