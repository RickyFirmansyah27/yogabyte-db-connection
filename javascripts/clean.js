import { InitLogger, Logger } from './src/logger.js';
import { Connect, CloseDB, ExecuteSQLWithParams } from './src/database.js';

const main = async () => {
    try {
        InitLogger();
        await Connect();

        Logger.info("Starting database cleanup...");

        await ExecuteSQLWithParams("DROP TABLE IF EXISTS DemoAccount");

        Logger.info("Successfully dropped table DemoAccount.");
        Logger.info("Cleanup complete.");

        await CloseDB();
    } catch (err) {
        Logger.error("Error during cleanup:", { error: err.message });
    }
};

await main();
