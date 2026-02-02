import winston from 'winston';

let logger;

const customFormat = winston.format.printf(({ level, message, timestamp, ...meta }) => {
    let metaStr = '';

    // We will just join all remaining meta keys
    if (Object.keys(meta).length > 0) {
        const parts = [];
        for (const [key, value] of Object.entries(meta)) {
            // Check if value is object? usage in main.js seems to be primitives usually.
            parts.push(`${key}=${value}`);
        }
        if (parts.length > 0) {
            metaStr = ` | ${parts.join(' ')}`;
        }
    }

    return `${timestamp} [${level.toUpperCase()}]: ${message}${metaStr}`;
});

export const InitLogger = () => {
    logger = winston.createLogger({
        level: 'info',
        format: winston.format.combine(
            winston.format.timestamp({
                format: 'Mon, 02 Jan 2006 15:04:05', // Helper for humans, winston uses fecha. 
                // Actual correct pattern for "Mon, 02 Feb 2026 15:35:48":
                // ddd, DD MMM YYYY HH:mm:ss
                format: 'ddd, DD MMM YYYY HH:mm:ss'
            }),
            customFormat
        ),
        transports: [
            new winston.transports.Console()
        ]
    });
};

export const GetLogger = () => {
    if (!logger) {
        InitLogger();
    }
    return logger;
};

// Singleton alias for convenience if needed, but we follow Go pattern of GetLogger() or just global logger
export const Logger = {
    info: (msg, meta) => GetLogger().info(msg, meta),
    error: (msg, meta) => GetLogger().error(msg, meta),
    warn: (msg, meta) => GetLogger().warn(msg, meta),
};
