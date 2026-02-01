export const Logger = {
    info: (message, meta = {}) => {
        console.log(`[INFO] ${message}`, Object.keys(meta).length ? JSON.stringify(meta) : '');
    },
    error: (message, meta = {}) => {
        console.error(`[ERROR] ${message}`, Object.keys(meta).length ? JSON.stringify(meta) : '');
    },
    warn: (message, meta = {}) => {
        console.warn(`[WARN] ${message}`, Object.keys(meta).length ? JSON.stringify(meta) : '');
    }
};
