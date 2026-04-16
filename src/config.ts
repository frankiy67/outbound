import * as fs from 'fs';

export const DATA_DIR = 'D:\\outbound-data';

// Ensure the data directory exists at import time.
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}
