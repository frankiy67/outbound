import * as fs from 'fs';
import * as path from 'path';
import os from 'os';

export const DATA_DIR = process.env.DATA_DIR ||
  (process.platform === 'win32'
    ? 'D:\\outbound-data'
    : path.join(os.homedir(), 'outbound-data'));

// Ensure the data directory exists at import time.
if (!fs.existsSync(DATA_DIR)) {
  fs.mkdirSync(DATA_DIR, { recursive: true });
}
