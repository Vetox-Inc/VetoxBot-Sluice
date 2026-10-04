#!/usr/bin/env node
'use strict';

const { spawn } = require('node:child_process');
const { binaryPath } = require('..');

const child = spawn(binaryPath, process.argv.slice(2), { stdio: 'inherit' });
// The binary finishes the requests in flight before it exits, and the launcher waits for it.
// On Windows a console signal reaches the binary by itself, and kill() would end it at once.
const forwarded = ['SIGINT', 'SIGTERM', 'SIGHUP'];
for (const signal of forwarded) {
  process.on(signal, () => {
    if (process.platform !== 'win32' && child.exitCode === null) child.kill(signal);
  });
}
child.on('error', (error) => {
  console.error(error.message);
  process.exit(1);
});
child.on('exit', (code, signal) => {
  if (signal) {
    for (const name of forwarded) process.removeAllListeners(name);
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 1);
});
