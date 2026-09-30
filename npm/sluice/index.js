'use strict';

// Resolves the binary that npm installed from the matching platform package.
function resolveBinary() {
  const platform = `${process.platform}-${process.arch}`;
  const executable = process.platform === 'win32' ? 'sluice.exe' : 'sluice';
  try {
    return require.resolve(`@vetox-bot/sluice-${platform}/bin/${executable}`);
  } catch {
    throw new Error(
      `@vetox-bot/sluice has no binary for ${platform}. Prebuilt binaries cover linux, darwin and win32 on x64 and arm64; ` +
        'elsewhere use the container image or build from source.',
    );
  }
}

Object.defineProperty(exports, 'binaryPath', { enumerable: true, get: resolveBinary });
