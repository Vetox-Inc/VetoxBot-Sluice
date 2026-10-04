# @vetox-bot/sluice

[Sluice](https://github.com/Vetox-Inc/VetoxBot-Sluice) is a rate-limit proxy for Discord's REST API. Bots send their
requests to it instead of to Discord, and it keeps them inside Discord's limits however many processes share a token.
This package installs the prebuilt binary for your platform: it needs no Go toolchain and runs no install script.

```sh
npx @vetox-bot/sluice --version
PORT=8080 npx @vetox-bot/sluice
```

Point your library at it, for example discord.js: `new Client({ rest: { api: 'http://127.0.0.1:8080/api' } })`.

To run the binary without Node in front of it (under PM2 or systemd), use its path:

```js
const { binaryPath } = require('@vetox-bot/sluice');
```

On `SIGTERM`, Sluice lets requests in flight finish for up to 15 seconds, so give it 20 to stop, for example
`kill_timeout: 20000` under PM2.

Configuration, behaviour, metrics and benchmarks are documented in the
[repository](https://github.com/Vetox-Inc/VetoxBot-Sluice#readme). Sluice runs as its own process, and your bot talks
to it over HTTP. It is licensed under GPL-3.0 and derives from nirn-proxy.
