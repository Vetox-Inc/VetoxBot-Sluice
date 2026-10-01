# @vetox-bot/sluice

[Sluice](https://github.com/Vetox-Inc/VetoxBot-Sluice) is a Discord REST rate-limit proxy, the maintained successor
to [nirn-proxy](https://github.com/germanoeich/nirn-proxy) by Germano Eichenberg, built on Melonly-Moderation's rewrite
of its engine. This package installs its prebuilt binary for your platform; no Go toolchain and no install script are
needed.

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

Configuration, behaviour and metrics are documented in the
[repository](https://github.com/Vetox-Inc/VetoxBot-Sluice#readme). Licensed under GPL-3.0; Sluice runs as its own
process and your bot talks to it over HTTP.
