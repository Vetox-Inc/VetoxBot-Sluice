package proxy

// Version is set at build time with
// -ldflags "-X github.com/Vetox-Inc/VetoxBot-Sluice/internal/proxy.Version=<version>".
var Version = "dev"

func userAgent() string {
	return "DiscordBot (https://github.com/Vetox-Inc/VetoxBot-Sluice, " + Version + ")"
}
