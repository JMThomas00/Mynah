# Mynah

An AI persona for [Concord](https://github.com/JMThomas00/Concord) channels.
Mynah answers with any OpenAI-compatible model (OpenAI, OpenRouter, a local
server such as Ollama or LM Studio, Hermes' API server...) and streams each
reply into the channel as it's written. Markdown renders as it arrives:
code blocks, lists, bold and links.

## Chat with it on your own computer

**Download:** from the [Releases](https://github.com/JMThomas00/Mynah/releases)
page, get the zip for your system (`mynah_windows_amd64.zip`,
`mynah_darwin_arm64.zip` for Apple silicon, `mynah_linux_amd64.zip`, ...),
unzip it, and run it from a terminal, pointed at your model:

```sh
./mynah -endpoint http://127.0.0.1:11434/v1/chat/completions -model llama3.2
# Windows: .\mynah.exe -endpoint ... -model ...
```

Add `-key <API key>` for a hosted service, and `-persona "You are..."` to give
it a personality. With no `-endpoint` it just echoes. On macOS, if it's blocked
as being from an unidentified developer, run `xattr -d com.apple.quarantine mynah`
once. **Or with Go installed:** `go install github.com/JMThomas00/mynah@latest`,
then run `mynah`.

## Use it on a Concord server

You need to be the server owner, or have the **Manage Plugins** permission.

1. In Concord, open **Server Settings → Plugins** and press **I** (install).
2. Type `JMThomas00/Mynah` and press Enter. Concord downloads the release for
   the server's system, verifies it and starts it.
3. Select **Mynah** and press **Enter** for its settings:
   - **AI endpoint URL** and **Model**: e.g.
     `https://api.openai.com/v1/chat/completions` and `gpt-4o-mini`. Left
     empty, Mynah echoes, which is handy for checking it works.
   - **API key**: stored encrypted; only Mynah ever sees it.
   - **Persona**: who Mynah is and how it answers (its system prompt).
   - **Answer @mentions in any channel** and the **trigger word**: with these on,
     `@mynah what's the capital of Peru?` works in any channel.
4. Open **Server Settings → Channels**, create a channel, and choose **Mynah** as
   its type. Mynah answers every message posted there. The channel's
   options set a per-member rate limit.

Replies are queued and answered one at a time, so a single local model is
never asked to generate two at once. While it thinks, the channel shows
"Mynah is typing"; then the reply appears and grows.

To update later: select it in **Server Settings → Plugins**, press **U**, then
Enter.

### Several personas on one server

Each install is one persona, and installing from Settings > Plugins gives
you one Mynah per server. A second persona (say "Burt" beside "Alice") needs
a second copy of the plugin folder with a different `[plugin].id` in its
`plugin.toml`, placed by hand and then picked up with **S** (rescan); each
copy gets its own settings, persona and channels, grouped under Mynah in
Settings > Plugins. Installing extra personas from the UI isn't supported
yet.

## Develop

```sh
go run .              # chat standalone (echo mode without -endpoint)
go test ./...         # tests, including a fake Concord server (sdk/plugintest)
go run release.go     # the release zips, in dist/
```

Release a version by pushing a tag: `git tag v0.2.0 && git push --tags`.

- `internal/gateway`: the OpenAI-compatible client (streamed server-sent
  events, or one JSON reply from servers that don't stream).
- `internal/relay`: the plugin: channels, @mentions, rate limits, the reply
  queue, and streaming replies with the Concord SDK's `Conn.Stream`.
- `internal/ratelimit`: per-member token buckets.

Installs from before v0.2.0 kept the API key in `plugin.toml`
(`GATEWAY_API_KEY`) and the persona in a file (`PERSONA_DOC_PATH`); both still
work, and the settings above take precedence.
