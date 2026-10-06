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
3. Select **Mynah** and press **Enter** to open its page. It lists Mynah's
   personas ("instances"), starting with the first one, plus every channel
   they answer in. Press **Enter** on an instance for its settings:
   - **AI endpoint URL** and **Model**: e.g.
     `https://api.openai.com/v1/chat/completions` and `gpt-4o-mini`. Left
     empty, Mynah echoes, which is handy for checking it works.
   - **API key**: stored encrypted; only Mynah ever sees it.
   - **Persona**: who it is and how it answers (its system prompt).
   - **Answer @mentions**, **Where @mentions work** and **Mention name**: see
     below.
4. On the same page, choose **+ New channel** (or create one in **Server
   Settings → Channels** with **Mynah** as its type). Under **Configure…** pick
   which instance answers there, plus a per-member rate limit. It answers
   every message posted in that channel.

### Where each persona answers

Each instance can have channels of its own, answer @mentions, or both:

| You want | Its own channel | Answer @mentions |
|---|---|---|
| A channel just for it | yes | off |
| @mentions only | none | on |
| Both (hybrid) | yes | on |

**Where @mentions work** picks the channels: one, a handful, or none picked
for every channel. People mention it by name (`@Alice what's the capital of
Peru?`); set **Mention name** to use a different word.

Replies are queued and answered one at a time, so a single local model is
never asked to generate two at once. While it thinks, the channel shows
"Mynah is typing"; then the reply appears and grows.

To update later: select it in **Server Settings → Plugins**, press **U**, then
Enter.

### Several personas on one server

On Mynah's page, **+ Add instance** asks for a name and starts another
persona (say "Burt" beside "Alice"). It runs separately, with its own
settings and channels. **N** renames one, **R** restarts it, **T** turns it off
and on, and **X** (twice) removes it. Its channels and settings are kept, so
adding it again with the same name brings them back. Everywhere in chat it
shows as its own name; Mynah is only the plugin's name. Updating Mynah
updates every instance.

Instances need a Concord server from 2026-09-30 or later. Before that,
each extra persona was a hand-made copy of the plugin folder with its own
`[plugin].id`. In **Settings → Plugins**, select such a copy and press **M** to
turn it into an instance. It keeps its account, channels and settings,
apart from the API key, which you enter again.

## Develop

```sh
go run .              # chat standalone (echo mode without -endpoint)
go test ./...         # tests, including a fake Concord server (sdk/plugintest)
go run release.go     # the release zips, in dist/
```

Release a version by pushing a tag: `git tag v0.3.0 && git push --tags`.

- `internal/gateway`: the OpenAI-compatible client (streamed server-sent
  events, or one JSON reply from servers that don't stream).
- `internal/relay`: the plugin: channels, @mentions, rate limits, the reply
  queue, and streaming replies with the Concord SDK's `Conn.Stream`.
- `internal/ratelimit`: per-member token buckets.

Installs from before v0.2.0 kept the API key in `plugin.toml`
(`GATEWAY_API_KEY`) and the persona in a file (`PERSONA_DOC_PATH`); both still
work, and the settings above take precedence.

## License

MIT License — see LICENSE file for details.
