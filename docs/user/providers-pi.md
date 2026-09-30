# Pi

Pi is an opt-in provider. Install Pi **0.84.1 or newer on the machine running your T3 Code environment**, then configure a subscription or API key with Pi before adding it in **Settings → Providers**. An empty binary path uses `pi` from that machine's `PATH`.

T3 Code discovers Pi's configured models and their thinking levels. It uses Pi's existing configuration and credentials; it does not copy them into T3 Code. Available models indicate configured credentials, not that a remote API key has been validated.

## Accounts and sessions

Add separate Pi instances for different accounts. Set `PI_CODING_AGENT_DIR` in each instance's environment variables to choose its configuration directory. `PI_CODING_AGENT_SESSION_DIR` optionally overrides where Pi saves conversations. Usage includes these configured session directories, including disabled accounts with retained history.

Pi conversations resume from their saved session files after restarting T3 Code. Keep those files: if one is missing, T3 Code reports it rather than silently starting an empty replacement conversation.

## Working with Pi

- Models, thinking traces, images, tool activity, approvals and extension questions use the normal chat controls on web, desktop and mobile.
- Pi's extension commands, prompt templates and skills are discovered for the project's working directory. Built-in terminal-only commands such as `/login` and `/settings` must be run in Pi itself.
- A message sent as a steer reaches the current Pi run. Queued follow-ups wait in T3 Code until that run finishes, including any automatic retries or compaction.
- Manual compaction uses Pi's native compaction. Pi controls automatic compaction through its own settings.

Pi has no native permission gate. T3 Code supplies one for permission modes that require approval, and refuses to start a gated session if the extension cannot load. Full access runs without this gate. The gate controls tool calls, not the code executed by other installed extensions; only install extensions you trust.

## Background work

Recognized `subagent` extension results appear as agent activity. Jobs from `bg` / `bg_status` are background shell work, not native subagents. They remain active after Pi finishes replying and update when Pi receives a completion notification or checks their status.

Runs from the `pi-dynamic-workflows` package appear as workflows in the Agents panel, with each agent listed under its phase. A background run stays active after Pi replies. Its progress updates each time one of its agents finishes, and its result shows in the chat when Pi receives it. Pausing a run ends its entry; resuming it brings the entry back.

**Stop** also stops the Pi process when background jobs or workflow runs are active, allowing its extensions to clean up. The next message resumes the saved conversation. To stop just one job, ask Pi to kill that job with `bg_status`.

## Limitations

Pi does not expose native plan mode. Use normal mode and tool approvals to control changes.

**Edit from here** forks the Pi session just before the chosen message, so the original session file stays on disk. It works for messages sent with this version of T3 Code or later; earlier messages cannot be rewound to.

Extensions that require Pi's terminal UI, custom widgets or a custom editor do not have that UI in T3 Code. Standard confirmation, selection and text-input dialogs are supported.
