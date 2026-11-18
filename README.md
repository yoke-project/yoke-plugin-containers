# containers

A Yoke Plugin that reads and drives the host's container engine — Podman or Docker, over its API
socket: the containers and their state, the images, a container's output and the engine's own record of
it; starting, stopping, restarting, pausing, killing and removing a container, pulling and removing an
image.

| | |
| --- | --- |
| questions | `containers.status` (the engine reached or not, and its counts, with its sequence), `containers.list` (every container, with its sequence), `containers.images` (every image, with its sequence), `containers.inspect` `{id, kind?: "container" \| "image"}`, `containers.logs` `{id, tail?}`, `containers.permissions` |
| commands | `containers.start`, `containers.pause`, `containers.unpause`, `containers.kill` `{id}`; `containers.stop`, `containers.restart` `{id, timeout?}`; `containers.remove` `{id, force?, volumes?}`; `containers.image.pull` `{reference}`; `containers.image.remove` `{id, force?}` |
| streams | `containers.status`: every change, none lost; `containers.list` and `containers.images`: the whole list at every change |
| occurrences | `containers.container.started` (10), `containers.container.exited` (10 with 0, 30 otherwise), `containers.container.unhealthy` (50), `containers.container.oom` (70), `containers.job.finished` when a command acknowledged as still running ends |

Every payload is JSON and states `schema`; a question with no fields takes `{}`. A container is named by
its id, a prefix of it, or its name; an image by its id or a tag. Every occurrence carries its facts as
JSON in its detail — `line`, and the container, its image, its exit code — which is what an interface
client receives. A list too long for one payload is cut, and says `"truncated": true`.

**Changes are the engine's own events.** The Plugin keeps the engine's event stream open and reads the
containers or the images again when an event says they changed; nothing is read again at an interval.
An engine that goes away is looked for again, and `containers.status` says why it is not reached.

**Every stream carries a `sequence`**, counted per stream from the unit's start, and each of the three
questions states the sequence of the last record it reflects. **An interface that attaches late**
subscribes to a stream, then asks the question of the same name, and drops what the stream brings at or
below the sequence it was answered with.

**The engine's place** is the first that answers of `/run/user/<uid>/podman/podman.sock` (rootless
Podman, its socket service running), `/run/podman/podman.sock` and `/var/run/docker.sock`, or the ones
named with `--socket <path>` among the unit's arguments. A unit is handed none of the host's environment,
so `DOCKER_HOST` and `CONTAINER_HOST` are not read. One unit follows one engine; an application that
wants two runs two units.

**Reading is never limited; changing is.** The application names the containers commands may act on with
`--allow <pattern>` and `--protect <pattern>` among the unit's arguments, matched against a container's
names and id, and the images with `--allow-image <pattern>` and `--protect-image <pattern>`, matched
against an image's tags and id, `*` matching any run of characters. **A container labelled
`dev.yoke-project.*` is one Yoke runs, and is always protected**, with the images such containers run: the
Plugin never stops, kills or removes the Core's own units. A container's and an image's record says
whether commands refuse it, as `protected`.

Nothing is granted by declaring it: an operator grants the capabilities the Manifest names, one for each
question, command, stream and occurrence, and the socket still decides what the account may do —
`containers.permissions` says what it allows. An account that reaches Docker's socket through the
`docker` group is as much as root on the host. `containers.inspect` gives the engine's record as it is,
the container's environment in it. A pull carries no registry credentials.

Take it into an application with `yoke-compose add plugin https://github.com/yoke-project/yoke-plugin-containers@<tag>`.
Build it with `yoke-compose build`, and try it from `try/`.

Licensed under the Apache License 2.0.
