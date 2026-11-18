# Trying containers

This directory is an application project that uses the Plugin in `..`: `yoke-compose init` made it and
`add plugin .. --yes` took the Plugin into it. Nothing here is the Plugin, and taking the Plugin into
another application never takes this directory.

| Path | What it is |
| --- | --- |
| `yoke.yaml` | the application: the unit `containers` and the channel `panel`, a local socket for one client |
| `ui/` | the panel: an interface client written with Slint and the Rust interface library. It shows the engine, every container with its state and every image, starts, stops, restarts and removes a container, pulls and removes an image, and says at once when a container turns unhealthy. It reaches the Plugin through the contract alone and links none of its code |
| `follow/` | an interface client: it asks every question, follows every stream and every event of the unit, and prints each as one line |
| `run.sh` | runs the application on a local Core, grants what the Plugin declares, and opens the panel, or follows the unit with `--follow` |

```sh
./run.sh                        # the panel; closing it stops everything
./run.sh --read-only            # no command granted: the panel reads, and the Core refuses every change
./run.sh --follow --seconds 10  # one line per answer, frame and event, in place of the panel
```

The Core and its programs come from the module proxy, `yoke-core` among them:

    go install github.com/yoke-project/yoke/cmd/{yoke,yoke-compose,yoke-core,yoke-guardian,yoke-auth}@latest

Rootless Podman serves its socket only while its socket service runs: `systemctl --user start
podman.socket`. To keep commands off every container but a few, give the unit its arguments before
running, for instance:

    yoke-compose set units.containers.args '["--allow", "try-*", "--allow-image", "docker.io/library/alpine:*"]'

Start a container from another terminal while it runs to see `containers.list` move and
`containers.container.started` reported. With `--follow`, a command goes from another terminal, as
`run.sh` says:

    echo '{"id":"try-web"}' | YOKE_INSTANCE=<instance> yoke unit command containers containers.stop --input -
