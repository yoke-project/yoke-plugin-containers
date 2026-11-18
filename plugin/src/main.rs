//! containers: the host's container engine — its containers and its images — as a Yoke Plugin unit.
//!
//! The unit's arguments: `--socket <path>` names the engine's socket, as many times as there are places
//! to try, in place of the fixed ones; `--allow`, `--protect`, `--allow-image` and `--protect-image`
//! name what commands may act on.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use containers::backend::{default_places, Socket};
use containers::plugin::{declaration, handle, on_said, Containers, Dirty, Policy};
use tokio::sync::broadcast::error::RecvError;
use yoke_sdk::plugin::{start, Event};

/// The engine says a few things at once for one change — a container created, attached, started: the
/// Plugin reads again once they have settled.
const SETTLE: Duration = Duration::from_millis(200);

fn places(args: &[String]) -> Vec<PathBuf> {
    let named: Vec<PathBuf> =
        args.windows(2).filter(|w| w[0] == "--socket").map(|w| PathBuf::from(&w[1])).collect();
    if named.is_empty() {
        default_places()
    } else {
        named
    }
}

#[tokio::main]
async fn main() {
    // `containers --manifest` prints the Manifest the declaration generates, which the bundle carries.
    if std::env::args().nth(1).as_deref() == Some("--manifest") {
        print!("{}", declaration().manifest());
        return;
    }
    let args: Vec<String> = std::env::args().skip(1).collect();
    let unit = match start(&declaration()).await {
        Ok(unit) => Arc::new(unit),
        Err(err) => {
            eprintln!("containers: {err}");
            std::process::exit(1);
        }
    };
    let (socket, mut said) = Socket::start(places(&args));
    let plugin = Containers::new(socket, Policy::from_args(args));
    if let Err(err) = unit.health(50, "looking for the container engine").await {
        eprintln!("containers: {err}");
    }
    let mut dirty = Dirty::default();
    let settle = tokio::time::sleep(Duration::from_secs(3600));
    tokio::pin!(settle);
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("containers listens for SIGTERM");
    loop {
        tokio::select! {
            event = unit.next() => match event {
                None | Some(Event::Ended(_)) => return,
                Some(Event::Activated(a)) => {
                    if let Err(err) = plugin.activated(unit.as_ref(), &a.stream).await {
                        eprintln!("containers: {err}");
                    }
                }
                Some(Event::Stopped { stream }) => plugin.stopped(&stream),
                // Each on its own task: a container that takes its time to stop holds nothing else.
                Some(event) => {
                    let (unit, plugin) = (unit.clone(), plugin.clone());
                    tokio::spawn(async move {
                        if let Err(err) = handle(unit.as_ref(), &event, &plugin).await {
                            eprintln!("containers: {err}");
                        }
                    });
                }
            },
            said = said.recv() => match said {
                Ok(said) => {
                    let (more, r) = on_said(unit.as_ref(), &said, &plugin).await;
                    if let Err(err) = r {
                        eprintln!("containers: {err}");
                    }
                    if more.any() && !dirty.any() {
                        settle.as_mut().reset(tokio::time::Instant::now() + SETTLE);
                    }
                    dirty = dirty.or(more);
                }
                // Too much at once: whatever was missed is read again.
                Err(RecvError::Lagged(_)) => {
                    if !dirty.any() {
                        settle.as_mut().reset(tokio::time::Instant::now() + SETTLE);
                    }
                    dirty = Dirty::ALL;
                }
                Err(RecvError::Closed) => return,
            },
            _ = &mut settle, if dirty.any() => {
                if let Err(err) = plugin.refresh(unit.as_ref(), std::mem::take(&mut dirty)).await {
                    eprintln!("containers: {err}");
                }
            },
            _ = term.recv() => {
                let _ = unit.close().await;
                while let Some(event) = unit.next().await {
                    if matches!(event, Event::Ended(_)) {
                        break;
                    }
                }
                return;
            }
        }
    }
}
