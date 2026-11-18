//! Bridges the window and the containers Plugin, through the interface contract alone.
//!
//! - **Reading.** `containers.status`, `containers.list` and `containers.images` are streams with a
//!   sequence: the panel subscribes to each first, then asks it, and drops every frame at or below the
//!   sequence it was answered with. A list frame is the whole list, and replaces it.
//! - **Acting.** A button is a command — start, stop, restart, remove a container; pull, remove an image;
//!   its acknowledgement is the message at the top. What commands refuse is shown as protected, with no
//!   button. A command still running is accepted, and `containers.job.finished` says how it ended.
//! - **The events** are the Core's events about the unit: its state, its health, the occurrences it
//!   reports — a container started, exited, unhealthy.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use slint::{ComponentHandle, ModelRc, SharedString, VecModel, Weak};
use tokio::runtime::Handle;
use tokio::time::{interval, sleep};
use yoke_proto::interface::v1::subscribed::Carries;
use yoke_sdk::interface::{Attachment, Filter, Options};

use crate::{ContainerRow, ImageRow, MainWindow};

const STATUS: &str = "containers.status";
const LIST: &str = "containers.list";
const IMAGES: &str = "containers.images";
const MAX_EVENTS: usize = 300;
const EVENTS_REDRAW: Duration = Duration::from_millis(500);
const AGAIN: Duration = Duration::from_secs(2);

#[derive(Default)]
struct Log {
    lines: VecDeque<String>,
    dirty: bool,
}

pub struct Shared {
    unit: String,
    attachment: OnceLock<Arc<Attachment>>,
    started: Mutex<Vec<String>>,
}

#[derive(Clone)]
struct Ctx {
    at: Arc<Attachment>,
    unit: String,
    ui: Weak<MainWindow>,
    log: Arc<Mutex<Log>>,
    t0: Instant,
    shared: Arc<Shared>,
}

fn say(ui: &Weak<MainWindow>, msg: impl Into<String>) {
    let m = msg.into();
    let _ = ui.upgrade_in_event_loop(move |u| u.set_message(m.into()));
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or("").to_string()
}

fn bytes(n: i64) -> String {
    const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let (mut v, mut i) = (n.max(0) as f64, 0);
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 { format!("{n} B") } else { format!("{v:.1} {}", U[i]) }
}

/// Runs a command on the runtime, once attached, and says how it went.
fn on_command(rt: &Handle, ready: &Arc<OnceLock<Ctx>>, weak: &Weak<MainWindow>, r#type: &'static str, payload: Value) {
    match ready.get() {
        Some(c) => {
            let c = c.clone();
            rt.spawn(async move {
                match command(&c, r#type, payload).await {
                    Ok(line) => say(&c.ui, line),
                    Err(e) => say(&c.ui, format!("Not done: {e}")),
                }
            });
        }
        None => say(weak, "Not attached to the application yet"),
    }
}

pub fn start(rt: Handle, address: String, unit: String, ui: &MainWindow) -> Arc<Shared> {
    let shared = Arc::new(Shared { unit: unit.clone(), attachment: OnceLock::new(), started: Mutex::new(Vec::new()) });
    let weak = ui.as_weak();
    let log = Arc::new(Mutex::new(Log::default()));
    let ready: Arc<OnceLock<Ctx>> = Arc::new(OnceLock::new());

    {
        let (ready, rt, weak) = (ready.clone(), rt.clone(), weak.clone());
        ui.on_act(move |r#type, id| {
            let r#type = match r#type.as_str() {
                "containers.start" => "containers.start",
                "containers.stop" => "containers.stop",
                "containers.restart" => "containers.restart",
                _ => "containers.remove",
            };
            on_command(&rt, &ready, &weak, r#type, json!({ "id": id.to_string() }));
        });
    }
    {
        let (ready, rt, weak) = (ready.clone(), rt.clone(), weak.clone());
        ui.on_remove_image(move |id| on_command(&rt, &ready, &weak, "containers.image.remove", json!({ "id": id.to_string() })));
    }
    {
        let (ready, rt, weak) = (ready.clone(), rt.clone(), weak.clone());
        ui.on_pull(move |reference| {
            let reference = reference.trim().to_string();
            if reference.is_empty() {
                say(&weak, "Name an image to pull");
                return;
            }
            say(&weak, format!("Pulling {reference}..."));
            on_command(&rt, &ready, &weak, "containers.image.pull", json!({ "reference": reference }));
        });
    }
    {
        let (log, weak) = (log.clone(), weak.clone());
        ui.on_clear_events(move || {
            let mut l = log.lock().unwrap();
            l.lines.clear();
            l.dirty = false;
            let _ = weak.upgrade_in_event_loop(|u| u.set_events(ModelRc::new(VecModel::default())));
        });
    }

    let shared2 = shared.clone();
    rt.spawn(async move {
        let at = match Attachment::attach(&address, Options::new()).await {
            Ok(at) => Arc::new(at),
            Err(e) => {
                say(&weak, format!("Cannot attach to {address}: {e}"));
                return;
            }
        };
        let _ = shared2.attachment.set(at.clone());
        let c = Ctx { at, unit, ui: weak, log, t0: Instant::now(), shared: shared2 };
        let _ = ready.set(c.clone());
        say(&c.ui, "Attached");
        tokio::spawn(redraw_events(c.clone()));
        tokio::spawn(events(c.clone()));
        for stream in [STATUS, LIST, IMAGES] {
            tokio::spawn(follow(c.clone(), stream));
        }
    });
    shared
}

/// Once the window has closed: the streams this panel started are stopped.
pub async fn finish(shared: Arc<Shared>) {
    let Some(at) = shared.attachment.get() else { return };
    let started = std::mem::take(&mut *shared.started.lock().unwrap());
    for stream in started {
        let _ = tokio::time::timeout(Duration::from_secs(2), at.stop_stream(&shared.unit, &stream)).await;
    }
}

async fn command(c: &Ctx, r#type: &str, payload: Value) -> Result<String, String> {
    match c.at.command(&c.unit, r#type, payload.to_string().as_bytes()).await {
        Ok(ack) => match ack.outcome {
            2 => Ok(ack.line),
            1 => Ok(format!("Still running — {}", ack.line)),
            _ => Err(ack.line),
        },
        Err(e) => Err(e.to_string()),
    }
}

async fn ask(c: &Ctx, r#type: &str) -> Result<Value, String> {
    let answer = c.at.ask(&c.unit, r#type, b"").await.map_err(|e| e.to_string())?;
    serde_json::from_slice(&answer).map_err(|e| e.to_string())
}

fn log(c: &Ctx, line: String) {
    let mut l = c.log.lock().unwrap();
    l.lines.push_front(format!("+{:>8.3}s  {line}", c.t0.elapsed().as_secs_f32()));
    l.lines.truncate(MAX_EVENTS);
    l.dirty = true;
}

/// Subscribes, starts the stream where it does not flow, asks the present, and keeps only what is newer.
async fn follow(c: Ctx, stream: &'static str) {
    loop {
        match c.at.subscribe_stream(&c.unit, stream).await {
            Ok(mut delivery) => {
                if !delivery.flowing {
                    match c.at.start_stream(&c.unit, stream).await {
                        Ok(_) => c.shared.started.lock().unwrap().push(stream.to_string()),
                        Err(e) => log(&c, format!("{stream} cannot be started: {e}")),
                    }
                }
                let mut seen = 0u64;
                match ask(&c, stream).await {
                    Ok(v) => {
                        seen = v["sequence"].as_u64().unwrap_or(0);
                        show(&c, stream, &v);
                    }
                    // A list is failed while no engine is reached; the status says why.
                    Err(e) => log(&c, format!("{stream} cannot be asked: {e}")),
                }
                loop {
                    match delivery.next().await {
                        Ok(Some(frame)) => {
                            let Ok(v) = serde_json::from_slice::<Value>(&frame.payload) else { continue };
                            let sequence = v["sequence"].as_u64().unwrap_or(0);
                            if sequence > seen {
                                seen = sequence;
                                show(&c, stream, &v);
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            log(&c, format!("{stream}: {e}"));
                            break;
                        }
                    }
                }
                log(&c, format!("{stream}: the delivery ended; subscribing again"));
            }
            Err(e) => log(&c, format!("{stream} cannot be subscribed to: {e}")),
        }
        sleep(AGAIN).await;
    }
}

fn show(c: &Ctx, stream: &str, v: &Value) {
    match stream {
        STATUS => on_status(c, v),
        LIST => show_containers(c, v),
        _ => show_images(c, v),
    }
}

fn on_status(c: &Ctx, v: &Value) {
    let reachable = v["reachable"].as_bool().unwrap_or(false);
    let engine = format!(
        "{} {}{}",
        s(&v["engine"]),
        s(&v["version"]),
        if v["rootless"].as_bool().unwrap_or(false) { ", rootless" } else { "" }
    );
    let counts = if reachable {
        format!(
            "{} containers, {} running, {} paused · {} images · {}",
            v["containers"], v["running"], v["paused"], v["images"], s(&v["socket"])
        )
    } else {
        s(&v["reason"])
    };
    log(c, format!("status   #{} {}", v["sequence"], if reachable { &engine } else { "no engine" }));
    let _ = c.ui.upgrade_in_event_loop(move |u| {
        u.set_reachable(reachable);
        u.set_engine(engine.into());
        u.set_counts(counts.into());
        if u.get_message() == "Attached" || u.get_message() == "Starting..." {
            u.set_message("Ready".into());
        }
    });
}

/// The running first, then by name.
fn show_containers(c: &Ctx, v: &Value) {
    let mut list = v["containers"].as_array().cloned().unwrap_or_default();
    list.sort_by_key(|x| (x["state"] != "running", s(&x["name"])));
    let rows: Vec<ContainerRow> = list
        .iter()
        .map(|x| ContainerRow {
            id: s(&x["id"]).into(),
            name: s(&x["name"]).into(),
            image: s(&x["image"]).into(),
            state: s(&x["state"]).into(),
            status: s(&x["status"]).into(),
            ports: x["ports"].as_array().into_iter().flatten().filter_map(Value::as_str).collect::<Vec<_>>().join(", ").into(),
            running: matches!(x["state"].as_str(), Some("running" | "paused" | "restarting")),
            protected: x["protected"].as_bool().unwrap_or(true),
        })
        .collect();
    if v["truncated"].as_bool().unwrap_or(false) {
        log(c, "containers: more than one payload holds; the list is cut".into());
    }
    let _ = c.ui.upgrade_in_event_loop(move |u| u.set_containers(ModelRc::new(VecModel::from(rows))));
}

/// The tagged first, by tag; the rest by id.
fn show_images(c: &Ctx, v: &Value) {
    let mut list = v["images"].as_array().cloned().unwrap_or_default();
    let first_tag = |x: &Value| x["tags"][0].as_str().map(str::to_string);
    list.sort_by_key(|x| (first_tag(x).is_none(), first_tag(x).unwrap_or_else(|| s(&x["id"]))));
    let rows: Vec<ImageRow> = list
        .iter()
        .map(|x| {
            let tags: Vec<&str> = x["tags"].as_array().into_iter().flatten().filter_map(Value::as_str).collect();
            let used = x["containers"].as_u64().unwrap_or(0);
            let id = s(&x["id"]);
            ImageRow {
                tags: if tags.is_empty() { "<untagged>".into() } else { tags.join(", ").into() },
                size: bytes(x["size"].as_i64().unwrap_or(0)).into(),
                used: match used {
                    0 => String::new(),
                    1 => "1 container".into(),
                    n => format!("{n} containers"),
                }
                .into(),
                in_use: used > 0,
                protected: x["protected"].as_bool().unwrap_or(true),
                id: id.chars().take(12).collect::<String>().into(),
            }
        })
        .collect();
    let _ = c.ui.upgrade_in_event_loop(move |u| u.set_images(ModelRc::new(VecModel::from(rows))));
}

async fn events(c: Ctx) {
    let filter = Filter { subject_kind: "unit".into(), subject_identity: c.unit.clone(), ..Default::default() };
    let mut sub = match c.at.subscribe(filter).await {
        Ok(s) => s,
        Err(e) => {
            log(&c, format!("events cannot be subscribed to: {e}"));
            return;
        }
    };
    loop {
        match sub.next().await {
            Ok(Some(sub)) => match sub.carries {
                Some(Carries::Event(e)) => {
                    let detail: Value = serde_json::from_slice(&e.detail).unwrap_or(Value::Null);
                    let line = detail["line"].as_str().map(str::to_string).unwrap_or_else(|| {
                        let d = detail.to_string();
                        if d.len() > 160 { format!("{}…", &d[..160]) } else { d }
                    });
                    let class = if e.occurrence.is_empty() { String::new() } else { format!(" {}", e.occurrence) };
                    log(&c, format!("event    {} {}{class} [{}] {line}", e.seq, e.r#type, e.severity));
                    // What the person should see at once: a container unhealthy, out of memory, a job ended.
                    if e.occurrence.starts_with("containers.") && e.severity >= 50 || e.occurrence == "containers.job.finished" {
                        say(&c.ui, line);
                    }
                }
                Some(Carries::Overflow(_)) => log(&c, "events: the panel fell behind and some were dropped".into()),
                _ => {}
            },
            Ok(None) => break,
            Err(e) => {
                log(&c, format!("events: {e}"));
                break;
            }
        }
    }
}

async fn redraw_events(c: Ctx) {
    let mut tick = interval(EVENTS_REDRAW);
    loop {
        tick.tick().await;
        let rows = {
            let mut l = c.log.lock().unwrap();
            if !l.dirty {
                continue;
            }
            l.dirty = false;
            l.lines.iter().map(|s| SharedString::from(s.as_str())).collect::<Vec<_>>()
        };
        let _ = c.ui.upgrade_in_event_loop(move |u| u.set_events(ModelRc::new(VecModel::from(rows))));
    }
}
