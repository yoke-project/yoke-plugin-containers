//! The containers Plugin: the host's container engine — its containers and its images — as questions,
//! commands, streams and occurrences.
//!
//! - **`containers.status`, `containers.list` and `containers.images` are each a question and a stream of
//!   the same record**, with a sequence counted per stream from the unit's start: the engine and its
//!   counts; every container, whole; every image, whole. A change is counted once, whether or not the
//!   stream flows, and the question states the sequence of the last record it reflects. A client that
//!   subscribes and then asks holds the present, and drops what the stream brings at or below the
//!   sequence it was answered with.
//! - **Changes are the engine's own events**: an event about a container reads the containers again, one
//!   about an image the images; nothing is read again at an interval. Some events are occurrences too:
//!   a container started, exited, unhealthy, killed for want of memory.
//! - **Reading is never limited; changing is.** The application names the containers commands may act on
//!   with `--allow <pattern>` and `--protect <pattern>`, the images with `--allow-image` and
//!   `--protect-image`, `*` matching any run of characters. A container labelled `dev.yoke-project.*` is
//!   one Yoke runs, and is always protected, with the images such containers run.
//! - **A command is acknowledged** `Done`, `Failed` with `<code>: <message>`, or `Accepted` when it is
//!   still running after [`COMMAND_WAIT`]; `containers.job.finished` then says how it ended.
//! - **Every occurrence carries its facts as JSON in its detail**, which is what an interface client
//!   receives; the line is for the log.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use yoke_sdk::plugin::{Capability, Command, Declaration, Event, Object, Outcome, Question, Severity, Stream, Unit};

use crate::backend::{Action, Container, ContainersBackend, EngineError, EngineEvent, EngineInfo, Image, Kind, Said};

pub const PLUGIN: &str = "com.yoke.containers";
/// The format every answer, frame and detail states as `schema`.
pub const SCHEMA: u32 = 1;

pub const STATUS: &str = "containers.status";
pub const LIST: &str = "containers.list";
pub const IMAGES: &str = "containers.images";
pub const INSPECT: &str = "containers.inspect";
pub const LOGS: &str = "containers.logs";
pub const PERMISSIONS: &str = "containers.permissions";
pub const QUERIES: [&str; 6] = [STATUS, LIST, IMAGES, INSPECT, LOGS, PERMISSIONS];

pub const START: &str = "containers.start";
pub const STOP: &str = "containers.stop";
pub const RESTART: &str = "containers.restart";
pub const PAUSE: &str = "containers.pause";
pub const UNPAUSE: &str = "containers.unpause";
pub const KILL: &str = "containers.kill";
pub const REMOVE: &str = "containers.remove";
pub const PULL: &str = "containers.image.pull";
pub const IMAGE_REMOVE: &str = "containers.image.remove";
pub const COMMANDS: [&str; 9] = [START, STOP, RESTART, PAUSE, UNPAUSE, KILL, REMOVE, PULL, IMAGE_REMOVE];

pub const STREAMS: [&str; 3] = [STATUS, LIST, IMAGES];

pub const STARTED: &str = "containers.container.started";
pub const EXITED: &str = "containers.container.exited";
pub const UNHEALTHY: &str = "containers.container.unhealthy";
pub const OOM: &str = "containers.container.oom";
pub const JOB_FINISHED: &str = "containers.job.finished";
pub const OCCURRENCES: [&str; 5] = [STARTED, EXITED, UNHEALTHY, OOM, JOB_FINISHED];

/// How long a command is waited for before it is acknowledged as accepted, under the Core's 30 s.
pub const COMMAND_WAIT: Duration = Duration::from_secs(25);
/// How long a question is waited for, under the Core's 30 s.
pub const ANSWER_WAIT: Duration = Duration::from_secs(25);
/// The most an answer or a frame holds, under the 1 MiB the Core carries; a longer list is cut and says
/// `"truncated": true`.
pub const MAX_PAYLOAD: usize = (1 << 20) - 1024;
/// The lines of output `containers.logs` gives by default, and at most.
pub const LOG_TAIL: u32 = 200;
pub const LOG_TAIL_MAX: u32 = 5000;
/// The labels of what Yoke itself runs.
pub const YOKE_LABELS: &str = "dev.yoke-project.";

pub fn declaration() -> Declaration {
    let answer = |q: &&str| Capability { name: format!("query.{q}.answer"), governs: Object::Query(q.to_string()) };
    let accept = |c: &&str| Capability { name: format!("command.{c}.accept"), governs: Object::Command(c.to_string()) };
    let publish = |s: &&str| Capability { name: format!("stream.{s}.publish"), governs: Object::Stream(s.to_string()) };
    let report = |o: &&str| Capability { name: format!("event.{o}.report"), governs: Object::Occurrence(o.to_string()) };
    Declaration {
        id: PLUGIN.into(),
        queries: QUERIES.iter().map(|q| q.to_string()).collect(),
        commands: COMMANDS.iter().map(|c| c.to_string()).collect(),
        streams: vec![
            // The engine reached or lost is not a change a client may miss.
            Stream { id: STATUS.into(), tolerates_loss: false, tolerates_reorder: false, max_frame: 0 },
            // The whole list again each time, so a lost one is made good by the next.
            Stream { id: LIST.into(), tolerates_loss: true, tolerates_reorder: false, max_frame: 0 },
            Stream { id: IMAGES.into(), tolerates_loss: true, tolerates_reorder: false, max_frame: 0 },
        ],
        occurrences: OCCURRENCES.iter().map(|o| o.to_string()).collect(),
        capabilities: QUERIES
            .iter()
            .map(answer)
            .chain(COMMANDS.iter().map(accept))
            .chain(STREAMS.iter().map(publish))
            .chain(OCCURRENCES.iter().map(report))
            .collect(),
        ..Default::default()
    }
}

#[derive(Debug)]
pub enum Error {
    Invalid(String),
    /// Yoke runs it, or the application protects it.
    Protected(String),
    /// The application names what commands may act on, and not this.
    NotAllowed(String),
    Engine(EngineError),
}

impl From<EngineError> for Error {
    fn from(e: EngineError) -> Self {
        Error::Engine(e)
    }
}

pub fn code(e: &Error) -> &'static str {
    match e {
        Error::Invalid(_) | Error::Engine(EngineError::Invalid(_)) => "invalid",
        Error::Protected(_) => "protected",
        Error::NotAllowed(_) => "not_allowed",
        Error::Engine(EngineError::Unavailable(_)) => "unavailable",
        Error::Engine(EngineError::NotFound(_)) => "not_found",
        Error::Engine(EngineError::Conflict(_)) => "conflict",
        Error::Engine(EngineError::PermissionDenied(_)) => "permission_denied",
        Error::Engine(EngineError::Engine(_)) => "backend",
    }
}

pub fn message(e: &Error) -> String {
    match e {
        Error::Invalid(m) | Error::Protected(m) | Error::NotAllowed(m) => m.clone(),
        Error::Engine(e) => e.to_string(),
    }
}

/// What commands may act on: the unit's arguments `--allow`, `--protect`, `--allow-image` and
/// `--protect-image`, each as many times as needed, `*` matching any run of characters. A container is
/// matched by its names and its id, an image by its tags and its id.
#[derive(Debug, Clone, Default)]
pub struct Policy {
    pub allow: Vec<String>,
    pub protect: Vec<String>,
    pub allow_image: Vec<String>,
    pub protect_image: Vec<String>,
}

impl Policy {
    pub fn from_args(args: impl IntoIterator<Item = String>) -> Self {
        let mut p = Policy::default();
        let mut args = args.into_iter();
        while let Some(a) = args.next() {
            match a.as_str() {
                "--allow" => p.allow.extend(args.next()),
                "--protect" => p.protect.extend(args.next()),
                "--allow-image" => p.allow_image.extend(args.next()),
                "--protect-image" => p.protect_image.extend(args.next()),
                _ => {}
            }
        }
        p
    }

    /// Why commands may not act on the container, or nothing.
    pub fn refuses_container(&self, c: &Container) -> Option<Error> {
        let name = if c.name().is_empty() { short(&c.id) } else { c.name() };
        if is_yoke(c) {
            return Some(Error::Protected(format!("{name} is run by Yoke, and is never acted on")));
        }
        let names: Vec<&str> = c.names.iter().map(String::as_str).chain([c.id.as_str()]).collect();
        refuses(&self.allow, &self.protect, &names, name)
    }

    /// Why commands may not act on the image, or nothing. `yoke_images` are the images containers Yoke
    /// runs were made from.
    pub fn refuses_image(&self, i: &Image, yoke_images: &HashSet<&str>) -> Option<Error> {
        let name = i.tags.first().map(String::as_str).unwrap_or_else(|| short(&i.id));
        if i.labels.keys().any(|k| k.starts_with(YOKE_LABELS)) || yoke_images.contains(i.id.as_str()) {
            return Some(Error::Protected(format!("{name} is an image Yoke runs, and is never acted on")));
        }
        let names: Vec<&str> = i.tags.iter().map(String::as_str).chain([i.id.as_str()]).collect();
        refuses(&self.allow_image, &self.protect_image, &names, name)
    }

    /// Why an image not yet on the host may not be pulled, or nothing.
    pub fn refuses_reference(&self, reference: &str) -> Option<Error> {
        refuses(&self.allow_image, &self.protect_image, &[reference], reference)
    }
}

fn refuses(allow: &[String], protect: &[String], names: &[&str], name: &str) -> Option<Error> {
    if protect.iter().any(|p| names.iter().any(|n| matches(p, n))) {
        return Some(Error::Protected(format!("{name} is protected by the application, and is never acted on")));
    }
    if !allow.is_empty() && !allow.iter().any(|p| names.iter().any(|n| matches(p, n))) {
        return Some(Error::NotAllowed(format!("{name} is not among what the application allows")));
    }
    None
}

pub fn is_yoke(c: &Container) -> bool {
    c.labels.keys().any(|k| k.starts_with(YOKE_LABELS))
}

/// Whether name matches pattern, `*` standing for any run of characters.
pub fn matches(pattern: &str, name: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == name;
    }
    let (first, last) = (parts[0], parts[parts.len() - 1]);
    if !name.starts_with(first) || !name.ends_with(last) || name.len() < first.len() + last.len() {
        return false;
    }
    let mut rest = &name[first.len()..name.len() - last.len()];
    for part in &parts[1..parts.len() - 1] {
        match rest.find(part) {
            Some(i) => rest = &rest[i + part.len()..],
            None => return false,
        }
    }
    true
}

/// A full id as the engines print it; anything else as it is.
fn short(id: &str) -> &str {
    if id.len() == 64 && id.bytes().all(|b| b.is_ascii_hexdigit()) {
        &id[..12]
    } else {
        id
    }
}

fn input<T: DeserializeOwned>(what: &str, payload: &[u8]) -> Result<T, Error> {
    let payload = if payload.iter().all(u8::is_ascii_whitespace) { b"{}".as_slice() } else { payload };
    serde_json::from_slice(payload).map_err(|e| {
        let kind = match e.classify() {
            serde_json::error::Category::Data => "a field is missing, unknown or of the wrong type",
            _ => "it is not JSON",
        };
        Error::Invalid(format!("the payload is not what {what} takes: {kind}, at line {} column {}", e.line(), e.column()))
    })
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NoInput {}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KindInput {
    Container,
    Image,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectInput {
    pub id: String,
    #[serde(default)]
    pub kind: Option<KindInput>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogsInput {
    pub id: String,
    #[serde(default)]
    pub tail: Option<u32>,
}

/// `containers.start`, `pause`, `unpause`, `kill`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetInput {
    pub id: String,
}

/// `containers.stop` and `containers.restart`: the grace, in seconds, before the container is killed.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopInput {
    pub id: String,
    #[serde(default)]
    pub timeout: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveInput {
    pub id: String,
    #[serde(default)]
    pub force: bool,
    /// The container's anonymous volumes go with it.
    #[serde(default)]
    pub volumes: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PullInput {
    pub reference: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageRemoveInput {
    /// An id, or a tag, which takes that tag away and the image with its last.
    pub id: String,
    #[serde(default)]
    pub force: bool,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

/// The largest prefix (or, `newest`, suffix) of a list whose payload fits: `make(items, truncated)`.
pub fn fit(list: &[Value], newest: bool, make: impl Fn(&[Value], bool) -> Value) -> Vec<u8> {
    let full = make(list, false).to_string().into_bytes();
    if full.len() <= MAX_PAYLOAD {
        return full;
    }
    let part = |n: usize| if newest { &list[list.len() - n..] } else { &list[..n] };
    let (mut lo, mut hi) = (0, list.len());
    while lo < hi {
        let mid = (lo + hi).div_ceil(2);
        if make(part(mid), true).to_string().len() <= MAX_PAYLOAD {
            lo = mid
        } else {
            hi = mid - 1
        }
    }
    make(part(lo), true).to_string().into_bytes()
}

pub trait Acts {
    fn answer(&self, q: &Question, payload: Vec<u8>) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>>;
    fn fail(&self, q: &Question, code: &str, message: &str) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>>;
    fn ack(&self, c: &Command, outcome: Outcome, line: &str) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>>;
    /// An occurrence, its facts as JSON in its detail.
    fn report(&self, occurrence: &str, severity: u8, line: &str, detail: Value) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>>;
    fn emit(&self, stream: &str, payload: Vec<u8>) -> impl std::future::Future<Output = Result<(), yoke_sdk::base::Error>>;
}

impl Acts for Unit {
    async fn answer(&self, q: &Question, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        Unit::answer(self, q, payload).await
    }
    async fn fail(&self, q: &Question, code: &str, message: &str) -> Result<(), yoke_sdk::base::Error> {
        Unit::fail(self, &q.id, code, message).await
    }
    async fn ack(&self, c: &Command, outcome: Outcome, line: &str) -> Result<(), yoke_sdk::base::Error> {
        Unit::ack(self, c, outcome, line).await
    }
    async fn report(&self, occurrence: &str, severity: u8, line: &str, detail: Value) -> Result<(), yoke_sdk::base::Error> {
        Unit::report(self, occurrence, Some(Severity::of(severity)), line, detail.to_string().into_bytes()).await
    }
    async fn emit(&self, stream: &str, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        Unit::emit(self, stream, payload).await
    }
}

/// One stream's record: its body, and the sequence and time of its last change. A body that is null was
/// never read.
struct Record {
    sequence: u64,
    at: u64,
    body: Value,
}

impl Record {
    fn new() -> Record {
        Record { sequence: 0, at: now_ms(), body: Value::Null }
    }

    /// Takes the body if it differs: counted once.
    fn set(&mut self, body: Value) -> bool {
        if self.body == body {
            return false;
        }
        self.body = body;
        self.sequence += 1;
        self.at = now_ms();
        true
    }

    fn head(&self) -> Value {
        json!({ "schema": SCHEMA, "sequence": self.sequence, "at": self.at })
    }

    /// The status: the body beside the head.
    fn whole(&self) -> Vec<u8> {
        let mut v = self.head();
        if let (Some(v), Some(body)) = (v.as_object_mut(), self.body.as_object()) {
            v.extend(body.clone());
        }
        v.to_string().into_bytes()
    }

    /// A list, under `key`, cut to what a payload holds.
    fn list(&self, key: &str) -> Vec<u8> {
        let items = self.body.as_array().cloned().unwrap_or_default();
        fit(&items, false, |part, truncated| {
            let mut v = self.head();
            v[key] = Value::Array(part.to_vec());
            v["truncated"] = json!(truncated);
            v
        })
    }
}

/// What an engine event makes worth reading again.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Dirty {
    pub containers: bool,
    pub images: bool,
}

impl Dirty {
    pub const ALL: Dirty = Dirty { containers: true, images: true };

    pub fn any(&self) -> bool {
        self.containers || self.images
    }

    pub fn or(self, o: Dirty) -> Dirty {
        Dirty { containers: self.containers || o.containers, images: self.images || o.images }
    }
}

/// Events about a container that change nothing a list shows.
const QUIET: [&str; 10] =
    ["exec_create", "exec_start", "exec_die", "attach", "detach", "resize", "top", "export", "archive-path", "extract-to-dir"];

struct State {
    engine: Option<EngineInfo>,
    /// Why the engine is not reached.
    reason: String,
    containers: Vec<Container>,
    images: Vec<Image>,
    status: Record,
    list: Record,
    image_list: Record,
}

pub struct Containers {
    pub backend: Arc<dyn ContainersBackend>,
    pub policy: Policy,
    state: Mutex<State>,
    flows: [AtomicBool; 3],
}

impl Containers {
    pub fn new(backend: Arc<dyn ContainersBackend>, policy: Policy) -> Arc<Containers> {
        let me = Containers {
            backend,
            policy,
            state: Mutex::new(State {
                engine: None,
                reason: "the engine is being looked for".into(),
                containers: Vec::new(),
                images: Vec::new(),
                status: Record::new(),
                list: Record::new(),
                image_list: Record::new(),
            }),
            flows: [AtomicBool::new(false), AtomicBool::new(false), AtomicBool::new(false)],
        };
        {
            let mut st = me.state.lock().unwrap();
            let body = status_body(&st);
            st.status.body = body;
        }
        Arc::new(me)
    }

    fn flag(&self, stream: &str) -> Option<&AtomicBool> {
        STREAMS.iter().position(|s| *s == stream).map(|i| &self.flows[i])
    }

    fn flows(&self, stream: &str) -> bool {
        self.flag(stream).is_some_and(|f| f.load(Ordering::Relaxed))
    }

    /// The engine was reached: everything is read again.
    pub async fn reached(&self, u: &impl Acts, info: EngineInfo) -> Result<(), yoke_sdk::base::Error> {
        {
            let mut st = self.state.lock().unwrap();
            st.engine = Some(info);
            st.reason.clear();
        }
        self.refresh(u, Dirty::ALL).await
    }

    /// The engine was lost, or not found: the status says why, and the lists are not answered until it is
    /// reached again.
    pub async fn lost(&self, u: &impl Acts, why: &str) -> Result<(), yoke_sdk::base::Error> {
        let record = {
            let mut st = self.state.lock().unwrap();
            st.engine = None;
            st.reason = why.to_string();
            let body = status_body(&st);
            st.status.set(body).then(|| st.status.whole())
        };
        match record {
            Some(r) if self.flows(STATUS) => u.emit(STATUS, r).await,
            _ => Ok(()),
        }
    }

    /// The grade and the line of the unit's health.
    pub fn health(&self) -> (u8, String) {
        let st = self.state.lock().unwrap();
        match &st.engine {
            Some(e) => (0, format!("{} {} at {}{}", e.kind, e.version, e.socket, if e.rootless { ", rootless" } else { "" })),
            None => (50, format!("no container engine: {}", st.reason)),
        }
    }

    /// One event of the engine's: reported where it is an occurrence, and what it makes worth reading
    /// again.
    pub async fn event(&self, u: &impl Acts, e: &EngineEvent) -> (Dirty, Result<(), yoke_sdk::base::Error>) {
        match e.kind.as_str() {
            "container" => {
                let reported = self.occurrence(u, e).await;
                let dirty = Dirty { containers: !QUIET.contains(&e.action.as_str()), images: false };
                (dirty, reported)
            }
            "image" => (Dirty { containers: false, images: true }, Ok(())),
            _ => (Dirty::default(), Ok(())),
        }
    }

    async fn occurrence(&self, u: &impl Acts, e: &EngineEvent) -> Result<(), yoke_sdk::base::Error> {
        let name = if e.name.is_empty() { short(&e.id).to_string() } else { e.name.clone() };
        let image = e.attributes.get("image").cloned().unwrap_or_default();
        let mut detail = json!({ "schema": SCHEMA, "id": e.id, "name": name, "image": image });
        let (occurrence, severity, line) = match (e.action.as_str(), e.detail.as_str()) {
            ("start", _) => (STARTED, 10, format!("{name} started")),
            ("die", _) => {
                let code: i64 = e.attributes.get("exitCode").and_then(|c| c.parse().ok()).unwrap_or(-1);
                detail["exit_code"] = json!(code);
                (EXITED, if code == 0 { 10 } else { 30 }, format!("{name} exited with {code}"))
            }
            ("health_status", "unhealthy") => {
                detail["health"] = json!("unhealthy");
                (UNHEALTHY, 50, format!("{name} is unhealthy"))
            }
            ("oom", _) => (OOM, 70, format!("{name} ran out of memory")),
            _ => return Ok(()),
        };
        detail["line"] = json!(line);
        u.report(occurrence, severity, &line, detail).await
    }

    /// Reads again what is dirty, and says what changed: each record counted, and emitted where it flows.
    pub async fn refresh(&self, u: &impl Acts, dirty: Dirty) -> Result<(), yoke_sdk::base::Error> {
        if self.state.lock().unwrap().engine.is_none() {
            return Ok(());
        }
        // An engine lost meanwhile is said by its socket: what cannot be read now is left as it was.
        let containers = if dirty.containers { self.backend.containers().await.ok() } else { None };
        let images = if dirty.images { self.backend.images().await.ok() } else { None };
        let changed = {
            let mut st = self.state.lock().unwrap();
            if let Some(c) = containers {
                st.containers = c;
            }
            if let Some(i) = images {
                st.images = i;
            }
            let list: Vec<Value> = st.containers.iter().map(|c| self.container_json(c)).collect();
            let yoke_images = yoke_images(&st.containers);
            let image_list: Vec<Value> =
                st.images.iter().map(|i| self.image_json(i, &st.containers, &yoke_images)).collect();
            let status = status_body(&st);
            let mut changed = Vec::new();
            if st.list.set(Value::Array(list)) {
                changed.push((LIST, st.list.list("containers")));
            }
            if st.image_list.set(Value::Array(image_list)) {
                changed.push((IMAGES, st.image_list.list("images")));
            }
            if st.status.set(status) {
                changed.push((STATUS, st.status.whole()));
            }
            changed
        };
        let mut first = Ok(());
        for (stream, payload) in changed {
            if self.flows(stream) {
                first = first.and(u.emit(stream, payload).await);
            }
        }
        first
    }

    pub fn container_json(&self, c: &Container) -> Value {
        json!({
            "id": c.id, "name": c.name(), "names": c.names, "image": c.image, "image_id": c.image_id,
            "command": c.command, "state": c.state, "status": c.status, "health": c.health, "created": c.created,
            "ports": c.ports, "yoke": is_yoke(c), "protected": self.policy.refuses_container(c).is_some(),
        })
    }

    fn image_json(&self, i: &Image, containers: &[Container], yoke_images: &HashSet<&str>) -> Value {
        json!({
            "id": i.id, "tags": i.tags, "size": i.size, "created": i.created,
            "containers": containers.iter().filter(|c| c.image_id == i.id).count(),
            "protected": self.policy.refuses_image(i, yoke_images).is_some(),
        })
    }

    /// A stream was activated: it is given the present record at once, where there is one.
    pub async fn activated(&self, u: &impl Acts, stream: &str) -> Result<(), yoke_sdk::base::Error> {
        let Some(flag) = self.flag(stream) else { return Ok(()) };
        flag.store(true, Ordering::Relaxed);
        let payload = {
            let st = self.state.lock().unwrap();
            match stream {
                STATUS => Some(st.status.whole()),
                LIST if st.engine.is_some() && !st.list.body.is_null() => Some(st.list.list("containers")),
                IMAGES if st.engine.is_some() && !st.image_list.body.is_null() => Some(st.image_list.list("images")),
                _ => None,
            }
        };
        match payload {
            Some(p) => u.emit(stream, p).await,
            None => Ok(()),
        }
    }

    pub fn stopped(&self, stream: &str) {
        if let Some(flag) = self.flag(stream) {
            flag.store(false, Ordering::Relaxed);
        }
    }

    fn reached_or_why(&self) -> Result<(), Error> {
        let st = self.state.lock().unwrap();
        match st.engine {
            Some(_) if !st.list.body.is_null() => Ok(()),
            Some(_) => Err(Error::Engine(EngineError::Unavailable("the engine is being read".into()))),
            None => Err(Error::Engine(EngineError::Unavailable(format!("no container engine: {}", st.reason)))),
        }
    }

    pub async fn answer(&self, query: &str, payload: &[u8]) -> Result<Vec<u8>, Error> {
        match query {
            STATUS => {
                let _: NoInput = input(query, payload)?;
                Ok(self.state.lock().unwrap().status.whole())
            }
            LIST => {
                let _: NoInput = input(query, payload)?;
                self.reached_or_why()?;
                Ok(self.state.lock().unwrap().list.list("containers"))
            }
            IMAGES => {
                let _: NoInput = input(query, payload)?;
                self.reached_or_why()?;
                Ok(self.state.lock().unwrap().image_list.list("images"))
            }
            INSPECT => {
                let i: InspectInput = input(query, payload)?;
                let kind = match i.kind {
                    Some(KindInput::Image) => Kind::Image,
                    _ => Kind::Container,
                };
                let record = self.backend.inspect(kind, &i.id).await?;
                let kind = if kind == Kind::Image { "image" } else { "container" };
                let v = json!({ "schema": SCHEMA, "kind": kind, "id": i.id, "inspect": record, "truncated": false });
                let bytes = v.to_string().into_bytes();
                if bytes.len() <= MAX_PAYLOAD {
                    Ok(bytes)
                } else {
                    Ok(json!({ "schema": SCHEMA, "kind": kind, "id": i.id, "inspect": null, "truncated": true }).to_string().into_bytes())
                }
            }
            LOGS => {
                let i: LogsInput = input(query, payload)?;
                let tail = i.tail.unwrap_or(LOG_TAIL).min(LOG_TAIL_MAX);
                let lines = self.backend.logs(&i.id, tail).await?;
                let lines: Vec<Value> = lines
                    .iter()
                    .map(|l| json!({ "stream": if l.stderr { "stderr" } else { "stdout" }, "text": l.text }))
                    .collect();
                // The newest lines are kept where they do not all fit.
                Ok(fit(&lines, true, |part, truncated| {
                    json!({ "schema": SCHEMA, "id": i.id, "lines": part, "truncated": truncated })
                }))
            }
            PERMISSIONS => {
                let _: NoInput = input(query, payload)?;
                let engine = self.state.lock().unwrap().engine.clone();
                let permissions: Vec<Value> = self
                    .backend
                    .permissions()
                    .await
                    .iter()
                    .map(|p| json!({ "action": p.action, "allowed": p.allowed, "detail": p.detail }))
                    .collect();
                Ok(json!({
                    "schema": SCHEMA,
                    "engine": engine.as_ref().map(|e| e.kind.clone()),
                    "socket": engine.as_ref().map(|e| e.socket.clone()),
                    "rootless": engine.as_ref().map(|e| e.rootless),
                    "permissions": permissions,
                })
                .to_string()
                .into_bytes())
            }
            other => Err(Error::Invalid(format!("{other} is not a question of {PLUGIN}"))),
        }
    }

    /// The container a command names, read afresh, and whether commands may act on it.
    async fn container(&self, id: &str) -> Result<Container, Error> {
        if id.is_empty() {
            return Err(Error::Invalid("the payload names no container: {\"id\": \"<id or name>\"}".into()));
        }
        let list = self.backend.containers().await?;
        let c = find_container(&list, id)?.clone();
        match self.policy.refuses_container(&c) {
            Some(e) => Err(e),
            None => Ok(c),
        }
    }

    pub async fn act(&self, command: &str, payload: &[u8]) -> Result<String, Error> {
        let named = |c: &Container| if c.name().is_empty() { short(&c.id).to_string() } else { c.name().to_string() };
        match command {
            START | PAUSE | UNPAUSE | KILL => {
                let i: TargetInput = input(command, payload)?;
                let c = self.container(&i.id).await?;
                let (action, done) = match command {
                    START => (Action::Start, "started"),
                    PAUSE => (Action::Pause, "paused"),
                    UNPAUSE => (Action::Unpause, "unpaused"),
                    _ => (Action::Kill, "killed"),
                };
                self.backend.act(&c.id, action, None).await?;
                Ok(format!("{} {done}", named(&c)))
            }
            STOP | RESTART => {
                let i: StopInput = input(command, payload)?;
                let c = self.container(&i.id).await?;
                let (action, done) = if command == STOP { (Action::Stop, "stopped") } else { (Action::Restart, "restarted") };
                self.backend.act(&c.id, action, i.timeout).await?;
                Ok(format!("{} {done}", named(&c)))
            }
            REMOVE => {
                let i: RemoveInput = input(command, payload)?;
                let c = self.container(&i.id).await?;
                self.backend.remove(&c.id, i.force, i.volumes).await?;
                Ok(format!("{} removed{}", named(&c), if i.volumes { " with its anonymous volumes" } else { "" }))
            }
            PULL => {
                let i: PullInput = input(command, payload)?;
                crate::backend::valid_ref(&i.reference)?;
                let (containers, images) = (self.backend.containers().await?, self.backend.images().await?);
                let refused = match find_image(&images, &i.reference) {
                    Ok((image, _)) => self.policy.refuses_image(image, &yoke_images(&containers)),
                    Err(_) => self.policy.refuses_reference(&i.reference),
                };
                if let Some(e) = refused {
                    return Err(e);
                }
                self.backend.pull(&i.reference).await?;
                Ok(format!("{} pulled", i.reference))
            }
            IMAGE_REMOVE => {
                let i: ImageRemoveInput = input(command, payload)?;
                if i.id.is_empty() {
                    return Err(Error::Invalid("the payload names no image: {\"id\": \"<id or tag>\"}".into()));
                }
                let (containers, images) = (self.backend.containers().await?, self.backend.images().await?);
                let (image, name) = find_image(&images, &i.id)?;
                if let Some(e) = self.policy.refuses_image(image, &yoke_images(&containers)) {
                    return Err(e);
                }
                self.backend.remove_image(&name, i.force).await?;
                Ok(format!("{} removed", if name == image.id { short(&image.id).to_string() } else { name }))
            }
            other => Err(Error::Invalid(format!("{other} is not a command of {PLUGIN}"))),
        }
    }
}

fn status_body(st: &State) -> Value {
    match &st.engine {
        Some(e) => {
            let count = |state: &str| st.containers.iter().filter(|c| c.state == state).count();
            let (running, paused) = (count("running"), count("paused"));
            json!({
                "reachable": true, "reason": null,
                "engine": e.kind, "version": e.version, "api_version": e.api_version, "socket": e.socket,
                "rootless": e.rootless, "os": e.os,
                "containers": st.containers.len(), "running": running, "paused": paused,
                "stopped": st.containers.len() - running - paused, "images": st.images.len(),
            })
        }
        None => json!({
            "reachable": false, "reason": st.reason,
            "engine": null, "version": null, "api_version": null, "socket": null, "rootless": null, "os": null,
            "containers": null, "running": null, "paused": null, "stopped": null, "images": null,
        }),
    }
}

/// The images that containers Yoke runs were made from.
fn yoke_images(containers: &[Container]) -> HashSet<&str> {
    containers.iter().filter(|c| is_yoke(c)).map(|c| c.image_id.as_str()).collect()
}

/// A container by its id, its name, or a prefix of its id that names one alone.
pub fn find_container<'a>(list: &'a [Container], id: &str) -> Result<&'a Container, Error> {
    if let Some(c) = list.iter().find(|c| c.id == id || c.names.iter().any(|n| n == id)) {
        return Ok(c);
    }
    let by_prefix: Vec<&Container> = list.iter().filter(|c| c.id.starts_with(id)).collect();
    match by_prefix.as_slice() {
        [c] => Ok(c),
        [] => Err(Error::Engine(EngineError::NotFound(format!("no container {id}")))),
        _ => Err(Error::Invalid(format!("{id} begins the id of more than one container"))),
    }
}

/// An image by its id, a prefix of it that names one alone, or a tag, written whole or as the engine
/// shortens it (`alpine` for `docker.io/library/alpine:latest`); and what to name to the engine.
pub fn find_image<'a>(list: &'a [Image], name: &str) -> Result<(&'a Image, String), Error> {
    let id = name.strip_prefix("sha256:").unwrap_or(name);
    if let Some(i) = list.iter().find(|i| i.id == id) {
        return Ok((i, i.id.clone()));
    }
    let tagged = if name.rsplit('/').next().is_some_and(|last| last.contains(':') || last.contains('@')) {
        name.to_string()
    } else {
        format!("{name}:latest")
    };
    let spellings = [tagged.clone(), format!("docker.io/{tagged}"), format!("docker.io/library/{tagged}"), format!("localhost/{tagged}")];
    for i in list {
        if let Some(t) = i.tags.iter().find(|t| spellings.contains(t)) {
            return Ok((i, t.clone()));
        }
    }
    let by_prefix: Vec<&Image> = if id.len() >= 4 { list.iter().filter(|i| i.id.starts_with(id)).collect() } else { vec![] };
    match by_prefix.as_slice() {
        [i] => Ok((i, i.id.clone())),
        [] => Err(Error::Engine(EngineError::NotFound(format!("no image {name}")))),
        _ => Err(Error::Invalid(format!("{name} begins the id of more than one image"))),
    }
}

/// Answers a question or carries a command out, and says so on the Session.
pub async fn handle(u: &impl Acts, event: &Event, p: &Containers) -> Result<(), yoke_sdk::base::Error> {
    match event {
        Event::Question(q) if QUERIES.contains(&q.r#type.as_str()) => {
            match tokio::time::timeout(ANSWER_WAIT, p.answer(&q.r#type, &q.payload)).await {
                Ok(Ok(answer)) => u.answer(q, answer).await,
                Ok(Err(e)) => u.fail(q, code(&e), &message(&e)).await,
                Err(_) => u.fail(q, "backend", "the engine did not answer in time").await,
            }
        }
        Event::Command(c) if COMMANDS.contains(&c.r#type.as_str()) => {
            let job = p.act(&c.r#type, &c.payload);
            tokio::pin!(job);
            match tokio::time::timeout(COMMAND_WAIT, &mut job).await {
                Ok(Ok(line)) => u.ack(c, Outcome::Done, &line).await,
                Ok(Err(e)) => u.ack(c, Outcome::Failed, &format!("{}: {}", code(&e), message(&e))).await,
                Err(_) => {
                    let line = format!("still running after {} s: {JOB_FINISHED} says how it ends", COMMAND_WAIT.as_secs());
                    u.ack(c, Outcome::Accepted, &line).await?;
                    let (line, severity, failed) = match job.await {
                        Ok(line) => (line, 10, None),
                        Err(e) => (format!("{}: {}", code(&e), message(&e)), 50, Some(code(&e))),
                    };
                    let detail = json!({ "schema": SCHEMA, "line": line, "command": c.r#type, "command_id": c.id, "failed": failed });
                    u.report(JOB_FINISHED, severity, &line, detail).await
                }
            }
        }
        _ => Ok(()),
    }
}

/// What the engine's socket said, applied: the unit's health follows the engine reached or lost.
pub async fn on_said(u: &Unit, said: &Said, p: &Containers) -> (Dirty, Result<(), yoke_sdk::base::Error>) {
    match said {
        Said::Reached(info) => {
            let r = p.reached(u, info.clone()).await;
            let (grade, line) = p.health();
            (Dirty::default(), r.and(u.health(grade, &line).await))
        }
        Said::Lost(why) => {
            let r = p.lost(u, why).await;
            let (grade, line) = p.health();
            (Dirty::default(), r.and(u.health(grade, &line).await))
        }
        Said::Event(e) => p.event(u, e).await,
    }
}
