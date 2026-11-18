//! The host's container engine over its API socket: Podman's or Docker's, which speak the same API.
//!
//! [`Socket`] looks for the engine at fixed places — rootless Podman under `/run/user/<uid>`, rootful
//! Podman, Docker — or at the one the unit's arguments name, and follows the engine's own event stream:
//! it says when the engine was reached, when it was lost, and every event the engine reports. It reads
//! the containers and the images, and acts on them. Nothing here knows of Yoke.

use std::collections::BTreeMap;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::broadcast;

use crate::http::UnixHttp;

/// How long a read waits for the engine.
const READ: Duration = Duration::from_secs(20);
/// How long a change waits for the engine: a stop may take the container's grace period and more.
const CHANGE: Duration = Duration::from_secs(180);
/// How long a pull may take.
const PULL: Duration = Duration::from_secs(3600);
/// The most of a container's output read for one answer.
const MAX_LOGS: usize = 16 << 20;

#[derive(Debug, Clone, PartialEq)]
pub enum EngineError {
    /// No engine answers at the place, or it went away.
    Unavailable(String),
    NotFound(String),
    /// The engine refused it as it stands: a running container removed without force, an image in use.
    Conflict(String),
    /// The engine refused the request as malformed: a reference it cannot read.
    Invalid(String),
    /// The socket, or the engine, refused this account.
    PermissionDenied(String),
    Engine(String),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Unavailable(m)
            | EngineError::NotFound(m)
            | EngineError::Conflict(m)
            | EngineError::Invalid(m)
            | EngineError::PermissionDenied(m)
            | EngineError::Engine(m) => write!(f, "{m}"),
        }
    }
}

impl EngineError {
    fn from_io(e: &io::Error, what: &str) -> EngineError {
        match e.kind() {
            io::ErrorKind::PermissionDenied => EngineError::PermissionDenied(format!("{what}: {e}")),
            io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused | io::ErrorKind::UnexpectedEof => {
                EngineError::Unavailable(format!("{what}: {e}"))
            }
            _ => EngineError::Engine(format!("{what}: {e}")),
        }
    }

    /// An HTTP status and the engine's error body, `{"message": ...}`.
    pub fn from_status(status: u16, body: &[u8]) -> EngineError {
        let msg = serde_json::from_slice::<Value>(body)
            .ok()
            .and_then(|v| v.get("message").and_then(Value::as_str).map(str::to_string))
            .unwrap_or_else(|| String::from_utf8_lossy(body).trim().chars().take(300).collect());
        match status {
            401 | 403 => EngineError::PermissionDenied(msg),
            404 => EngineError::NotFound(msg),
            409 => EngineError::Conflict(msg),
            400 => EngineError::Invalid(msg),
            _ => EngineError::Engine(format!("the engine answered {status}: {msg}")),
        }
    }
}

/// The engine reached: what it is and where.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EngineInfo {
    /// `podman` or `docker`.
    pub kind: String,
    pub version: String,
    pub api_version: String,
    pub socket: String,
    pub rootless: bool,
    pub os: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Container {
    /// The full id, 64 hex characters.
    pub id: String,
    /// Without the leading slash the API gives them.
    pub names: Vec<String>,
    pub image: String,
    pub image_id: String,
    pub command: String,
    /// `created`, `running`, `paused`, `restarting`, `exited`, `dead`, `removing`.
    pub state: String,
    /// The engine's own words: `Up 2 hours (healthy)`.
    pub status: String,
    /// `healthy`, `unhealthy`, `starting`, or empty without a health check.
    pub health: String,
    /// Unix seconds.
    pub created: i64,
    /// `0.0.0.0:8080->80/tcp`, or `80/tcp` where nothing is published.
    pub ports: Vec<String>,
    pub labels: BTreeMap<String, String>,
}

impl Container {
    pub fn name(&self) -> &str {
        self.names.first().map(String::as_str).unwrap_or("")
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Image {
    /// Without `sha256:`.
    pub id: String,
    pub tags: Vec<String>,
    /// Bytes.
    pub size: i64,
    pub created: i64,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogLine {
    pub stderr: bool,
    pub text: String,
}

/// One event of the engine's: `container die`, `image pull`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EngineEvent {
    /// `container`, `image`, `volume`, `network`.
    pub kind: String,
    /// `start`, `die`, `health_status`, `oom`, `pull`, `delete`.
    pub action: String,
    /// What follows a colon in the action: `healthy` in `health_status: healthy`.
    pub detail: String,
    pub id: String,
    pub name: String,
    pub attributes: BTreeMap<String, String>,
}

/// What the engine's socket says.
#[derive(Debug, Clone, PartialEq)]
pub enum Said {
    /// Reached, and its event stream open: whatever was read before is to be read again.
    Reached(EngineInfo),
    /// Lost, or not found: why.
    Lost(String),
    Event(EngineEvent),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Start,
    Stop,
    Restart,
    Pause,
    Unpause,
    Kill,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Action::Start => "start",
            Action::Stop => "stop",
            Action::Restart => "restart",
            Action::Pause => "pause",
            Action::Unpause => "unpause",
            Action::Kill => "kill",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Container,
    Image,
}

/// One thing the account may or may not do with the engine, and how that was found.
#[derive(Debug, Clone, PartialEq)]
pub struct Permission {
    pub action: String,
    pub allowed: bool,
    pub detail: String,
}

#[async_trait]
pub trait ContainersBackend: Send + Sync + 'static {
    async fn containers(&self) -> Result<Vec<Container>, EngineError>;
    async fn images(&self) -> Result<Vec<Image>, EngineError>;
    /// The engine's own record of one container or image, as it gives it.
    async fn inspect(&self, kind: Kind, id: &str) -> Result<Value, EngineError>;
    /// The last `tail` lines of a container's output.
    async fn logs(&self, id: &str, tail: u32) -> Result<Vec<LogLine>, EngineError>;
    /// `timeout` is the grace a stop or a restart gives, in seconds.
    async fn act(&self, id: &str, action: Action, timeout: Option<u32>) -> Result<(), EngineError>;
    async fn remove(&self, id: &str, force: bool, volumes: bool) -> Result<(), EngineError>;
    async fn pull(&self, reference: &str) -> Result<(), EngineError>;
    /// An image by its id, or one of its tags, which takes that tag away.
    async fn remove_image(&self, name: &str, force: bool) -> Result<(), EngineError>;
    async fn permissions(&self) -> Vec<Permission>;
}

/// The places looked at when the unit names none, in this order.
pub fn default_places() -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Some(uid) = parse_proc_status(&std::fs::read_to_string("/proc/self/status").unwrap_or_default()).0 {
        v.push(PathBuf::from(format!("/run/user/{uid}/podman/podman.sock")));
    }
    v.push(PathBuf::from("/run/podman/podman.sock"));
    v.push(PathBuf::from("/var/run/docker.sock"));
    v
}

/// The engine behind a unix socket.
pub struct Socket {
    places: Vec<PathBuf>,
    current: Mutex<Option<UnixHttp>>,
}

impl Socket {
    /// Starts following the engine at the first of `places` that answers; what it says arrives on the
    /// receiver, beginning with `Reached` or `Lost`.
    pub fn start(places: Vec<PathBuf>) -> (Arc<Socket>, broadcast::Receiver<Said>) {
        let me = Arc::new(Socket { places, current: Mutex::new(None) });
        let (tx, rx) = broadcast::channel(1024);
        tokio::spawn(follow(me.clone(), tx));
        (me, rx)
    }

    fn places_text(&self) -> String {
        self.places.iter().map(|p| p.display().to_string()).collect::<Vec<_>>().join(", ")
    }

    fn http(&self) -> Result<UnixHttp, EngineError> {
        self.current
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| EngineError::Unavailable(format!("no container engine answers at {}", self.places_text())))
    }

    async fn get(&self, target: &str) -> Result<Value, EngineError> {
        let http = self.http()?;
        let (status, body) = http.request("GET", target, READ).await.map_err(|e| EngineError::from_io(&e, target))?;
        if status >= 400 {
            return Err(EngineError::from_status(status, &body));
        }
        json(&body)
    }

    async fn change(&self, method: &str, target: &str) -> Result<(), EngineError> {
        let http = self.http()?;
        let (status, body) = http.request(method, target, CHANGE).await.map_err(|e| EngineError::from_io(&e, target))?;
        if status >= 400 {
            return Err(EngineError::from_status(status, &body));
        }
        Ok(())
    }

    /// The first place that answers, and what answers there.
    async fn reach(&self) -> Result<(UnixHttp, EngineInfo), String> {
        let mut why = Vec::new();
        for place in &self.places {
            if std::fs::symlink_metadata(place).is_err() {
                why.push(format!("{}: no socket", place.display()));
                continue;
            }
            let http = UnixHttp::new(place);
            match engine_info(&http).await {
                Ok(info) => return Ok((http, info)),
                Err(e) => why.push(format!("{}: {e}", place.display())),
            }
        }
        Err(format!("no container engine answers ({})", why.join("; ")))
    }
}

async fn engine_info(http: &UnixHttp) -> Result<EngineInfo, EngineError> {
    let socket = http.path().display().to_string();
    let version = ping_get(http, "/version").await?;
    let info = ping_get(http, "/info").await?;
    Ok(parse_info(&version, &info, &socket))
}

async fn ping_get(http: &UnixHttp, target: &str) -> Result<Value, EngineError> {
    let what = http.path().display().to_string();
    let (status, body) = http.request("GET", target, Duration::from_secs(5)).await.map_err(|e| EngineError::from_io(&e, &what))?;
    if status >= 400 {
        return Err(EngineError::from_status(status, &body));
    }
    json(&body)
}

/// Keeps the engine's event stream open: reached, every event, lost, and reached again, waiting 1 s, then
/// longer up to 15 s, between attempts. Nothing is read again here: a `Reached` says to read it all.
async fn follow(me: Arc<Socket>, tx: broadcast::Sender<Said>) {
    let (mut backoff, mut down) = (1u64, false);
    loop {
        let why = match me.reach().await {
            Ok((http, info)) => match http.open("GET", "/events", None).await {
                Ok(mut events) if events.status < 400 => {
                    *me.current.lock().unwrap() = Some(http);
                    let _ = tx.send(Said::Reached(info));
                    backoff = 1;
                    down = false;
                    let mut js = JsonStream::default();
                    loop {
                        match events.next().await {
                            Ok(Some(chunk)) => {
                                for v in js.push(&chunk) {
                                    if let Some(e) = parse_event(&v) {
                                        let _ = tx.send(Said::Event(e));
                                    }
                                }
                            }
                            Ok(None) => break "the engine closed its event stream".to_string(),
                            Err(e) => break format!("the engine's event stream failed: {e}"),
                        }
                    }
                }
                Ok(mut events) => {
                    let body = events.collect().await.unwrap_or_default();
                    format!("the engine refused its event stream: {}", EngineError::from_status(events.status, &body))
                }
                Err(e) => format!("the engine's event stream: {e}"),
            },
            Err(why) => why,
        };
        *me.current.lock().unwrap() = None;
        if !down {
            let _ = tx.send(Said::Lost(why));
            down = true;
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(15);
    }
}

/// Percent-encodes a query value or a path segment.
fn enc(s: &str) -> String {
    let mut o = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            o.push(b as char);
        } else {
            o.push_str(&format!("%{b:02X}"));
        }
    }
    o
}

/// Refuses what could not be an id, a name or a reference before it reaches a URL.
pub fn valid_ref(r: &str) -> Result<(), EngineError> {
    let ok = !r.is_empty()
        && r.len() <= 512
        && !r.starts_with('-')
        && !r.contains("..")
        && r.chars().all(|c| c.is_ascii_alphanumeric() || "_.-:/@+".contains(c));
    if ok {
        Ok(())
    } else {
        Err(EngineError::Invalid(format!("'{r}' is not an id, a name or a reference")))
    }
}

/// `repo/name:tag` as (`repo/name`, `tag`); a digest stays in the name; the tag is `latest` where none is
/// given.
pub fn split_reference(r: &str) -> (String, String) {
    if r.contains('@') {
        return (r.to_string(), String::new());
    }
    match r.rfind(':') {
        Some(i) if !r[i + 1..].contains('/') => (r[..i].to_string(), r[i + 1..].to_string()),
        _ => (r.to_string(), "latest".to_string()),
    }
}

#[async_trait]
impl ContainersBackend for Socket {
    async fn containers(&self) -> Result<Vec<Container>, EngineError> {
        Ok(parse_containers(&self.get("/containers/json?all=1").await?))
    }

    async fn images(&self) -> Result<Vec<Image>, EngineError> {
        Ok(parse_images(&self.get("/images/json").await?))
    }

    async fn inspect(&self, kind: Kind, id: &str) -> Result<Value, EngineError> {
        valid_ref(id)?;
        match kind {
            Kind::Container => self.get(&format!("/containers/{}/json", enc(id))).await,
            Kind::Image => self.get(&format!("/images/{}/json", enc(id))).await,
        }
    }

    async fn logs(&self, id: &str, tail: u32) -> Result<Vec<LogLine>, EngineError> {
        valid_ref(id)?;
        let record = self.get(&format!("/containers/{}/json", enc(id))).await?;
        let tty = record.pointer("/Config/Tty").and_then(Value::as_bool).unwrap_or(false);
        let target = format!("/containers/{}/logs?stdout=1&stderr=1&follow=0&tail={tail}", enc(id));
        let http = self.http()?;
        let work = async {
            let mut r = http.open("GET", &target, None).await?;
            let mut body = Vec::new();
            while let Some(c) = r.next().await? {
                body.extend_from_slice(&c);
                if body.len() > MAX_LOGS {
                    break;
                }
            }
            Ok::<_, io::Error>((r.status, body))
        };
        let (status, body) = tokio::time::timeout(READ, work)
            .await
            .map_err(|_| EngineError::Engine("the engine gave no output in time".into()))?
            .map_err(|e| EngineError::from_io(&e, "the output"))?;
        if status >= 400 {
            return Err(EngineError::from_status(status, &body));
        }
        Ok(decode_logs(&body, tty))
    }

    async fn act(&self, id: &str, action: Action, timeout: Option<u32>) -> Result<(), EngineError> {
        valid_ref(id)?;
        let t = match (action, timeout) {
            (Action::Stop | Action::Restart, Some(t)) => format!("?t={t}"),
            _ => String::new(),
        };
        self.change("POST", &format!("/containers/{}/{}{t}", enc(id), action.as_str())).await
    }

    async fn remove(&self, id: &str, force: bool, volumes: bool) -> Result<(), EngineError> {
        valid_ref(id)?;
        self.change("DELETE", &format!("/containers/{}?force={}&v={}", enc(id), force as u8, volumes as u8)).await
    }

    async fn pull(&self, reference: &str) -> Result<(), EngineError> {
        valid_ref(reference)?;
        let (name, tag) = split_reference(reference);
        let target = format!("/images/create?fromImage={}&tag={}", enc(&name), enc(&tag));
        let http = self.http()?;
        let work = async {
            let mut r = http.open("POST", &target, None).await?;
            let body = r.collect().await?;
            Ok::<_, io::Error>((r.status, body))
        };
        let (status, body) = tokio::time::timeout(PULL, work)
            .await
            .map_err(|_| EngineError::Engine(format!("the pull did not end within {} s", PULL.as_secs())))?
            .map_err(|e| EngineError::from_io(&e, "the pull"))?;
        if status >= 400 {
            return Err(EngineError::from_status(status, &body));
        }
        // The progress is a stream of objects, and a failure one of them, after a 200.
        for v in JsonStream::default().push(&body) {
            if let Some(e) = v.get("error").and_then(Value::as_str) {
                return Err(if e.contains("not found") || e.contains("manifest unknown") {
                    EngineError::NotFound(e.to_string())
                } else {
                    EngineError::Engine(e.to_string())
                });
            }
        }
        Ok(())
    }

    async fn remove_image(&self, name: &str, force: bool) -> Result<(), EngineError> {
        valid_ref(name)?;
        self.change("DELETE", &format!("/images/{}?force={}", enc(name), force as u8)).await
    }

    async fn permissions(&self) -> Vec<Permission> {
        let places = self.places.clone();
        tokio::task::spawn_blocking(move || probe_permissions(&places)).await.unwrap_or_default()
    }
}

/// What this account may do at each place, read from the socket and the account's groups.
fn probe_permissions(places: &[PathBuf]) -> Vec<Permission> {
    let (_, gids) = parse_proc_status(&std::fs::read_to_string("/proc/self/status").unwrap_or_default());
    let groups = std::fs::read_to_string("/etc/group").unwrap_or_default();
    let mut out = Vec::new();
    for p in places {
        let action = format!("use {}", p.display());
        match std::fs::metadata(p) {
            Ok(md) => {
                let group = group_name(&groups, md.gid()).unwrap_or_else(|| format!("gid {}", md.gid()));
                let can = std::os::unix::net::UnixStream::connect(p).is_ok();
                let mut detail = format!(
                    "the socket is uid {}, group {group}, mode {:04o}; connecting {}",
                    md.uid(),
                    md.mode() & 0o7777,
                    if can { "succeeds" } else { "is refused to this account" }
                );
                if can && group == "docker" && gids.contains(&md.gid()) {
                    detail.push_str(
                        "; the account is in the docker group, which is as much as root: a container it starts may mount the host",
                    );
                }
                out.push(Permission { action, allowed: can, detail });
            }
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => out.push(Permission {
                action,
                allowed: false,
                detail: "the socket's directory is closed to this account".into(),
            }),
            Err(_) => out.push(Permission { action, allowed: false, detail: "no socket there: no engine serves it".into() }),
        }
    }
    out
}

/// The effective uid, and the groups, from `/proc/self/status`.
pub fn parse_proc_status(status: &str) -> (Option<u32>, Vec<u32>) {
    let mut uid = None;
    let mut gids = Vec::new();
    for l in status.lines() {
        if let Some(r) = l.strip_prefix("Uid:") {
            uid = r.split_whitespace().nth(1).and_then(|x| x.parse().ok());
        } else if let Some(r) = l.strip_prefix("Groups:") {
            gids.extend(r.split_whitespace().filter_map(|x| x.parse::<u32>().ok()));
        } else if let Some(r) = l.strip_prefix("Gid:") {
            gids.extend(r.split_whitespace().nth(1).and_then(|x| x.parse::<u32>().ok()));
        }
    }
    (uid, gids)
}

fn group_name(group_file: &str, gid: u32) -> Option<String> {
    group_file.lines().find_map(|l| {
        let mut f = l.split(':');
        let name = f.next()?;
        (f.nth(1)?.parse::<u32>().ok()? == gid).then(|| name.to_string())
    })
}

// What the engine answers, read.

fn s(v: &Value, key: &str) -> String {
    v.get(key).and_then(Value::as_str).unwrap_or("").to_string()
}

fn n(v: &Value, key: &str) -> i64 {
    v.get(key).and_then(|x| x.as_i64().or_else(|| x.as_f64().map(|f| f as i64))).unwrap_or(0)
}

fn labels(v: &Value) -> BTreeMap<String, String> {
    v.get("Labels")
        .and_then(Value::as_object)
        .map(|o| o.iter().map(|(k, x)| (k.clone(), x.as_str().unwrap_or("").to_string())).collect())
        .unwrap_or_default()
}

fn json(body: &[u8]) -> Result<Value, EngineError> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(Value::Null);
    }
    serde_json::from_slice(body).map_err(|e| EngineError::Engine(format!("the engine answered what is not JSON: {e}")))
}

pub fn norm_id(id: &str) -> &str {
    id.strip_prefix("sha256:").unwrap_or(id)
}

pub fn parse_info(version: &Value, info: &Value, socket: &str) -> EngineInfo {
    let podman = version
        .get("Components")
        .and_then(Value::as_array)
        .is_some_and(|c| c.iter().any(|x| s(x, "Name").contains("Podman")));
    let rootless = info
        .get("SecurityOptions")
        .and_then(Value::as_array)
        .is_some_and(|o| o.iter().filter_map(Value::as_str).any(|o| o.contains("rootless")));
    EngineInfo {
        kind: if podman { "podman" } else { "docker" }.into(),
        version: s(version, "Version"),
        api_version: s(version, "ApiVersion"),
        socket: socket.into(),
        rootless,
        os: s(info, "OperatingSystem"),
    }
}

/// `(healthy)`, `(unhealthy)`, `(health: starting)` in the engine's words.
fn health(status: &str) -> String {
    if status.contains("(unhealthy)") {
        "unhealthy"
    } else if status.contains("(healthy)") {
        "healthy"
    } else if status.contains("(health: starting)") {
        "starting"
    } else {
        ""
    }
    .into()
}

fn ports(v: &Value) -> Vec<String> {
    let mut out: Vec<String> = v
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|p| {
            let private = n(p, "PrivatePort");
            let proto = match s(p, "Type") {
                t if t.is_empty() => "tcp".to_string(),
                t => t,
            };
            if private <= 0 {
                return None;
            }
            Some(match n(p, "PublicPort") {
                public if public > 0 => {
                    let ip = match s(p, "IP") {
                        ip if ip.is_empty() => "0.0.0.0".to_string(),
                        ip => ip,
                    };
                    format!("{ip}:{public}->{private}/{proto}")
                }
                _ => format!("{private}/{proto}"),
            })
        })
        .collect();
    out.dedup();
    out
}

/// `GET /containers/json?all=1`.
pub fn parse_containers(v: &Value) -> Vec<Container> {
    v.as_array()
        .into_iter()
        .flatten()
        .map(|c| {
            let names = c
                .get("Names")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(|n| n.trim_start_matches('/').to_string())
                .collect();
            let state = s(c, "State").to_lowercase();
            let status = match s(c, "Status") {
                st if st.is_empty() => state.clone(),
                st => st,
            };
            Container {
                id: s(c, "Id"),
                names,
                image: s(c, "Image"),
                image_id: norm_id(&s(c, "ImageID")).to_string(),
                command: s(c, "Command"),
                health: health(&status),
                state,
                status,
                created: n(c, "Created"),
                ports: ports(c.get("Ports").unwrap_or(&Value::Null)),
                labels: labels(c),
            }
        })
        .filter(|c| !c.id.is_empty())
        .collect()
}

/// `GET /images/json`.
pub fn parse_images(v: &Value) -> Vec<Image> {
    v.as_array()
        .into_iter()
        .flatten()
        .map(|i| Image {
            id: norm_id(&s(i, "Id")).to_string(),
            tags: i
                .get("RepoTags")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .filter(|t| *t != "<none>:<none>")
                .map(str::to_string)
                .collect(),
            size: n(i, "Size"),
            created: n(i, "Created"),
            labels: labels(i),
        })
        .filter(|i| !i.id.is_empty())
        .collect()
}

/// One object of `GET /events`.
pub fn parse_event(v: &Value) -> Option<EngineEvent> {
    let kind = s(v, "Type");
    let full = match s(v, "Action") {
        a if a.is_empty() => s(v, "status"),
        a => a,
    };
    if kind.is_empty() || full.is_empty() {
        return None;
    }
    let (action, mut detail) = match full.split_once(':') {
        Some((a, d)) => (a.trim().to_string(), d.trim().to_string()),
        None => (full, String::new()),
    };
    let attributes: BTreeMap<String, String> = v
        .pointer("/Actor/Attributes")
        .and_then(Value::as_object)
        .map(|o| o.iter().map(|(k, x)| (k.clone(), x.as_str().map(str::to_string).unwrap_or_else(|| x.to_string()))).collect())
        .unwrap_or_default();
    if action == "health_status" && detail.is_empty() {
        detail = attributes.get("health_status").cloned().unwrap_or_default();
    }
    let id = v.pointer("/Actor/ID").and_then(Value::as_str).map(str::to_string).unwrap_or_else(|| s(v, "id"));
    let name = attributes.get("name").cloned().unwrap_or_default();
    Some(EngineEvent { kind, action, detail, id, name, attributes })
}

/// A container's output: framed as stdout and stderr, or, for a terminal, raw.
pub fn decode_logs(body: &[u8], tty: bool) -> Vec<LogLine> {
    let mut pieces: Vec<(bool, &[u8])> = Vec::new();
    let mut rest = body;
    let framed = !tty && body.len() >= 8 && body[0] <= 2 && body[1..4] == [0, 0, 0];
    if framed {
        while rest.len() >= 8 && rest[0] <= 2 && rest[1..4] == [0, 0, 0] {
            let size = u32::from_be_bytes([rest[4], rest[5], rest[6], rest[7]]) as usize;
            let end = (8 + size).min(rest.len());
            pieces.push((rest[0] == 2, &rest[8..end]));
            rest = &rest[end..];
        }
    }
    if !rest.is_empty() {
        pieces.push((false, rest));
    }
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let mut lines = Vec::new();
    let flush = |buf: &mut Vec<u8>, stderr: bool, lines: &mut Vec<LogLine>| {
        let mut text = String::from_utf8_lossy(buf).into_owned();
        if text.ends_with('\r') {
            text.pop();
        }
        lines.push(LogLine { stderr, text });
        buf.clear();
    };
    for (stderr, piece) in pieces {
        let buf = if stderr { &mut err } else { &mut out };
        for &c in piece {
            if c == b'\n' {
                flush(buf, stderr, &mut lines);
            } else {
                buf.push(c);
            }
        }
    }
    if !out.is_empty() {
        flush(&mut out, false, &mut lines);
    }
    if !err.is_empty() {
        flush(&mut err, true, &mut lines);
    }
    lines
}

/// Reads JSON values one after another as their bytes arrive.
#[derive(Default)]
pub struct JsonStream {
    buf: Vec<u8>,
}

impl JsonStream {
    pub fn push(&mut self, data: &[u8]) -> Vec<Value> {
        self.buf.extend_from_slice(data);
        let mut out = Vec::new();
        let consumed = {
            let mut it = serde_json::Deserializer::from_slice(&self.buf).into_iter::<Value>();
            let (mut end, mut broken) = (0, false);
            // The offset is read after each value, so the iterator is not lent to a `for`.
            #[allow(clippy::while_let_on_iterator)]
            while let Some(r) = it.next() {
                match r {
                    Ok(v) => {
                        out.push(v);
                        end = it.byte_offset();
                    }
                    Err(e) => {
                        broken = !e.is_eof();
                        break;
                    }
                }
            }
            // What cannot be read is dropped, so that one bad value does not stop every later one.
            if broken { self.buf.len() } else { end }
        };
        self.buf.drain(..consumed);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn what_the_engines_answer_is_read() {
        let docker = json!([{"Id":"167ea0b4cbb8aaaa1111222233334444555566667777888899990000aaaabbbb","Names":["/web"],
            "Image":"nginx:1.27","ImageID":"sha256:0123","Command":"nginx -g 'daemon off;'","Created":1755000000,
            "Ports":[{"IP":"0.0.0.0","PrivatePort":80,"PublicPort":8080,"Type":"tcp"},{"IP":"0.0.0.0","PrivatePort":80,"PublicPort":8080,"Type":"tcp"},{"PrivatePort":53,"Type":"udp"}],
            "Labels":{"a":"b"},"State":"running","Status":"Up 8 hours (healthy)"}]);
        let c = &parse_containers(&docker)[0];
        assert_eq!((c.name(), c.state.as_str(), c.health.as_str(), c.image_id.as_str()), ("web", "running", "healthy", "0123"));
        assert_eq!(c.ports, vec!["0.0.0.0:8080->80/tcp", "53/udp"]);
        let images = parse_images(&json!([{"Id":"sha256:feb5","RepoTags":["<none>:<none>"],"Size":5,"Created":1,"Labels":null}]));
        assert!(images[0].tags.is_empty());
        assert_eq!(images[0].id, "feb5");
        let info = parse_info(
            &json!({"Version":"6.2.0","ApiVersion":"1.44","Components":[{"Name":"Podman Engine"}]}),
            &json!({"SecurityOptions":["name=seccomp","name=rootless"],"OperatingSystem":"arch"}),
            "/run/user/1000/podman/podman.sock",
        );
        assert_eq!((info.kind.as_str(), info.rootless), ("podman", true));
    }

    #[test]
    fn an_event_is_read_whichever_engine_says_it() {
        let e = parse_event(&json!({"Type":"container","Action":"health_status: unhealthy","Actor":{"ID":"abc","Attributes":{"name":"web"}}})).unwrap();
        assert_eq!((e.action.as_str(), e.detail.as_str(), e.name.as_str()), ("health_status", "unhealthy", "web"));
        let p = parse_event(&json!({"Type":"container","Action":"health_status","Actor":{"ID":"abc","Attributes":{"name":"web","health_status":"healthy"}}})).unwrap();
        assert_eq!(p.detail, "healthy");
        assert!(parse_event(&json!({})).is_none());
    }

    #[test]
    fn output_is_split_into_lines_framed_or_raw() {
        let frame = |ty: u8, p: &[u8]| {
            let mut v = vec![ty, 0, 0, 0];
            v.extend_from_slice(&(p.len() as u32).to_be_bytes());
            v.extend_from_slice(p);
            v
        };
        let mut body = frame(1, b"hello\n");
        body.extend(frame(2, b"oops\n"));
        body.extend(frame(1, b"last"));
        let lines = decode_logs(&body, false);
        assert_eq!(lines.iter().map(|l| (l.stderr, l.text.as_str())).collect::<Vec<_>>(), vec![(false, "hello"), (true, "oops"), (false, "last")]);
        assert_eq!(decode_logs(b"one\r\ntwo\n", true).len(), 2);
    }

    #[test]
    fn json_values_are_read_as_they_arrive() {
        let mut js = JsonStream::default();
        assert_eq!(js.push(b"{\"a\":1}\n{\"b\":").len(), 1);
        assert_eq!(js.push(b"2}\n{\"c\":3}").len(), 2);
    }

    #[test]
    fn references_are_split_and_checked() {
        assert_eq!(split_reference("alpine"), ("alpine".into(), "latest".into()));
        assert_eq!(split_reference("localhost:5000/app:v1"), ("localhost:5000/app".into(), "v1".into()));
        assert!(valid_ref("docker.io/library/alpine:3.20").is_ok());
        assert!(valid_ref("--all").is_err() && valid_ref("a b").is_err() && valid_ref("../x").is_err());
        assert_eq!(enc("a b/c"), "a%20b%2Fc");
    }

    #[test]
    fn the_account_is_read_from_proc() {
        let (uid, gids) = parse_proc_status("Uid:\t1000\t1000\t1000\t1000\nGid:\t1000\t1000\t1000\t1000\nGroups:\t5 965\n");
        assert_eq!(uid, Some(1000));
        assert!(gids.contains(&965) && gids.contains(&1000));
        assert_eq!(group_name("root:x:0:\ndocker:x:965:someone\n", 965).as_deref(), Some("docker"));
    }
}
