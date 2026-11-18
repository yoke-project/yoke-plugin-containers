//! The Plugin against an engine that is not there: a fake backend stands in for it, and a recorder for
//! the Core. Nothing here reaches a real engine.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use containers::backend::*;
use containers::plugin::*;
use serde_json::{json, Value};
use yoke_sdk::plugin::{Command, Event, Outcome, Question};

const WEB: &str = "aaaa0000111122223333444455556666777788889999aaaabbbbccccddddeeee";
const DB: &str = "aaab0000111122223333444455556666777788889999aaaabbbbccccddddeeee";
const CORE: &str = "c0de0000111122223333444455556666777788889999aaaabbbbccccddddeeee";

fn container(id: &str, name: &str, image_id: &str, state: &str, labels: &[(&str, &str)]) -> Container {
    Container {
        id: id.into(),
        names: vec![name.into()],
        image: format!("{name}:latest"),
        image_id: image_id.into(),
        state: state.into(),
        status: if state == "running" { "Up 2 minutes".into() } else { "Exited (0) 1 minute ago".into() },
        labels: labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        ..Default::default()
    }
}

fn image(id: &str, tag: &str) -> Image {
    Image { id: id.into(), tags: vec![tag.into()], size: 1000, created: 1, labels: BTreeMap::new() }
}

/// A host's engine: a web server running, a database stopped, and a unit Yoke runs. It records every
/// change asked of it; `busy` is refused as a conflict, and `slow` takes a minute.
struct Fake {
    containers: Mutex<Vec<Container>>,
    images: Mutex<Vec<Image>>,
    done: Mutex<Vec<String>>,
}

impl Fake {
    fn new() -> Arc<Fake> {
        Arc::new(Fake {
            containers: Mutex::new(vec![
                container(WEB, "web", "1111", "running", &[]),
                container(DB, "db", "2222", "exited", &[]),
                container(CORE, "panel", "3333", "running", &[("dev.yoke-project.unit", "panel")]),
                container("5105", "slow", "1111", "running", &[]),
                container("b05e", "busy", "1111", "running", &[]),
            ]),
            images: Mutex::new(vec![
                image("1111", "docker.io/library/nginx:latest"),
                image("2222", "docker.io/library/postgres:17"),
                image("3333", "localhost/yoke-panel:latest"),
                image("4444", "docker.io/library/alpine:latest"),
            ]),
            done: Mutex::new(Vec::new()),
        })
    }

    fn done(&self) -> Vec<String> {
        std::mem::take(&mut *self.done.lock().unwrap())
    }

    async fn change(&self, what: String, id: &str) -> Result<(), EngineError> {
        if id == "b05e" {
            return Err(EngineError::Conflict("container is running: stop it before removing it".into()));
        }
        if id == "5105" {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
        self.done.lock().unwrap().push(what);
        Ok(())
    }
}

#[async_trait]
impl ContainersBackend for Fake {
    async fn containers(&self) -> Result<Vec<Container>, EngineError> {
        Ok(self.containers.lock().unwrap().clone())
    }
    async fn images(&self) -> Result<Vec<Image>, EngineError> {
        Ok(self.images.lock().unwrap().clone())
    }
    async fn inspect(&self, kind: Kind, id: &str) -> Result<Value, EngineError> {
        match kind {
            Kind::Container if id == "web" => Ok(json!({"Id": WEB, "Config": {"Tty": false}})),
            Kind::Image if id == "4444" => Ok(json!({"Id": "sha256:4444", "Architecture": "amd64"})),
            _ => Err(EngineError::NotFound(format!("no such object: {id}"))),
        }
    }
    async fn logs(&self, id: &str, tail: u32) -> Result<Vec<LogLine>, EngineError> {
        if id != "web" {
            return Err(EngineError::NotFound(format!("no container {id}")));
        }
        let all: Vec<LogLine> = (1..=10).map(|n| LogLine { stderr: n % 5 == 0, text: format!("line {n}") }).collect();
        Ok(all[all.len().saturating_sub(tail as usize)..].to_vec())
    }
    async fn act(&self, id: &str, action: Action, timeout: Option<u32>) -> Result<(), EngineError> {
        let t = timeout.map(|t| format!(" t={t}")).unwrap_or_default();
        self.change(format!("{} {}{t}", action.as_str(), &id[..4]), id).await
    }
    async fn remove(&self, id: &str, force: bool, volumes: bool) -> Result<(), EngineError> {
        self.change(format!("remove {} force={force} volumes={volumes}", &id[..4]), id).await
    }
    async fn pull(&self, reference: &str) -> Result<(), EngineError> {
        self.change(format!("pull {reference}"), reference).await
    }
    async fn remove_image(&self, name: &str, force: bool) -> Result<(), EngineError> {
        self.change(format!("remove-image {name} force={force}"), name).await
    }
    async fn permissions(&self) -> Vec<Permission> {
        vec![Permission { action: "use /run/podman/podman.sock".into(), allowed: true, detail: "connecting succeeds".into() }]
    }
}

#[derive(Default)]
struct Core {
    said: Mutex<Vec<String>>,
}

impl Core {
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.said.lock().unwrap())
    }
    // The error is the library's own, as large as it is.
    #[allow(clippy::result_large_err)]
    fn push(&self, line: String) -> Result<(), yoke_sdk::base::Error> {
        self.said.lock().unwrap().push(line);
        Ok(())
    }
}

impl Acts for Core {
    async fn answer(&self, _q: &Question, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        assert!(payload.len() <= MAX_PAYLOAD);
        self.push(format!("answer {}", String::from_utf8_lossy(&payload)))
    }
    async fn fail(&self, _q: &Question, code: &str, message: &str) -> Result<(), yoke_sdk::base::Error> {
        self.push(format!("fail {code}: {message}"))
    }
    async fn ack(&self, _c: &Command, outcome: Outcome, line: &str) -> Result<(), yoke_sdk::base::Error> {
        self.push(format!("ack {outcome:?}: {line}"))
    }
    async fn report(&self, occurrence: &str, severity: u8, line: &str, detail: Value) -> Result<(), yoke_sdk::base::Error> {
        assert_eq!(detail["line"], line, "the detail carries the line a client reads");
        assert_eq!(detail["schema"], SCHEMA);
        self.push(format!("report {occurrence} {severity}: {line}"))
    }
    async fn emit(&self, stream: &str, payload: Vec<u8>) -> Result<(), yoke_sdk::base::Error> {
        assert!(payload.len() <= MAX_PAYLOAD);
        self.push(format!("emit {stream} {}", String::from_utf8_lossy(&payload)))
    }
}

fn question(r#type: &str, payload: &str) -> Event {
    Event::Question(Question { id: "q-1".into(), r#type: r#type.into(), payload: payload.as_bytes().to_vec() })
}

fn command(r#type: &str, payload: &str) -> Event {
    Event::Command(Command { id: "c-1".into(), r#type: r#type.into(), payload: payload.as_bytes().to_vec() })
}

fn json_of(line: &str, skip: usize) -> Value {
    serde_json::from_str(line.splitn(skip + 1, ' ').nth(skip).unwrap()).unwrap()
}

fn engine() -> EngineInfo {
    EngineInfo {
        kind: "podman".into(),
        version: "6.2.0".into(),
        api_version: "1.44".into(),
        socket: "/run/user/1000/podman/podman.sock".into(),
        rootless: true,
        os: "linux".into(),
    }
}

fn event(kind: &str, action: &str, detail: &str, name: &str, attributes: &[(&str, &str)]) -> EngineEvent {
    let mut attributes: BTreeMap<String, String> = attributes.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    attributes.insert("name".into(), name.into());
    EngineEvent { kind: kind.into(), action: action.into(), detail: detail.into(), id: WEB.into(), name: name.into(), attributes }
}

/// The Plugin with the engine reached, and nothing said yet.
async fn reached(fake: &Arc<Fake>, policy: Policy) -> (Arc<Containers>, Core) {
    let (p, core) = (Containers::new(fake.clone(), policy), Core::default());
    p.reached(&core, engine()).await.unwrap();
    assert!(core.take().is_empty(), "nothing flows yet");
    (p, core)
}

async fn ask(p: &Containers, core: &Core, q: &str, payload: &str) -> String {
    handle(core, &question(q, payload), p).await.unwrap();
    core.take().remove(0)
}

async fn run(p: &Containers, core: &Core, c: &str, payload: &str) -> String {
    handle(core, &command(c, payload), p).await.unwrap();
    core.take().remove(0)
}

#[test]
fn the_manifest_kept_is_the_manifest_declared() {
    assert_eq!(declaration().manifest(), include_str!("../com.yoke.containers.yaml"));
}

#[test]
fn every_object_declared_has_one_capability() {
    let d = declaration();
    assert_eq!(d.capabilities.len(), d.queries.len() + d.commands.len() + d.streams.len() + d.occurrences.len());
}

#[tokio::test]
async fn the_status_says_the_engine_reached_and_lost_each_counted_once() {
    let (fake, core) = (Fake::new(), Core::default());
    let p = Containers::new(fake.clone(), Policy::default());
    let s = json_of(&ask(&p, &core, STATUS, "").await, 1);
    assert_eq!((s["sequence"].as_u64(), s["reachable"].as_bool()), (Some(0), Some(false)));

    p.activated(&core, STATUS).await.unwrap();
    assert_eq!(json_of(&core.take()[0], 2)["sequence"], 0, "an activated stream is given the present at once");
    p.lost(&core, "no socket at /run/podman/podman.sock").await.unwrap();
    let s = json_of(&core.take()[0], 2);
    assert_eq!((s["sequence"].as_u64(), s["reason"].as_str()), (Some(1), Some("no socket at /run/podman/podman.sock")));
    p.lost(&core, "no socket at /run/podman/podman.sock").await.unwrap();
    assert!(core.take().is_empty(), "the same reason is no change");
    assert!(p.health().0 == 50);

    p.reached(&core, engine()).await.unwrap();
    let s = json_of(&core.take()[0], 2);
    assert_eq!(s["sequence"], 2);
    assert_eq!((s["engine"].as_str(), s["rootless"].as_bool()), (Some("podman"), Some(true)));
    assert_eq!((s["containers"].as_u64(), s["running"].as_u64(), s["stopped"].as_u64(), s["images"].as_u64()), (Some(5), Some(4), Some(1), Some(4)));
    assert_eq!(p.health(), (0, "podman 6.2.0 at /run/user/1000/podman/podman.sock, rootless".to_string()));
    assert_eq!(json_of(&ask(&p, &core, STATUS, "{}").await, 1)["sequence"], 2, "the question states the sequence the stream is at");
}

#[tokio::test]
async fn without_the_engine_the_lists_are_failed_with_the_reason() {
    let (fake, core) = (Fake::new(), Core::default());
    let p = Containers::new(fake, Policy::default());
    p.lost(&core, "no container engine answers").await.unwrap();
    assert_eq!(ask(&p, &core, LIST, "").await, "fail unavailable: no container engine: no container engine answers");
    assert!(ask(&p, &core, IMAGES, "").await.starts_with("fail unavailable: "));
    p.activated(&core, LIST).await.unwrap();
    assert!(core.take().is_empty(), "a list never read is not given");
}

#[tokio::test]
async fn the_lists_are_answered_with_their_sequence_and_flow_only_when_they_change() {
    let fake = Fake::new();
    let (p, core) = reached(&fake, Policy::default()).await;
    let list = json_of(&ask(&p, &core, LIST, "").await, 1);
    assert_eq!((list["sequence"].as_u64(), list["truncated"].as_bool()), (Some(1), Some(false)));
    let names: Vec<&str> = list["containers"].as_array().unwrap().iter().map(|c| c["name"].as_str().unwrap()).collect();
    assert_eq!(names, ["web", "db", "panel", "slow", "busy"]);
    assert_eq!((list["containers"][2]["yoke"].as_bool(), list["containers"][2]["protected"].as_bool()), (Some(true), Some(true)));
    assert_eq!(list["containers"][0]["protected"], false);
    let images = json_of(&ask(&p, &core, IMAGES, "").await, 1);
    assert_eq!(images["sequence"], 1);
    assert_eq!((images["images"][0]["containers"].as_u64(), images["images"][3]["containers"].as_u64()), (Some(3), Some(0)));
    assert_eq!(images["images"][2]["protected"], true, "the image a unit of Yoke runs is protected with it");

    for s in STREAMS {
        p.activated(&core, s).await.unwrap();
    }
    assert_eq!(core.take().len(), 3, "each stream is given the present at once");

    // An event that changes nothing a list shows is read again, and nothing flows.
    let (dirty, _) = p.event(&core, &event("container", "exec_start", "", "web", &[])).await;
    assert!(!dirty.any());
    let (dirty, _) = p.event(&core, &event("container", "rename", "", "web", &[])).await;
    assert_eq!(dirty, Dirty { containers: true, images: false });
    p.refresh(&core, dirty).await.unwrap();
    assert!(core.take().is_empty(), "the same list is no change");

    // The database starts: the list and the status change; the images do not.
    fake.containers.lock().unwrap()[1].state = "running".into();
    p.refresh(&core, dirty).await.unwrap();
    let said = core.take();
    assert_eq!(said.len(), 2, "{said:?}");
    assert!(said[0].starts_with(&format!("emit {LIST} ")));
    assert_eq!(json_of(&said[0], 2)["sequence"], 2);
    assert_eq!(json_of(&said[1], 2)["running"], 5);

    // A pull is an image event: the images are read again.
    fake.images.lock().unwrap().push(image("5555", "docker.io/library/busybox:latest"));
    let (dirty, _) = p.event(&core, &event("image", "pull", "", "busybox", &[])).await;
    assert_eq!(dirty, Dirty { containers: false, images: true });
    p.refresh(&core, dirty).await.unwrap();
    let said = core.take();
    assert_eq!(json_of(&said[0], 2)["sequence"], 2);
    assert_eq!(json_of(&said[0], 2)["images"].as_array().unwrap().len(), 5);

    // A stream stopped flows no more, and is still counted.
    p.stopped(LIST);
    fake.containers.lock().unwrap().remove(1);
    p.refresh(&core, Dirty::ALL).await.unwrap();
    let said = core.take();
    assert!(said.iter().all(|l| !l.starts_with(&format!("emit {LIST} "))), "{said:?}");
    assert_eq!(json_of(&ask(&p, &core, LIST, "").await, 1)["sequence"], 3);
}

#[tokio::test]
async fn an_interface_that_attaches_late_drops_what_it_was_answered() {
    let fake = Fake::new();
    let (p, core) = reached(&fake, Policy::default()).await;
    // It subscribes, the stream starts and is given the present, a change lands, then it asks.
    p.activated(&core, LIST).await.unwrap();
    fake.containers.lock().unwrap()[0].state = "paused".into();
    p.refresh(&core, Dirty::ALL).await.unwrap();
    let frames: Vec<u64> = core.take().iter().filter(|l| l.starts_with("emit containers.list ")).map(|l| json_of(l, 2)["sequence"].as_u64().unwrap()).collect();
    let answered = json_of(&ask(&p, &core, LIST, "").await, 1)["sequence"].as_u64().unwrap();
    assert_eq!(frames, [1, 2]);
    assert_eq!(answered, 2);
    assert!(frames.iter().all(|f| *f <= answered), "every frame it holds is at or below the answer, and dropped");
}

#[tokio::test]
async fn the_engines_events_are_reported_as_occurrences() {
    let fake = Fake::new();
    let (p, core) = reached(&fake, Policy::default()).await;
    let image = [("image", "nginx:latest")];
    for e in [
        event("container", "start", "", "web", &image),
        event("container", "die", "", "web", &[("image", "nginx:latest"), ("exitCode", "0")]),
        event("container", "die", "", "web", &[("image", "nginx:latest"), ("exitCode", "137")]),
        event("container", "health_status", "unhealthy", "web", &image),
        event("container", "health_status", "healthy", "web", &image),
        event("container", "oom", "", "web", &image),
        event("network", "connect", "", "web", &[]),
    ] {
        let (_, r) = p.event(&core, &e).await;
        r.unwrap();
    }
    assert_eq!(
        core.take(),
        [
            format!("report {STARTED} 10: web started"),
            format!("report {EXITED} 10: web exited with 0"),
            format!("report {EXITED} 30: web exited with 137"),
            format!("report {UNHEALTHY} 50: web is unhealthy"),
            format!("report {OOM} 70: web ran out of memory"),
        ]
    );
}

#[tokio::test]
async fn a_container_is_acted_on_by_its_name_or_its_id() {
    let fake = Fake::new();
    let (p, core) = reached(&fake, Policy::default()).await;
    assert_eq!(run(&p, &core, START, r#"{"id":"db"}"#).await, "ack Done: db started");
    assert_eq!(run(&p, &core, STOP, r#"{"id":"web","timeout":3}"#).await, "ack Done: web stopped");
    assert_eq!(run(&p, &core, RESTART, &format!(r#"{{"id":"{WEB}"}}"#)).await, "ack Done: web restarted");
    assert_eq!(run(&p, &core, PAUSE, r#"{"id":"aaaa"}"#).await, "ack Done: web paused");
    assert_eq!(run(&p, &core, UNPAUSE, r#"{"id":"web"}"#).await, "ack Done: web unpaused");
    assert_eq!(run(&p, &core, KILL, r#"{"id":"web"}"#).await, "ack Done: web killed");
    assert_eq!(run(&p, &core, REMOVE, r#"{"id":"db","volumes":true}"#).await, "ack Done: db removed with its anonymous volumes");
    assert_eq!(
        fake.done(),
        ["start aaab", "stop aaaa t=3", "restart aaaa", "pause aaaa", "unpause aaaa", "kill aaaa", "remove aaab force=false volumes=true"]
    );
}

#[tokio::test]
async fn what_cannot_be_done_is_failed_with_its_reason() {
    let fake = Fake::new();
    let (p, core) = reached(&fake, Policy::default()).await;
    assert_eq!(run(&p, &core, START, r#"{"id":"nobody"}"#).await, "ack Failed: not_found: no container nobody");
    assert_eq!(run(&p, &core, START, r#"{"id":"aaa"}"#).await, "ack Failed: invalid: aaa begins the id of more than one container");
    assert!(run(&p, &core, START, r#"{"id":"web","timeout":3}"#).await.starts_with("ack Failed: invalid: the payload is not what containers.start takes"));
    assert!(run(&p, &core, STOP, "not json").await.starts_with("ack Failed: invalid: the payload is not what containers.stop takes: it is not JSON"));
    assert!(run(&p, &core, REMOVE, r#"{"id":""}"#).await.starts_with("ack Failed: invalid: the payload names no container"));
    assert_eq!(run(&p, &core, REMOVE, r#"{"id":"busy"}"#).await, "ack Failed: conflict: container is running: stop it before removing it");
    assert!(fake.done().is_empty());
}

#[tokio::test]
async fn what_yoke_runs_is_never_acted_on() {
    let fake = Fake::new();
    let (p, core) = reached(&fake, Policy::default()).await;
    for c in [STOP, KILL, REMOVE] {
        assert_eq!(run(&p, &core, c, r#"{"id":"panel"}"#).await, "ack Failed: protected: panel is run by Yoke, and is never acted on");
    }
    assert_eq!(run(&p, &core, STOP, &format!(r#"{{"id":"{}"}}"#, &CORE[..12])).await, "ack Failed: protected: panel is run by Yoke, and is never acted on");
    assert_eq!(
        run(&p, &core, IMAGE_REMOVE, r#"{"id":"localhost/yoke-panel:latest","force":true}"#).await,
        "ack Failed: protected: localhost/yoke-panel:latest is an image Yoke runs, and is never acted on"
    );
    // Whatever the application allows.
    let (p, core) = reached(&fake, Policy::from_args(["--allow", "*", "--allow-image", "*"].map(String::from))).await;
    assert!(run(&p, &core, STOP, r#"{"id":"panel"}"#).await.starts_with("ack Failed: protected: "));
    assert!(fake.done().is_empty());
}

#[tokio::test]
async fn the_application_names_what_commands_may_act_on() {
    let fake = Fake::new();
    let args = ["--allow", "w*", "--allow", "db", "--protect", "db", "--allow-image", "docker.io/library/*", "--protect-image", "*postgres*"];
    let (p, core) = reached(&fake, Policy::from_args(args.map(String::from))).await;
    assert_eq!(run(&p, &core, STOP, r#"{"id":"web"}"#).await, "ack Done: web stopped");
    assert_eq!(run(&p, &core, START, r#"{"id":"db"}"#).await, "ack Failed: protected: db is protected by the application, and is never acted on");
    assert_eq!(run(&p, &core, STOP, r#"{"id":"slow"}"#).await, "ack Failed: not_allowed: slow is not among what the application allows");
    assert_eq!(run(&p, &core, IMAGE_REMOVE, r#"{"id":"alpine"}"#).await, "ack Done: docker.io/library/alpine:latest removed");
    assert_eq!(
        run(&p, &core, IMAGE_REMOVE, r#"{"id":"postgres:17"}"#).await,
        "ack Failed: protected: docker.io/library/postgres:17 is protected by the application, and is never acted on"
    );
    assert_eq!(run(&p, &core, PULL, r#"{"reference":"docker.io/library/busybox:1.37"}"#).await, "ack Done: docker.io/library/busybox:1.37 pulled");
    assert_eq!(
        run(&p, &core, PULL, r#"{"reference":"quay.io/someone/tool"}"#).await,
        "ack Failed: not_allowed: quay.io/someone/tool is not among what the application allows"
    );
    assert!(run(&p, &core, PULL, r#"{"reference":"-all"}"#).await.starts_with("ack Failed: invalid: "));
    let list = json_of(&ask(&p, &core, LIST, "").await, 1);
    let protected: Vec<bool> = list["containers"].as_array().unwrap().iter().map(|c| c["protected"].as_bool().unwrap()).collect();
    assert_eq!(protected, [false, true, true, true, true], "a client sees what commands refuse");
    assert_eq!(fake.done(), ["stop aaaa", "remove-image docker.io/library/alpine:latest force=false", "pull docker.io/library/busybox:1.37"]);
}

#[tokio::test]
async fn an_image_is_removed_by_its_id_or_its_tag() {
    let fake = Fake::new();
    let (p, core) = reached(&fake, Policy::default()).await;
    assert_eq!(run(&p, &core, IMAGE_REMOVE, r#"{"id":"sha256:4444","force":true}"#).await, "ack Done: 4444 removed");
    assert_eq!(run(&p, &core, IMAGE_REMOVE, r#"{"id":"nginx"}"#).await, "ack Done: docker.io/library/nginx:latest removed");
    assert_eq!(run(&p, &core, IMAGE_REMOVE, r#"{"id":"nothing:1"}"#).await, "ack Failed: not_found: no image nothing:1");
    assert_eq!(fake.done(), ["remove-image 4444 force=true", "remove-image docker.io/library/nginx:latest force=false"]);
}

#[tokio::test(start_paused = true)]
async fn a_command_still_running_is_accepted_and_its_end_reported() {
    let fake = Fake::new();
    let (p, core) = reached(&fake, Policy::default()).await;
    handle(&core, &command(STOP, r#"{"id":"slow"}"#), &p).await.unwrap();
    let said = core.take();
    assert_eq!(said[0], format!("ack Accepted: still running after 25 s: {JOB_FINISHED} says how it ends"));
    assert_eq!(said[1], format!("report {JOB_FINISHED} 10: slow stopped"));
}

#[tokio::test]
async fn a_container_and_an_image_are_inspected_and_the_output_read() {
    let fake = Fake::new();
    let (p, core) = reached(&fake, Policy::default()).await;
    let i = json_of(&ask(&p, &core, INSPECT, r#"{"id":"web"}"#).await, 1);
    assert_eq!((i["kind"].as_str(), i["inspect"]["Id"].as_str()), (Some("container"), Some(WEB)));
    let i = json_of(&ask(&p, &core, INSPECT, r#"{"id":"4444","kind":"image"}"#).await, 1);
    assert_eq!(i["inspect"]["Architecture"], "amd64");
    assert_eq!(ask(&p, &core, INSPECT, r#"{"id":"nothing"}"#).await, "fail not_found: no such object: nothing");
    assert!(ask(&p, &core, INSPECT, r#"{"id":"web","kind":"volume"}"#).await.starts_with("fail invalid: "));

    let l = json_of(&ask(&p, &core, LOGS, r#"{"id":"web","tail":3}"#).await, 1);
    let lines: Vec<(&str, &str)> = l["lines"].as_array().unwrap().iter().map(|l| (l["stream"].as_str().unwrap(), l["text"].as_str().unwrap())).collect();
    assert_eq!(lines, [("stdout", "line 8"), ("stdout", "line 9"), ("stderr", "line 10")]);
    assert_eq!(json_of(&ask(&p, &core, LOGS, r#"{"id":"web"}"#).await, 1)["lines"].as_array().unwrap().len(), 10);
    assert!(ask(&p, &core, LOGS, "{}").await.starts_with("fail invalid: "));

    let perm = json_of(&ask(&p, &core, PERMISSIONS, "").await, 1);
    assert_eq!((perm["engine"].as_str(), perm["permissions"][0]["allowed"].as_bool()), (Some("podman"), Some(true)));
}

#[test]
fn a_list_too_long_for_one_payload_is_cut_and_says_so() {
    let item = json!({ "text": "x".repeat(1000) });
    let list = vec![item; 3000];
    let make = |part: &[Value], truncated: bool| json!({ "lines": part, "truncated": truncated });
    let cut: Value = serde_json::from_slice(&fit(&list, true, make)).unwrap();
    let kept = cut["lines"].as_array().unwrap().len();
    assert!(cut["truncated"].as_bool().unwrap() && kept > 900 && kept < 1100, "{kept}");
    let whole: Value = serde_json::from_slice(&fit(&list[..10], false, make)).unwrap();
    assert_eq!(whole["truncated"], false);
}

#[test]
fn a_pattern_matches_any_run_of_characters() {
    assert!(matches("web*", "web-1") && matches("*db*", "the-db-1") && matches("a*b*c", "aXbYc") && matches("x", "x"));
    assert!(!matches("web*", "my-web") && !matches("a*b*c", "acb") && !matches("x", "xy"));
}
