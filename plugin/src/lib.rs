//! The host's container engine as a Yoke Plugin: Podman or Docker, over its API socket.
//!
//! `backend` talks to the engine and knows nothing of Yoke; `http` is what it speaks the API with;
//! `plugin` is the Plugin around it.

pub mod backend;
pub mod http;
pub mod plugin;
