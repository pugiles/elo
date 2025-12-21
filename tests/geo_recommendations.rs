use serde_json::Value;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command};
use std::thread;
use std::time::{Duration, Instant};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_server(port: u16, db_path: &str) -> ChildGuard {
    let exe = env!("CARGO_BIN_EXE_elo");
    let child = Command::new(exe)
        .env("ELO_API_KEY", "test_token")
        .env("ELO_HOST", "127.0.0.1")
        .env("ELO_PORT", port.to_string())
        .env("ELO_DB_PATH", db_path)
        .spawn()
        .expect("failed to start server");
    ChildGuard(child)
}

fn wait_for_port(addr: SocketAddr) {
    let start = Instant::now();
    loop {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        if start.elapsed() > Duration::from_secs(5) {
            panic!("server did not start in time");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn request_json(method: &str, url: &str, body: Option<&Value>) -> ureq::Response {
    let builder = ureq::request(method, url).set("x-api-key", "test_token");
    match body {
        Some(body) => builder
            .set("content-type", "application/json")
            .send_json(body),
        None => builder.call(),
    }
    .expect("request failed")
}

#[test]
fn recommendations_filter_by_geo_radius() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind failed");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let db_path = std::env::temp_dir().join(format!("elo_test_geo_{}.redb", port));
    if db_path.exists() {
        let _ = std::fs::remove_file(&db_path);
    }
    let _child = start_server(port, db_path.to_str().unwrap());
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    wait_for_port(addr);

    let base = format!("http://127.0.0.1:{}", port);

    request_json(
        "POST",
        &format!("{}/nodes", base),
        Some(&serde_json::json!({
            "id": "user:me",
            "data": { "type": "User", "location": "-23.5505,-46.6333" }
        })),
    );
    request_json(
        "POST",
        &format!("{}/nodes", base),
        Some(&serde_json::json!({ "id": "user:friend", "data": { "type": "User" } })),
    );
    request_json(
        "POST",
        &format!("{}/nodes", base),
        Some(&serde_json::json!({
            "id": "team:near",
            "data": { "type": "Team", "location": "-23.5510,-46.6340" }
        })),
    );
    request_json(
        "POST",
        &format!("{}/nodes", base),
        Some(&serde_json::json!({
            "id": "team:far",
            "data": { "type": "Team", "location": "-22.9068,-43.1729" }
        })),
    );

    request_json(
        "POST",
        &format!("{}/edges", base),
        Some(&serde_json::json!({ "from": "user:me", "to": "user:friend" })),
    );
    request_json(
        "POST",
        &format!("{}/edges", base),
        Some(&serde_json::json!({ "from": "user:friend", "to": "team:near" })),
    );
    request_json(
        "POST",
        &format!("{}/edges", base),
        Some(&serde_json::json!({ "from": "user:friend", "to": "team:far" })),
    );

    let response = request_json(
        "GET",
        &format!(
            "{}/recommendations?start=user:me&type=Team&radius_km=10",
            base
        ),
        None,
    );
    assert_eq!(response.status(), 200);
    let body: Vec<Value> = response.into_json().expect("invalid json");
    assert_eq!(body.len(), 1);
    assert_eq!(body[0]["id"].as_str(), Some("team:near"));

    let _ = std::fs::remove_file(&db_path);
}
