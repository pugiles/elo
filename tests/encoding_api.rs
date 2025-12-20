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
fn api_accepts_ids_with_separator_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind failed");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let db_path = std::env::temp_dir().join(format!("elo_test_encoding_{}.redb", port));
    if db_path.exists() {
        let _ = std::fs::remove_file(&db_path);
    }
    let _child = start_server(port, db_path.to_str().unwrap());
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    wait_for_port(addr);

    let node_id = "user\x1ftype\x1fadmin%";
    let encoded_id = urlencoding::encode(node_id);
    let base = format!("http://127.0.0.1:{}", port);

    let status = request_json(
        "POST",
        &format!("{}/nodes", base),
        Some(&serde_json::json!({ "id": node_id })),
    )
    .status();
    assert_eq!(status, 201);

    let status = request_json(
        "PUT",
        &format!("{}/nodes/{}/data", base, encoded_id),
        Some(&serde_json::json!({ "key": "type", "value": "Admin" })),
    )
    .status();
    assert_eq!(status, 204);

    let response = request_json("GET", &format!("{}/nodes/{}", base, encoded_id), None);
    assert_eq!(response.status(), 200);
    let body: Value = response.into_json().expect("invalid json");
    assert_eq!(body["id"].as_str(), Some(node_id));

    let response = request_json("GET", &format!("{}/nodes?type=Admin", base), None);
    assert_eq!(response.status(), 200);
    let body: Value = response.into_json().expect("invalid json");
    assert!(
        body.as_array()
            .unwrap()
            .iter()
            .any(|item| { item.get("id").and_then(Value::as_str) == Some(node_id) })
    );

    let _ = std::fs::remove_file(&db_path);
}
