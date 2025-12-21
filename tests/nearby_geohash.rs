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

fn encode_geohash(lat: f64, lon: f64, precision: usize) -> String {
    const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let precision = precision.max(1);
    let mut lat_range = [-90.0, 90.0];
    let mut lon_range = [-180.0, 180.0];
    let bits = [16, 8, 4, 2, 1];
    let mut bit = 0;
    let mut ch = 0;
    let mut even = true;
    let mut geohash = String::with_capacity(precision);

    while geohash.len() < precision {
        if even {
            let mid = (lon_range[0] + lon_range[1]) / 2.0;
            if lon >= mid {
                ch |= bits[bit];
                lon_range[0] = mid;
            } else {
                lon_range[1] = mid;
            }
        } else {
            let mid = (lat_range[0] + lat_range[1]) / 2.0;
            if lat >= mid {
                ch |= bits[bit];
                lat_range[0] = mid;
            } else {
                lat_range[1] = mid;
            }
        }
        even = !even;
        if bit < 4 {
            bit += 1;
        } else {
            geohash.push(BASE32[ch] as char);
            bit = 0;
            ch = 0;
        }
    }

    geohash
}

#[test]
fn nearby_filters_by_geo_hash_prefix() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind failed");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let db_path = std::env::temp_dir().join(format!("elo_test_nearby_{}.redb", port));
    if db_path.exists() {
        let _ = std::fs::remove_file(&db_path);
    }
    let _child = start_server(port, db_path.to_str().unwrap());
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    wait_for_port(addr);

    let base = format!("http://127.0.0.1:{}", port);

    let near_hash = encode_geohash(-23.5505, -46.6333, 6);
    let far_hash = encode_geohash(40.7128, -74.0060, 6);

    request_json(
        "POST",
        &format!("{}/nodes", base),
        Some(&serde_json::json!({
            "id": "gym:near",
            "data": { "type": "Gym", "geo_hash": near_hash }
        })),
    );
    request_json(
        "POST",
        &format!("{}/nodes", base),
        Some(&serde_json::json!({
            "id": "gym:far",
            "data": { "type": "Gym", "geo_hash": far_hash }
        })),
    );

    let prefix = &near_hash[..3];
    let response = request_json(
        "GET",
        &format!("{}/nearby?type=Gym&geo_hash_prefix={}", base, prefix),
        None,
    );
    assert_eq!(response.status(), 200);
    let body: Vec<Value> = response.into_json().expect("invalid json");
    assert_eq!(body.len(), 1);
    assert_eq!(body[0]["id"].as_str(), Some("gym:near"));

    let response = request_json(
        "GET",
        &format!(
            "{}/nearby?type=Gym&lat=-23.5505&lon=-46.6333&radius_km=10",
            base
        ),
        None,
    );
    assert_eq!(response.status(), 200);
    let body: Vec<Value> = response.into_json().expect("invalid json");
    assert_eq!(body.len(), 1);
    assert_eq!(body[0]["id"].as_str(), Some("gym:near"));

    let _ = std::fs::remove_file(&db_path);
}
