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

fn list_nodes(base: &str, node_type: &str) -> Vec<Value> {
    let response = request_json(
        "GET",
        &format!("{}/nodes?type={}", base, node_type),
        None,
    );
    response.into_json().expect("invalid json")
}

fn list_edges(base: &str, edge_type: &str) -> Vec<Value> {
    let response = request_json(
        "GET",
        &format!("{}/edges?type={}", base, edge_type),
        None,
    );
    response.into_json().expect("invalid json")
}

#[test]
fn lifecycle_simulation_grows_graph_over_phases() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind failed");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let db_path = std::env::temp_dir().join(format!("elo_test_lifecycle_{}.redb", port));
    if db_path.exists() {
        let _ = std::fs::remove_file(&db_path);
    }
    let _child = start_server(port, db_path.to_str().unwrap());
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    wait_for_port(addr);

    let base = format!("http://127.0.0.1:{}", port);
    let phases = [(1, 1, 0), (3, 2, 2), (5, 3, 4), (8, 4, 8), (13, 6, 13)];
    let mut total_users = 0;
    let mut total_teams = 0;
    let mut total_posts = 0;
    let mut total_memberships = 0;
    let mut total_authors = 0;

    for (phase_index, (users, teams, posts)) in phases.iter().enumerate() {
        let phase = phase_index + 1;
        let users = *users;
        let teams = *teams;
        let posts = *posts;

        for i in 0..teams {
            let team_id = format!("team:{}:{}", phase, i);
            request_json(
                "POST",
                &format!("{}/nodes", base),
                Some(&serde_json::json!({
                    "id": team_id,
                    "data": { "type": "Team", "phase": phase.to_string() }
                })),
            );
        }

        for i in 0..users {
            let user_id = format!("user:{}:{}", phase, i);
            request_json(
                "POST",
                &format!("{}/nodes", base),
                Some(&serde_json::json!({
                    "id": user_id,
                    "data": { "type": "User", "phase": phase.to_string() }
                })),
            );
        }

        for i in 0..posts {
            let post_id = format!("post:{}:{}", phase, i);
            request_json(
                "POST",
                &format!("{}/nodes", base),
                Some(&serde_json::json!({
                    "id": post_id,
                    "data": { "type": "Post", "phase": phase.to_string() }
                })),
            );
        }

        let team_count = teams.max(1);
        let user_count = users.max(1);

        for i in 0..users {
            let user_id = format!("user:{}:{}", phase, i);
            let team_index = i % team_count;
            let team_id = format!("team:{}:{}", phase, team_index);
            request_json(
                "POST",
                &format!("{}/edges", base),
                Some(&serde_json::json!({
                    "from": user_id,
                    "to": team_id,
                    "data": { "type": "member" }
                })),
            );
            total_memberships += 1;
        }

        for i in 0..posts {
            let post_id = format!("post:{}:{}", phase, i);
            let user_index = i % user_count;
            let team_index = i % team_count;
            let user_id = format!("user:{}:{}", phase, user_index);
            let team_id = format!("team:{}:{}", phase, team_index);
            request_json(
                "POST",
                &format!("{}/edges", base),
                Some(&serde_json::json!({
                    "from": user_id,
                    "to": post_id,
                    "data": { "type": "author" }
                })),
            );
            request_json(
                "POST",
                &format!("{}/edges", base),
                Some(&serde_json::json!({
                    "from": team_id,
                    "to": post_id,
                    "data": { "type": "host" }
                })),
            );
            total_authors += 1;
        }

        total_users += users;
        total_teams += teams;
        total_posts += posts;

        let users_now = list_nodes(&base, "User");
        let teams_now = list_nodes(&base, "Team");
        let posts_now = list_nodes(&base, "Post");
        assert!(
            users_now.len() >= total_users,
            "phase {} user count mismatch",
            phase
        );
        assert!(
            teams_now.len() >= total_teams,
            "phase {} team count mismatch",
            phase
        );
        assert!(
            posts_now.len() >= total_posts,
            "phase {} post count mismatch",
            phase
        );

        let memberships_now = list_edges(&base, "member");
        let authors_now = list_edges(&base, "author");
        assert!(
            memberships_now.len() >= total_memberships,
            "phase {} membership count mismatch",
            phase
        );
        assert!(
            authors_now.len() >= total_authors,
            "phase {} author count mismatch",
            phase
        );
    }

    let _ = std::fs::remove_file(&db_path);
}
