use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    middleware,
    routing::{get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    env,
    net::SocketAddr,
    sync::Arc,
};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

const DB_PATH: &str = "elo.db";

#[derive(Debug)]
struct Node {
    id: String,
    neighbors: Vec<Edge>,
    data: HashMap<String, String>,
}

impl Node {
    fn new(id: &str) -> Self {
        Node {
            id: id.to_string(),
            neighbors: Vec::new(),
            data: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct Edge {
    to: String,
    data: HashMap<String, String>,
}

impl Edge {
    fn new(to: &str) -> Self {
        Edge {
            to: to.to_string(),
            data: HashMap::new(),
        }
    }
}

#[derive(Debug)]
struct Graph {
    nodes: HashMap<String, Node>,
}

impl Graph {
    fn new() -> Self {
        Graph {
            nodes: HashMap::new(),
        }
    }

    fn add_node(&mut self, id: &str) {
        self.nodes.entry(id.to_string()).or_insert(Node::new(id));
    }

    fn add_edge(&mut self, from: &str, to: &str) {
        if let Some(node) = self.nodes.get_mut(from) {
            if node.neighbors.iter().any(|edge| edge.to == to) {
                return;
            }
            node.neighbors.push(Edge::new(to));
        }
    }

    fn has_node(&self, id: &str) -> bool {
        self.nodes.contains_key(id)
    }

    fn has_edge(&self, from: &str, to: &str) -> bool {
        self.nodes
            .get(from)
            .map(|node| node.neighbors.iter().any(|edge| edge.to == to))
            .unwrap_or(false)
    }

    fn get_node(&self, id: &str) -> Option<&Node> {
        self.nodes.get(id)
    }

    fn set_node_data(&mut self, id: &str, key: &str, value: &str) {
        if let Some(node) = self.nodes.get_mut(id) {
            node.data.insert(key.to_string(), value.to_string());
        }
    }

    fn set_edge_data(&mut self, from: &str, to: &str, key: &str, value: &str) {
        if let Some(node) = self.nodes.get_mut(from) {
            if let Some(edge) = node.neighbors.iter_mut().find(|edge| edge.to == to) {
                edge.data.insert(key.to_string(), value.to_string());
            }
        }
    }

    fn exist_path(&self, start: &str, end: &str) -> bool {
        let mut visited = HashSet::new();
        self.dfs(start, end, &mut visited)
    }

    fn dfs(&self, current: &str, target: &str, visited: &mut HashSet<String>) -> bool {
        if current == target {
            return true;
        }
        visited.insert(current.to_string());

        if let Some(node) = self.nodes.get(current) {
            for neighbor in &node.neighbors {
                if !visited.contains(&neighbor.to) {
                    if self.dfs(&neighbor.to, target, visited) {
                        return true;
                    }
                }
            }
        }
        false
    }
}

#[derive(Clone)]
struct AppState {
    graph: Arc<Mutex<Graph>>,
    db: SqlitePool,
    api_key: Arc<String>,
}

#[derive(Deserialize)]
struct CreateNode {
    id: String,
}

#[derive(Deserialize)]
struct CreateEdge {
    from: String,
    to: String,
}

#[derive(Deserialize)]
struct KeyValue {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct EdgeKeyValue {
    from: String,
    to: String,
    key: String,
    value: String,
}

#[derive(Serialize)]
struct NodeView {
    id: String,
    data: HashMap<String, String>,
    edges: Vec<EdgeView>,
}

#[derive(Serialize)]
struct EdgeView {
    to: String,
    data: HashMap<String, String>,
}

#[derive(Deserialize)]
struct PathQuery {
    from: String,
    to: String,
}

#[derive(Deserialize)]
struct NodeListQuery {
    r#type: Option<String>,
}

#[derive(Deserialize)]
struct EdgeListQuery {
    r#type: Option<String>,
    from: Option<String>,
    to: Option<String>,
}

#[derive(Serialize)]
struct PathResponse {
    exists: bool,
}

#[derive(Deserialize)]
struct RecommendQuery {
    start: String,
    r#type: String,
    num_key: Option<String>,
    min: Option<f64>,
    max: Option<f64>,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct Recommendation {
    id: String,
    score: f64,
    data: HashMap<String, String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("ELO_API_KEY").map_err(|_| "ELO_API_KEY not set")?;
    let host = env::var("ELO_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("ELO_PORT").unwrap_or_else(|_| "3000".to_string());
    let db_url = format!("sqlite://./{}?mode=rwc", DB_PATH);
    let pool = SqlitePool::connect(&db_url).await?;
    init_db(&pool).await?;
    let graph = load_graph(&pool).await?;

    let state = AppState {
        graph: Arc::new(Mutex::new(graph)),
        db: pool,
        api_key: Arc::new(api_key),
    };

    let app = Router::new()
        .route("/nodes", post(create_node).get(list_nodes))
        .route("/nodes/:id", get(get_node))
        .route("/nodes/:id/data", put(set_node_data))
        .route("/edges", post(create_edge).put(set_edge_data).get(list_edges))
        .route("/path", get(check_path))
        .route("/recommendations", get(recommend_nodes))
        .layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .with_state(state);

    let addr: SocketAddr = format!("{}:{}", host, port)
        .parse()
        .map_err(|_| "invalid ELO_HOST or ELO_PORT")?;
    println!("Listening on http://{}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn authenticate(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: middleware::Next,
) -> Result<axum::response::Response, StatusCode> {
    let provided = request
        .headers()
        .get("x-api-key")
        .and_then(|value| value.to_str().ok());

    if provided == Some(state.api_key.as_str()) {
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

async fn create_node(
    State(state): State<AppState>,
    Json(payload): Json<CreateNode>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut graph = state.graph.lock().await;
    graph.add_node(&payload.id);
    drop(graph);

    sqlx::query("INSERT OR IGNORE INTO nodes (id) VALUES (?)")
        .bind(&payload.id)
        .execute(&state.db)
        .await
        .map_err(db_error)?;

    Ok(StatusCode::CREATED)
}

async fn create_edge(
    State(state): State<AppState>,
    Json(payload): Json<CreateEdge>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut graph = state.graph.lock().await;
    if !graph.has_node(&payload.from) || !graph.has_node(&payload.to) {
        return Err((StatusCode::NOT_FOUND, "node not found".to_string()));
    }
    graph.add_edge(&payload.from, &payload.to);
    drop(graph);

    sqlx::query("INSERT OR IGNORE INTO edges (from_id, to_id) VALUES (?, ?)")
        .bind(&payload.from)
        .bind(&payload.to)
        .execute(&state.db)
        .await
        .map_err(db_error)?;

    Ok(StatusCode::CREATED)
}

async fn set_node_data(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<KeyValue>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut graph = state.graph.lock().await;
    if !graph.has_node(&id) {
        return Err((StatusCode::NOT_FOUND, "node not found".to_string()));
    }
    graph.set_node_data(&id, &payload.key, &payload.value);
    drop(graph);

    sqlx::query(
        "INSERT INTO node_data (node_id, key, value) VALUES (?, ?, ?) \
        ON CONFLICT(node_id, key) DO UPDATE SET value = excluded.value",
    )
    .bind(&id)
    .bind(&payload.key)
    .bind(&payload.value)
    .execute(&state.db)
    .await
    .map_err(db_error)?;

    Ok(StatusCode::NO_CONTENT)
}

async fn set_edge_data(
    State(state): State<AppState>,
    Json(payload): Json<EdgeKeyValue>,
) -> Result<StatusCode, (StatusCode, String)> {
    let mut graph = state.graph.lock().await;
    if !graph.has_edge(&payload.from, &payload.to) {
        return Err((StatusCode::NOT_FOUND, "edge not found".to_string()));
    }
    graph.set_edge_data(&payload.from, &payload.to, &payload.key, &payload.value);
    drop(graph);

    sqlx::query(
        "INSERT INTO edge_data (from_id, to_id, key, value) VALUES (?, ?, ?, ?) \
        ON CONFLICT(from_id, to_id, key) DO UPDATE SET value = excluded.value",
    )
    .bind(&payload.from)
    .bind(&payload.to)
    .bind(&payload.key)
    .bind(&payload.value)
    .execute(&state.db)
    .await
    .map_err(db_error)?;

    Ok(StatusCode::NO_CONTENT)
}

async fn get_node(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<NodeView>, (StatusCode, String)> {
    let graph = state.graph.lock().await;
    let node = graph
        .get_node(&id)
        .ok_or((StatusCode::NOT_FOUND, "node not found".to_string()))?;

    let edges = node
        .neighbors
        .iter()
        .map(|edge| EdgeView {
            to: edge.to.clone(),
            data: edge.data.clone(),
        })
        .collect();

    Ok(Json(NodeView {
        id: node.id.clone(),
        data: node.data.clone(),
        edges,
    }))
}

async fn list_nodes(
    State(state): State<AppState>,
    Query(query): Query<NodeListQuery>,
) -> Json<Vec<NodeView>> {
    let graph = state.graph.lock().await;
    let mut nodes = Vec::new();

    for node in graph.nodes.values() {
        if let Some(ref node_type) = query.r#type {
            if node.data.get("type") != Some(node_type) {
                continue;
            }
        }

        let edges = node
            .neighbors
            .iter()
            .map(|edge| EdgeView {
                to: edge.to.clone(),
                data: edge.data.clone(),
            })
            .collect();

        nodes.push(NodeView {
            id: node.id.clone(),
            data: node.data.clone(),
            edges,
        });
    }

    Json(nodes)
}

#[derive(Serialize)]
struct EdgeListView {
    from: String,
    to: String,
    data: HashMap<String, String>,
}

async fn list_edges(
    State(state): State<AppState>,
    Query(query): Query<EdgeListQuery>,
) -> Json<Vec<EdgeListView>> {
    let graph = state.graph.lock().await;
    let mut edges = Vec::new();

    for node in graph.nodes.values() {
        if let Some(ref from_filter) = query.from {
            if &node.id != from_filter {
                continue;
            }
        }

        for edge in &node.neighbors {
            if let Some(ref to_filter) = query.to {
                if &edge.to != to_filter {
                    continue;
                }
            }

            if let Some(ref edge_type) = query.r#type {
                if edge.data.get("type") != Some(edge_type) {
                    continue;
                }
            }

            edges.push(EdgeListView {
                from: node.id.clone(),
                to: edge.to.clone(),
                data: edge.data.clone(),
            });
        }
    }

    Json(edges)
}

async fn check_path(
    State(state): State<AppState>,
    Query(query): Query<PathQuery>,
) -> Json<PathResponse> {
    let graph = state.graph.lock().await;
    Json(PathResponse {
        exists: graph.exist_path(&query.from, &query.to),
    })
}

async fn recommend_nodes(
    State(state): State<AppState>,
    Query(query): Query<RecommendQuery>,
) -> Result<Json<Vec<Recommendation>>, (StatusCode, String)> {
    let graph = state.graph.lock().await;
    let start = graph
        .get_node(&query.start)
        .ok_or((StatusCode::NOT_FOUND, "start node not found".to_string()))?;

    let direct_neighbors: HashSet<String> =
        start.neighbors.iter().map(|edge| edge.to.clone()).collect();

    let mut scores: HashMap<String, f64> = HashMap::new();
    for edge in &start.neighbors {
        let weight1 = edge_weight(edge);
        if let Some(node) = graph.get_node(&edge.to) {
            for edge2 in &node.neighbors {
                let candidate = &edge2.to;
                if candidate == &query.start {
                    continue;
                }
                if direct_neighbors.contains(candidate) {
                    continue;
                }

                let weight2 = edge_weight(edge2);
                let entry = scores.entry(candidate.clone()).or_insert(0.0);
                *entry += weight1 * weight2;
            }
        }
    }

    let num_key = query.num_key.as_deref().unwrap_or("rating");
    let mut results = Vec::new();
    for (id, score) in scores {
        let node = match graph.get_node(&id) {
            Some(node) => node,
            None => continue,
        };

        if node.data.get("type").map(|value| value.as_str()) != Some(query.r#type.as_str()) {
            continue;
        }

        if query.min.is_some() || query.max.is_some() {
            let value = node
                .data
                .get(num_key)
                .and_then(|value| value.parse::<f64>().ok());
            let value = match value {
                Some(value) => value,
                None => continue,
            };

            if let Some(min) = query.min {
                if value < min {
                    continue;
                }
            }
            if let Some(max) = query.max {
                if value > max {
                    continue;
                }
            }
        }

        results.push(Recommendation {
            id: node.id.clone(),
            score,
            data: node.data.clone(),
        });
    }

    results.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.id.cmp(&right.id))
    });

    if let Some(limit) = query.limit {
        results.truncate(limit);
    }

    Ok(Json(results))
}

fn edge_weight(edge: &Edge) -> f64 {
    edge.data
        .get("weight")
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(1.0)
}

async fn init_db(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(pool)
        .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS nodes (\
            id TEXT PRIMARY KEY\
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS node_data (\
            node_id TEXT NOT NULL,\
            key TEXT NOT NULL,\
            value TEXT NOT NULL,\
            PRIMARY KEY (node_id, key),\
            FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE\
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS edges (\
            from_id TEXT NOT NULL,\
            to_id TEXT NOT NULL,\
            PRIMARY KEY (from_id, to_id),\
            FOREIGN KEY (from_id) REFERENCES nodes(id) ON DELETE CASCADE,\
            FOREIGN KEY (to_id) REFERENCES nodes(id) ON DELETE CASCADE\
        )",
    )
    .execute(pool)
    .await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS edge_data (\
            from_id TEXT NOT NULL,\
            to_id TEXT NOT NULL,\
            key TEXT NOT NULL,\
            value TEXT NOT NULL,\
            PRIMARY KEY (from_id, to_id, key),\
            FOREIGN KEY (from_id, to_id) REFERENCES edges(from_id, to_id) ON DELETE CASCADE\
        )",
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn load_graph(pool: &SqlitePool) -> Result<Graph, sqlx::Error> {
    let mut graph = Graph::new();

    let node_rows = sqlx::query("SELECT id FROM nodes").fetch_all(pool).await?;
    for row in node_rows {
        let id: String = row.try_get("id")?;
        graph.add_node(&id);
    }

    let edge_rows = sqlx::query("SELECT from_id, to_id FROM edges")
        .fetch_all(pool)
        .await?;
    for row in edge_rows {
        let from_id: String = row.try_get("from_id")?;
        let to_id: String = row.try_get("to_id")?;
        graph.add_edge(&from_id, &to_id);
    }

    let node_data_rows = sqlx::query("SELECT node_id, key, value FROM node_data")
        .fetch_all(pool)
        .await?;
    for row in node_data_rows {
        let node_id: String = row.try_get("node_id")?;
        let key: String = row.try_get("key")?;
        let value: String = row.try_get("value")?;
        graph.set_node_data(&node_id, &key, &value);
    }

    let edge_data_rows = sqlx::query("SELECT from_id, to_id, key, value FROM edge_data")
        .fetch_all(pool)
        .await?;
    for row in edge_data_rows {
        let from_id: String = row.try_get("from_id")?;
        let to_id: String = row.try_get("to_id")?;
        let key: String = row.try_get("key")?;
        let value: String = row.try_get("value")?;
        graph.set_edge_data(&from_id, &to_id, &key, &value);
    }

    Ok(graph)
}

fn db_error(error: sqlx::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}
