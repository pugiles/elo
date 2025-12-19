use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    middleware,
    routing::{get, post, put},
    Json, Router,
};
use redb::{Database, ReadOnlyTable, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    env,
    net::SocketAddr,
    sync::Arc,
};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

const DB_PATH: &str = "elo.redb";
const KEY_SEP: char = '\x1f';

const NODES_TABLE: TableDefinition<&str, &str> = TableDefinition::new("nodes");
const EDGES_TABLE: TableDefinition<&str, &str> = TableDefinition::new("edges");
const NODE_DATA_TABLE: TableDefinition<&str, &str> = TableDefinition::new("node_data");
const EDGE_DATA_TABLE: TableDefinition<&str, &str> = TableDefinition::new("edge_data");

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
    graph: Arc<RwLock<Graph>>,
    db: Arc<Database>,
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
    let db = Arc::new(Database::open(DB_PATH).or_else(|_| Database::create(DB_PATH))?);
    init_db(db.as_ref())?;
    let graph = load_graph(db.as_ref())?;

    let state = AppState {
        graph: Arc::new(RwLock::new(graph)),
        db,
        api_key: Arc::new(api_key),
    };

    let app = Router::new()
        .route("/nodes", post(create_node).get(list_nodes))
        .route("/nodes/{id}", get(get_node))
        .route("/nodes/{id}/data", put(set_node_data))
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
    let node_id = payload.id.clone();
    run_db(state.db.clone(), move |db| insert_node(db, &node_id)).await?;
    let mut graph = state.graph.write().await;
    graph.add_node(&payload.id);

    Ok(StatusCode::CREATED)
}

async fn create_edge(
    State(state): State<AppState>,
    Json(payload): Json<CreateEdge>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let graph = state.graph.read().await;
        if !graph.has_node(&payload.from) || !graph.has_node(&payload.to) {
            return Err((StatusCode::NOT_FOUND, "node not found".to_string()));
        }
    }
    let from = payload.from.clone();
    let to = payload.to.clone();
    run_db(state.db.clone(), move |db| insert_edge(db, &from, &to)).await?;
    let mut graph = state.graph.write().await;
    graph.add_edge(&payload.from, &payload.to);

    Ok(StatusCode::CREATED)
}

async fn set_node_data(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<KeyValue>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let graph = state.graph.read().await;
        if !graph.has_node(&id) {
            return Err((StatusCode::NOT_FOUND, "node not found".to_string()));
        }
    }
    let node_id = id.clone();
    let key = payload.key.clone();
    let value = payload.value.clone();
    run_db(state.db.clone(), move |db| insert_node_data(db, &node_id, &key, &value)).await?;
    let mut graph = state.graph.write().await;
    graph.set_node_data(&id, &payload.key, &payload.value);

    Ok(StatusCode::NO_CONTENT)
}

async fn set_edge_data(
    State(state): State<AppState>,
    Json(payload): Json<EdgeKeyValue>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let graph = state.graph.read().await;
        if !graph.has_edge(&payload.from, &payload.to) {
            return Err((StatusCode::NOT_FOUND, "edge not found".to_string()));
        }
    }
    let from = payload.from.clone();
    let to = payload.to.clone();
    let key = payload.key.clone();
    let value = payload.value.clone();
    run_db(state.db.clone(), move |db| insert_edge_data(db, &from, &to, &key, &value)).await?;
    let mut graph = state.graph.write().await;
    graph.set_edge_data(&payload.from, &payload.to, &payload.key, &payload.value);

    Ok(StatusCode::NO_CONTENT)
}

async fn get_node(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<NodeView>, (StatusCode, String)> {
    let node_id = id.clone();
    let node = run_db(state.db.clone(), move |db| get_node_from_db(db, &node_id)).await?;
    let node = node.ok_or((StatusCode::NOT_FOUND, "node not found".to_string()))?;

    Ok(Json(node))
}

async fn list_nodes(
    State(state): State<AppState>,
    Query(query): Query<NodeListQuery>,
) -> Result<Json<Vec<NodeView>>, (StatusCode, String)> {
    let node_type = query.r#type.clone();
    let nodes = run_db(state.db.clone(), move |db| list_nodes_from_db(db, node_type.as_deref()))
        .await?;

    Ok(Json(nodes))
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
) -> Result<Json<Vec<EdgeListView>>, (StatusCode, String)> {
    let edge_type = query.r#type.clone();
    let from = query.from.clone();
    let to = query.to.clone();
    let edges = run_db(state.db.clone(), move |db| {
        list_edges_from_db(db, edge_type.as_deref(), from.as_deref(), to.as_deref())
    })
    .await?;

    Ok(Json(edges))
}

async fn check_path(
    State(state): State<AppState>,
    Query(query): Query<PathQuery>,
) -> Json<PathResponse> {
    let graph = state.graph.read().await;
    Json(PathResponse {
        exists: graph.exist_path(&query.from, &query.to),
    })
}

async fn recommend_nodes(
    State(state): State<AppState>,
    Query(query): Query<RecommendQuery>,
) -> Result<Json<Vec<Recommendation>>, (StatusCode, String)> {
    let graph = state.graph.read().await;
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

fn init_db(db: &Database) -> Result<(), redb::Error> {
    let write_txn = db.begin_write()?;
    write_txn.open_table(NODES_TABLE)?;
    write_txn.open_table(NODE_DATA_TABLE)?;
    write_txn.open_table(EDGES_TABLE)?;
    write_txn.open_table(EDGE_DATA_TABLE)?;
    write_txn.commit()?;
    Ok(())
}

fn load_graph(db: &Database) -> Result<Graph, redb::Error> {
    let mut graph = Graph::new();

    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let read_txn = db.begin_read()?;
    let nodes_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODES_TABLE)?;
    for entry in nodes_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        graph.add_node(key.value());
    }

    let edges_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGES_TABLE)?;
    for entry in edges_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id, to_id)) = split_two(key.value()) {
            graph.add_edge(&from_id, &to_id);
        }
    }

    let node_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_DATA_TABLE)?;
    for entry in node_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((node_id, data_key)) = split_two(key.value()) {
            graph.set_node_data(&node_id, &data_key, value.value());
        }
    }

    let edge_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_DATA_TABLE)?;
    for entry in edge_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id, to_id, data_key)) = split_three(key.value()) {
            graph.set_edge_data(&from_id, &to_id, &data_key, value.value());
        }
    }

    Ok(graph)
}

fn get_node_from_db(db: &Database, node_id: &str) -> Result<Option<NodeView>, redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let read_txn = db.begin_read()?;
    let nodes_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODES_TABLE)?;
    if nodes_table.get(node_id)?.is_none() {
        return Ok(None);
    }

    let mut data = HashMap::new();
    let node_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_DATA_TABLE)?;
    for entry in node_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((id, data_key)) = split_two(key.value()) {
            if id == node_id {
                data.insert(data_key, value.value().to_string());
            }
        }
    }

    let mut edges = Vec::new();
    let mut edge_data: HashMap<String, HashMap<String, String>> = HashMap::new();
    let edge_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_DATA_TABLE)?;
    for entry in edge_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id, to_id, data_key)) = split_three(key.value()) {
            if from_id == node_id {
                edge_data
                    .entry(edge_key(&from_id, &to_id))
                    .or_default()
                    .insert(data_key, value.value().to_string());
            }
        }
    }

    let edges_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGES_TABLE)?;
    for entry in edges_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id, to_id)) = split_two(key.value()) {
            if from_id == node_id {
                let data = edge_data.remove(&edge_key(&from_id, &to_id)).unwrap_or_default();
                edges.push(EdgeView { to: to_id, data });
            }
        }
    }

    Ok(Some(NodeView {
        id: node_id.to_string(),
        data,
        edges,
    }))
}

fn list_nodes_from_db(
    db: &Database,
    node_type: Option<&str>,
) -> Result<Vec<NodeView>, redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let read_txn = db.begin_read()?;
    let nodes_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODES_TABLE)?;
    let node_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_DATA_TABLE)?;
    let edges_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGES_TABLE)?;
    let edge_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_DATA_TABLE)?;

    let mut node_data: HashMap<String, HashMap<String, String>> = HashMap::new();
    for entry in node_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((node_id, data_key)) = split_two(key.value()) {
            node_data
                .entry(node_id)
                .or_default()
                .insert(data_key, value.value().to_string());
        }
    }

    let mut edges_by_from: HashMap<String, Vec<String>> = HashMap::new();
    for entry in edges_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id, to_id)) = split_two(key.value()) {
            edges_by_from.entry(from_id).or_default().push(to_id);
        }
    }

    let mut edge_data: HashMap<String, HashMap<String, String>> = HashMap::new();
    for entry in edge_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id, to_id, data_key)) = split_three(key.value()) {
            edge_data
                .entry(edge_key(&from_id, &to_id))
                .or_default()
                .insert(data_key, value.value().to_string());
        }
    }

    let mut nodes = Vec::new();
    for entry in nodes_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        let node_id = key.value().to_string();
        let data = node_data.remove(&node_id).unwrap_or_default();
        if let Some(node_type) = node_type {
            if data.get("type").map(|value| value.as_str()) != Some(node_type) {
                continue;
            }
        }

        let mut edges = Vec::new();
        if let Some(to_list) = edges_by_from.get(&node_id) {
            for to_id in to_list {
                let data = edge_data.remove(&edge_key(&node_id, to_id)).unwrap_or_default();
                edges.push(EdgeView {
                    to: to_id.clone(),
                    data,
                });
            }
        }

        nodes.push(NodeView {
            id: node_id,
            data,
            edges,
        });
    }

    Ok(nodes)
}

fn list_edges_from_db(
    db: &Database,
    edge_type: Option<&str>,
    from_filter: Option<&str>,
    to_filter: Option<&str>,
) -> Result<Vec<EdgeListView>, redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let read_txn = db.begin_read()?;
    let edges_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGES_TABLE)?;
    let edge_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_DATA_TABLE)?;

    let mut edge_data: HashMap<String, HashMap<String, String>> = HashMap::new();
    for entry in edge_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id, to_id, data_key)) = split_three(key.value()) {
            edge_data
                .entry(edge_key(&from_id, &to_id))
                .or_default()
                .insert(data_key, value.value().to_string());
        }
    }

    let mut edges = Vec::new();
    for entry in edges_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id, to_id)) = split_two(key.value()) {
            if let Some(from_filter) = from_filter {
                if from_id != from_filter {
                    continue;
                }
            }
            if let Some(to_filter) = to_filter {
                if to_id != to_filter {
                    continue;
                }
            }

            let data = edge_data
                .remove(&edge_key(&from_id, &to_id))
                .unwrap_or_default();
            if let Some(edge_type) = edge_type {
                if data.get("type").map(|value| value.as_str()) != Some(edge_type) {
                    continue;
                }
            }

            edges.push(EdgeListView {
                from: from_id,
                to: to_id,
                data,
            });
        }
    }

    Ok(edges)
}

async fn run_db<T, F>(db: Arc<Database>, operation: F) -> Result<T, (StatusCode, String)>
where
    T: Send + 'static,
    F: FnOnce(&Database) -> Result<T, redb::Error> + Send + 'static,
{
    tokio::task::spawn_blocking(move || operation(db.as_ref()))
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .map_err(db_error)
}

fn insert_node(db: &Database, node_id: &str) -> Result<(), redb::Error> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(NODES_TABLE)?;
        table.insert(node_id, "")?;
    }
    write_txn.commit()?;
    Ok(())
}

fn insert_edge(db: &Database, from: &str, to: &str) -> Result<(), redb::Error> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(EDGES_TABLE)?;
        let key = edge_key(from, to);
        table.insert(key.as_str(), "")?;
    }
    write_txn.commit()?;
    Ok(())
}

fn insert_node_data(
    db: &Database,
    node_id: &str,
    key: &str,
    value: &str,
) -> Result<(), redb::Error> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(NODE_DATA_TABLE)?;
        let data_key = node_data_key(node_id, key);
        table.insert(data_key.as_str(), value)?;
    }
    write_txn.commit()?;
    Ok(())
}

fn insert_edge_data(
    db: &Database,
    from: &str,
    to: &str,
    key: &str,
    value: &str,
) -> Result<(), redb::Error> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(EDGE_DATA_TABLE)?;
        let data_key = edge_data_key(from, to, key);
        table.insert(data_key.as_str(), value)?;
    }
    write_txn.commit()?;
    Ok(())
}

fn edge_key(from: &str, to: &str) -> String {
    format!("{from}{KEY_SEP}{to}")
}

fn node_data_key(node_id: &str, key: &str) -> String {
    format!("{node_id}{KEY_SEP}{key}")
}

fn edge_data_key(from: &str, to: &str, key: &str) -> String {
    format!("{from}{KEY_SEP}{to}{KEY_SEP}{key}")
}

fn split_two(value: &str) -> Option<(String, String)> {
    let mut parts = value.splitn(2, KEY_SEP);
    let first = parts.next()?;
    let second = parts.next()?;
    Some((first.to_string(), second.to_string()))
}

fn split_three(value: &str) -> Option<(String, String, String)> {
    let mut parts = value.splitn(3, KEY_SEP);
    let first = parts.next()?;
    let second = parts.next()?;
    let third = parts.next()?;
    Some((first.to_string(), second.to_string(), third.to_string()))
}

fn db_error(error: redb::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}
