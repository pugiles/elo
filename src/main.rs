use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    middleware,
    routing::{get, post, put},
};
use redb::{Database, ReadOnlyTable, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
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
const NODE_INDEX_TABLE: TableDefinition<&str, &str> = TableDefinition::new("node_index");
const EDGE_INDEX_TABLE: TableDefinition<&str, &str> = TableDefinition::new("edge_index");

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
    data: Option<HashMap<String, String>>,
}

#[derive(Deserialize)]
struct CreateEdge {
    from: String,
    to: String,
    data: Option<HashMap<String, String>>,
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
    let db_path = env::var("ELO_DB_PATH").unwrap_or_else(|_| DB_PATH.to_string());
    let db =
        Arc::new(Database::open(db_path.as_str()).or_else(|_| Database::create(db_path.as_str()))?);
    init_db(db.as_ref())?;
    rebuild_indexes_if_empty(db.as_ref())?;
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
        .route(
            "/edges",
            post(create_edge).put(set_edge_data).get(list_edges),
        )
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
    if let Some(data) = payload.data {
        let node_id = payload.id.clone();
        let data_clone = data.clone();
        run_db(state.db.clone(), move |db| insert_node_data_bulk(db, &node_id, &data_clone))
            .await?;
        for (key, value) in data {
            graph.set_node_data(&payload.id, &key, &value);
        }
    }

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
    if let Some(data) = payload.data {
        let from_id = payload.from.clone();
        let to_id = payload.to.clone();
        let data_clone = data.clone();
        run_db(state.db.clone(), move |db| insert_edge_data_bulk(db, &from_id, &to_id, &data_clone))
            .await?;
        for (key, value) in data {
            graph.set_edge_data(&payload.from, &payload.to, &key, &value);
        }
    }

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
    run_db(state.db.clone(), move |db| {
        insert_node_data(db, &node_id, &key, &value)
    })
    .await?;
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
    run_db(state.db.clone(), move |db| {
        insert_edge_data(db, &from, &to, &key, &value)
    })
    .await?;
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
    let nodes = run_db(state.db.clone(), move |db| {
        list_nodes_from_db(db, node_type.as_deref())
    })
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
    write_txn.open_table(NODE_INDEX_TABLE)?;
    write_txn.open_table(EDGE_INDEX_TABLE)?;
    write_txn.commit()?;
    Ok(())
}

fn load_graph(db: &Database) -> Result<Graph, redb::Error> {
    let mut graph = Graph::new();
    let mut decode_cache = LruCache::new(4096);

    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let read_txn = db.begin_read()?;
    let nodes_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODES_TABLE)?;
    for entry in nodes_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some(node_id) = decode_component_cached(key.value(), &mut decode_cache) {
            graph.add_node(&node_id);
        }
    }

    let edges_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGES_TABLE)?;
    for entry in edges_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id_encoded, to_id_encoded)) = split_two(key.value()) {
            let from_id = match decode_component_cached(from_id_encoded, &mut decode_cache) {
                Some(from_id) => from_id,
                None => continue,
            };
            let to_id = match decode_component_cached(to_id_encoded, &mut decode_cache) {
                Some(to_id) => to_id,
                None => continue,
            };
            graph.add_edge(&from_id, &to_id);
        }
    }

    let node_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_DATA_TABLE)?;
    for entry in node_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((node_id_encoded, data_key_encoded)) = split_two(key.value()) {
            let node_id = match decode_component_cached(node_id_encoded, &mut decode_cache) {
                Some(node_id) => node_id,
                None => continue,
            };
            let data_key = match decode_component_cached(data_key_encoded, &mut decode_cache) {
                Some(data_key) => data_key,
                None => continue,
            };
            graph.set_node_data(&node_id, &data_key, value.value());
        }
    }

    let edge_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_DATA_TABLE)?;
    for entry in edge_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id_encoded, to_id_encoded, data_key_encoded)) = split_three(key.value()) {
            let from_id = match decode_component_cached(from_id_encoded, &mut decode_cache) {
                Some(from_id) => from_id,
                None => continue,
            };
            let to_id = match decode_component_cached(to_id_encoded, &mut decode_cache) {
                Some(to_id) => to_id,
                None => continue,
            };
            let data_key = match decode_component_cached(data_key_encoded, &mut decode_cache) {
                Some(data_key) => data_key,
                None => continue,
            };
            graph.set_edge_data(&from_id, &to_id, &data_key, value.value());
        }
    }

    Ok(graph)
}

fn get_node_from_db(db: &Database, node_id: &str) -> Result<Option<NodeView>, redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let mut decode_cache = LruCache::new(2048);
    let read_txn = db.begin_read()?;
    let nodes_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODES_TABLE)?;
    let encoded_node_id = encode_component(node_id);
    if nodes_table.get(encoded_node_id.as_str())?.is_none() {
        return Ok(None);
    }

    let mut data = HashMap::new();
    let node_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_DATA_TABLE)?;
    for entry in node_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((id_encoded, data_key_encoded)) = split_two(key.value()) {
            let id = match decode_component_cached(id_encoded, &mut decode_cache) {
                Some(id) => id,
                None => continue,
            };
            if id == node_id {
                let data_key = match decode_component_cached(data_key_encoded, &mut decode_cache) {
                    Some(data_key) => data_key,
                    None => continue,
                };
                data.insert(data_key, value.value().to_string());
            }
        }
    }

    let mut edges = Vec::new();
    let mut edge_data: HashMap<String, HashMap<String, String>> = HashMap::new();
    let edge_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_DATA_TABLE)?;
    for entry in edge_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id_encoded, to_id_encoded, data_key_encoded)) = split_three(key.value()) {
            let from_id = match decode_component_cached(from_id_encoded, &mut decode_cache) {
                Some(from_id) => from_id,
                None => continue,
            };
            if from_id == node_id {
                let to_id = match decode_component_cached(to_id_encoded, &mut decode_cache) {
                    Some(to_id) => to_id,
                    None => continue,
                };
                let data_key = match decode_component_cached(data_key_encoded, &mut decode_cache) {
                    Some(data_key) => data_key,
                    None => continue,
                };
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
        if let Some((from_id_encoded, to_id_encoded)) = split_two(key.value()) {
            let from_id = match decode_component_cached(from_id_encoded, &mut decode_cache) {
                Some(from_id) => from_id,
                None => continue,
            };
            if from_id == node_id {
                let to_id = match decode_component_cached(to_id_encoded, &mut decode_cache) {
                    Some(to_id) => to_id,
                    None => continue,
                };
                let data = edge_data
                    .remove(&edge_key(&from_id, &to_id))
                    .unwrap_or_default();
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
    let mut decode_cache = LruCache::new(4096);

    if let Some(node_type) = node_type {
        let node_ids = list_node_ids_by_index(db, "type", node_type)?;
        if node_ids.is_empty() {
            return Ok(Vec::new());
        }

        let read_txn = db.begin_read()?;
        let nodes_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODES_TABLE)?;
        let node_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_DATA_TABLE)?;
        let edges_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGES_TABLE)?;
        let edge_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_DATA_TABLE)?;

        let mut nodes = Vec::new();
        for node_id in node_ids {
            let encoded_node_id = encode_component(&node_id);
            if nodes_table.get(encoded_node_id.as_str())?.is_none() {
                continue;
            }
            let data = load_node_data_for_id(&node_data_table, &node_id)?;
            let edge_data = load_edge_data_for_from(&edge_data_table, &node_id)?;
            let mut edges = Vec::new();
            let prefix = edge_from_prefix(&node_id);
            for entry in edges_table.range(prefix.as_str()..)? {
                let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
                let key_value = key.value();
                if !key_value.starts_with(&prefix) {
                    break;
                }
                if let Some((_, to_id_encoded)) = split_two(key_value) {
                    let to_id = match decode_component_cached(to_id_encoded, &mut decode_cache) {
                        Some(to_id) => to_id,
                        None => continue,
                    };
                    let data = edge_data.get(&to_id).cloned().unwrap_or_default();
                    edges.push(EdgeView { to: to_id, data });
                }
            }

            nodes.push(NodeView {
                id: node_id,
                data,
                edges,
            });
        }

        return Ok(nodes);
    }

    let read_txn = db.begin_read()?;
    let nodes_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODES_TABLE)?;
    let node_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_DATA_TABLE)?;
    let edges_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGES_TABLE)?;
    let edge_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_DATA_TABLE)?;

    let mut node_data: HashMap<String, HashMap<String, String>> = HashMap::new();
    for entry in node_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((node_id_encoded, data_key_encoded)) = split_two(key.value()) {
            let node_id = match decode_component_cached(node_id_encoded, &mut decode_cache) {
                Some(node_id) => node_id,
                None => continue,
            };
            let data_key = match decode_component_cached(data_key_encoded, &mut decode_cache) {
                Some(data_key) => data_key,
                None => continue,
            };
            node_data
                .entry(node_id)
                .or_default()
                .insert(data_key, value.value().to_string());
        }
    }

    let mut edges_by_from: HashMap<String, Vec<String>> = HashMap::new();
    for entry in edges_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id_encoded, to_id_encoded)) = split_two(key.value()) {
            let from_id = match decode_component_cached(from_id_encoded, &mut decode_cache) {
                Some(from_id) => from_id,
                None => continue,
            };
            let to_id = match decode_component_cached(to_id_encoded, &mut decode_cache) {
                Some(to_id) => to_id,
                None => continue,
            };
            edges_by_from.entry(from_id).or_default().push(to_id);
        }
    }

    let mut edge_data: HashMap<String, HashMap<String, String>> = HashMap::new();
    for entry in edge_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id_encoded, to_id_encoded, data_key_encoded)) = split_three(key.value()) {
            let from_id = match decode_component_cached(from_id_encoded, &mut decode_cache) {
                Some(from_id) => from_id,
                None => continue,
            };
            let to_id = match decode_component_cached(to_id_encoded, &mut decode_cache) {
                Some(to_id) => to_id,
                None => continue,
            };
            let data_key = match decode_component_cached(data_key_encoded, &mut decode_cache) {
                Some(data_key) => data_key,
                None => continue,
            };
            edge_data
                .entry(edge_key(&from_id, &to_id))
                .or_default()
                .insert(data_key, value.value().to_string());
        }
    }

    let mut nodes = Vec::new();
    for entry in nodes_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        let node_id = match decode_component_cached(key.value(), &mut decode_cache) {
            Some(node_id) => node_id,
            None => continue,
        };
        let data = node_data.remove(&node_id).unwrap_or_default();
        if let Some(node_type) = node_type {
            if data.get("type").map(|value| value.as_str()) != Some(node_type) {
                continue;
            }
        }

        let mut edges = Vec::new();
        if let Some(to_list) = edges_by_from.get(&node_id) {
            for to_id in to_list {
                let data = edge_data
                    .remove(&edge_key(&node_id, to_id))
                    .unwrap_or_default();
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
    let mut decode_cache = LruCache::new(4096);

    if let Some(edge_type) = edge_type {
        let edges = list_edge_ids_by_index(db, "type", edge_type)?;
        if edges.is_empty() {
            return Ok(Vec::new());
        }

        let read_txn = db.begin_read()?;
        let edges_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGES_TABLE)?;
        let edge_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_DATA_TABLE)?;

        let mut results = Vec::new();
        for (from_id, to_id) in edges {
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
            if edges_table
                .get(edge_key(&from_id, &to_id).as_str())?
                .is_none()
            {
                continue;
            }
            let data = load_edge_data_for_edge(&edge_data_table, &from_id, &to_id)?;
            results.push(EdgeListView {
                from: from_id,
                to: to_id,
                data,
            });
        }

        return Ok(results);
    }

    let read_txn = db.begin_read()?;
    let edges_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGES_TABLE)?;
    let edge_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_DATA_TABLE)?;

    if let Some(from_filter) = from_filter {
        let mut results = Vec::new();
        let edge_data = load_edge_data_for_from(&edge_data_table, from_filter)?;
        let prefix = edge_from_prefix(from_filter);
        for entry in edges_table.range(prefix.as_str()..)? {
            let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
            let key_value = key.value();
            if !key_value.starts_with(&prefix) {
                break;
            }
            if let Some((_, to_id_encoded)) = split_two(key_value) {
                let to_id = match decode_component_cached(to_id_encoded, &mut decode_cache) {
                    Some(to_id) => to_id,
                    None => continue,
                };
                if let Some(to_filter) = to_filter {
                    if to_id != to_filter {
                        continue;
                    }
                }
                let data = edge_data.get(&to_id).cloned().unwrap_or_default();
                results.push(EdgeListView {
                    from: from_filter.to_string(),
                    to: to_id,
                    data,
                });
            }
        }
        return Ok(results);
    }

    let mut edge_data: HashMap<String, HashMap<String, String>> = HashMap::new();
    for entry in edge_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id_encoded, to_id_encoded, data_key_encoded)) = split_three(key.value()) {
            let from_id = match decode_component_cached(from_id_encoded, &mut decode_cache) {
                Some(from_id) => from_id,
                None => continue,
            };
            let to_id = match decode_component_cached(to_id_encoded, &mut decode_cache) {
                Some(to_id) => to_id,
                None => continue,
            };
            let data_key = match decode_component_cached(data_key_encoded, &mut decode_cache) {
                Some(data_key) => data_key,
                None => continue,
            };
            edge_data
                .entry(edge_key(&from_id, &to_id))
                .or_default()
                .insert(data_key, value.value().to_string());
        }
    }

    let mut edges = Vec::new();
    for entry in edges_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((from_id_encoded, to_id_encoded)) = split_two(key.value()) {
            let from_id = match decode_component_cached(from_id_encoded, &mut decode_cache) {
                Some(from_id) => from_id,
                None => continue,
            };
            let to_id = match decode_component_cached(to_id_encoded, &mut decode_cache) {
                Some(to_id) => to_id,
                None => continue,
            };
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
        let encoded_node_id = encode_component(node_id);
        table.insert(encoded_node_id.as_str(), "")?;
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
        let previous = table
            .get(data_key.as_str())?
            .map(|value| value.value().to_string());
        table.insert(data_key.as_str(), value)?;
        drop(table);
        let mut index_table = write_txn.open_table(NODE_INDEX_TABLE)?;
        if let Some(previous) = previous {
            let previous_key = node_index_key(key, &previous, node_id);
            index_table.remove(previous_key.as_str())?;
        }
        let index_key = node_index_key(key, value, node_id);
        index_table.insert(index_key.as_str(), "")?;
    }
    write_txn.commit()?;
    Ok(())
}

fn insert_node_data_bulk(
    db: &Database,
    node_id: &str,
    data: &HashMap<String, String>,
) -> Result<(), redb::Error> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(NODE_DATA_TABLE)?;
        let mut index_table = write_txn.open_table(NODE_INDEX_TABLE)?;
        for (key, value) in data {
            let data_key = node_data_key(node_id, key);
            let previous = table
                .get(data_key.as_str())?
                .map(|value| value.value().to_string());
            table.insert(data_key.as_str(), value.as_str())?;
            if let Some(previous) = previous {
                let previous_key = node_index_key(key, &previous, node_id);
                index_table.remove(previous_key.as_str())?;
            }
            let index_key = node_index_key(key, value, node_id);
            index_table.insert(index_key.as_str(), "")?;
        }
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
        let previous = table
            .get(data_key.as_str())?
            .map(|value| value.value().to_string());
        table.insert(data_key.as_str(), value)?;
        drop(table);
        let mut index_table = write_txn.open_table(EDGE_INDEX_TABLE)?;
        if let Some(previous) = previous {
            let previous_key = edge_index_key(key, &previous, from, to);
            index_table.remove(previous_key.as_str())?;
        }
        let index_key = edge_index_key(key, value, from, to);
        index_table.insert(index_key.as_str(), "")?;
    }
    write_txn.commit()?;
    Ok(())
}

fn insert_edge_data_bulk(
    db: &Database,
    from: &str,
    to: &str,
    data: &HashMap<String, String>,
) -> Result<(), redb::Error> {
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table(EDGE_DATA_TABLE)?;
        let mut index_table = write_txn.open_table(EDGE_INDEX_TABLE)?;
        for (key, value) in data {
            let data_key = edge_data_key(from, to, key);
            let previous = table
                .get(data_key.as_str())?
                .map(|value| value.value().to_string());
            table.insert(data_key.as_str(), value.as_str())?;
            if let Some(previous) = previous {
                let previous_key = edge_index_key(key, &previous, from, to);
                index_table.remove(previous_key.as_str())?;
            }
            let index_key = edge_index_key(key, value, from, to);
            index_table.insert(index_key.as_str(), "")?;
        }
    }
    write_txn.commit()?;
    Ok(())
}
fn edge_key(from: &str, to: &str) -> String {
    format!(
        "{}{KEY_SEP}{}",
        encode_component(from),
        encode_component(to)
    )
}

fn node_index_key(key: &str, value: &str, node_id: &str) -> String {
    format!(
        "{}{KEY_SEP}{}{KEY_SEP}{}",
        encode_component(key),
        encode_component(value),
        encode_component(node_id)
    )
}

fn edge_index_key(key: &str, value: &str, from: &str, to: &str) -> String {
    format!(
        "{}{KEY_SEP}{}{KEY_SEP}{}{KEY_SEP}{}",
        encode_component(key),
        encode_component(value),
        encode_component(from),
        encode_component(to)
    )
}

fn node_data_key(node_id: &str, key: &str) -> String {
    format!(
        "{}{KEY_SEP}{}",
        encode_component(node_id),
        encode_component(key)
    )
}

fn edge_data_key(from: &str, to: &str, key: &str) -> String {
    format!(
        "{}{KEY_SEP}{}{KEY_SEP}{}",
        encode_component(from),
        encode_component(to),
        encode_component(key)
    )
}

fn node_data_prefix(node_id: &str) -> String {
    format!("{}{}", encode_component(node_id), KEY_SEP)
}

fn edge_from_prefix(from: &str) -> String {
    format!("{}{}", encode_component(from), KEY_SEP)
}

fn edge_data_prefix(from: &str, to: &str) -> String {
    format!(
        "{}{KEY_SEP}{}{KEY_SEP}",
        encode_component(from),
        encode_component(to)
    )
}

fn index_prefix(key: &str, value: &str) -> String {
    format!(
        "{}{KEY_SEP}{}{KEY_SEP}",
        encode_component(key),
        encode_component(value)
    )
}

fn split_two(value: &str) -> Option<(&str, &str)> {
    let mut parts = value.splitn(2, KEY_SEP);
    let first = parts.next()?;
    let second = parts.next()?;
    Some((first, second))
}

fn split_three(value: &str) -> Option<(&str, &str, &str)> {
    let mut parts = value.splitn(3, KEY_SEP);
    let first = parts.next()?;
    let second = parts.next()?;
    let third = parts.next()?;
    Some((first, second, third))
}

fn split_four(value: &str) -> Option<(&str, &str, &str, &str)> {
    let mut parts = value.splitn(4, KEY_SEP);
    let first = parts.next()?;
    let second = parts.next()?;
    let third = parts.next()?;
    let fourth = parts.next()?;
    Some((first, second, third, fourth))
}

fn split_two_decoded(value: &str) -> Option<(String, String)> {
    let (first, second) = split_two(value)?;
    Some((decode_component(first)?, decode_component(second)?))
}

fn split_three_decoded(value: &str) -> Option<(String, String, String)> {
    let (first, second, third) = split_three(value)?;
    Some((
        decode_component(first)?,
        decode_component(second)?,
        decode_component(third)?,
    ))
}

#[cfg(test)]
fn split_four_decoded(value: &str) -> Option<(String, String, String, String)> {
    let (first, second, third, fourth) = split_four(value)?;
    Some((
        decode_component(first)?,
        decode_component(second)?,
        decode_component(third)?,
        decode_component(fourth)?,
    ))
}

fn encode_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        if *byte == (KEY_SEP as u8) || *byte == b'%' || !byte.is_ascii() {
            encoded.push('%');
            encoded.push(nibble_to_hex(byte >> 4));
            encoded.push(nibble_to_hex(byte & 0x0f));
        } else {
            encoded.push(*byte as char);
        }
    }
    encoded
}

fn decode_component(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == b'%' {
            if index + 2 >= bytes.len() {
                return None;
            }
            let high = hex_to_nibble(bytes[index + 1])?;
            let low = hex_to_nibble(bytes[index + 2])?;
            decoded.push((high << 4) | low);
            index += 3;
        } else {
            decoded.push(byte);
            index += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

fn nibble_to_hex(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'A' + (value - 10)) as char,
        _ => '0',
    }
}

fn hex_to_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

struct LruCache {
    capacity: usize,
    order: VecDeque<String>,
    map: HashMap<String, String>,
}

impl LruCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            order: VecDeque::new(),
            map: HashMap::new(),
        }
    }

    fn get(&mut self, key: &str) -> Option<String> {
        if let Some(value) = self.map.get(key).cloned() {
            self.touch(key);
            return Some(value);
        }
        None
    }

    fn insert(&mut self, key: String, value: String) {
        if self.map.contains_key(&key) {
            self.map.insert(key.clone(), value);
            self.touch(&key);
            return;
        }

        if self.map.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.map.remove(&oldest);
            }
        }
        self.order.push_back(key.clone());
        self.map.insert(key, value);
    }

    fn touch(&mut self, key: &str) {
        if let Some(position) = self.order.iter().position(|entry| entry == key) {
            self.order.remove(position);
            self.order.push_back(key.to_string());
        }
    }
}

fn decode_component_cached(value: &str, cache: &mut LruCache) -> Option<String> {
    if let Some(cached) = cache.get(value) {
        return Some(cached);
    }
    let decoded = decode_component(value)?;
    cache.insert(value.to_string(), decoded.clone());
    Some(decoded)
}

fn load_node_data_for_id(
    node_data_table: &ReadOnlyTable<&str, &str>,
    node_id: &str,
) -> Result<HashMap<String, String>, redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let prefix = node_data_prefix(node_id);
    let mut data = HashMap::new();
    let mut decode_cache = LruCache::new(1024);
    for entry in node_data_table.range(prefix.as_str()..)? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        let key_value = key.value();
        if !key_value.starts_with(&prefix) {
            break;
        }
        if let Some((_, data_key_encoded)) = split_two(key_value) {
            let data_key = match decode_component_cached(data_key_encoded, &mut decode_cache) {
                Some(data_key) => data_key,
                None => continue,
            };
            data.insert(data_key, value.value().to_string());
        }
    }
    Ok(data)
}

fn load_edge_data_for_from(
    edge_data_table: &ReadOnlyTable<&str, &str>,
    from: &str,
) -> Result<HashMap<String, HashMap<String, String>>, redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let prefix = edge_from_prefix(from);
    let mut edge_data: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut decode_cache = LruCache::new(1024);
    for entry in edge_data_table.range(prefix.as_str()..)? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        let key_value = key.value();
        if !key_value.starts_with(&prefix) {
            break;
        }
        if let Some((_, to_id_encoded, data_key_encoded)) = split_three(key_value) {
            let to_id = match decode_component_cached(to_id_encoded, &mut decode_cache) {
                Some(to_id) => to_id,
                None => continue,
            };
            let data_key = match decode_component_cached(data_key_encoded, &mut decode_cache) {
                Some(data_key) => data_key,
                None => continue,
            };
            edge_data
                .entry(to_id)
                .or_default()
                .insert(data_key, value.value().to_string());
        }
    }
    Ok(edge_data)
}

fn load_edge_data_for_edge(
    edge_data_table: &ReadOnlyTable<&str, &str>,
    from: &str,
    to: &str,
) -> Result<HashMap<String, String>, redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let prefix = edge_data_prefix(from, to);
    let mut data = HashMap::new();
    let mut decode_cache = LruCache::new(512);
    for entry in edge_data_table.range(prefix.as_str()..)? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        let key_value = key.value();
        if !key_value.starts_with(&prefix) {
            break;
        }
        if let Some((_, _, data_key_encoded)) = split_three(key_value) {
            let data_key = match decode_component_cached(data_key_encoded, &mut decode_cache) {
                Some(data_key) => data_key,
                None => continue,
            };
            data.insert(data_key, value.value().to_string());
        }
    }
    Ok(data)
}

fn list_node_ids_by_index(
    db: &Database,
    key: &str,
    value: &str,
) -> Result<Vec<String>, redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let read_txn = db.begin_read()?;
    let index_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_INDEX_TABLE)?;
    let prefix = index_prefix(key, value);
    let mut results = Vec::new();
    let mut decode_cache = LruCache::new(2048);
    for entry in index_table.range(prefix.as_str()..)? {
        let (index_key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        let key_value = index_key.value();
        if !key_value.starts_with(&prefix) {
            break;
        }
        if let Some((_, _, node_id_encoded)) = split_three(key_value) {
            if let Some(node_id) = decode_component_cached(node_id_encoded, &mut decode_cache) {
                results.push(node_id);
            }
        }
    }
    Ok(results)
}

fn list_edge_ids_by_index(
    db: &Database,
    key: &str,
    value: &str,
) -> Result<Vec<(String, String)>, redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let read_txn = db.begin_read()?;
    let index_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_INDEX_TABLE)?;
    let prefix = index_prefix(key, value);
    let mut results = Vec::new();
    let mut decode_cache = LruCache::new(2048);
    for entry in index_table.range(prefix.as_str()..)? {
        let (index_key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        let key_value = index_key.value();
        if !key_value.starts_with(&prefix) {
            break;
        }
        if let Some((_, _, from_id_encoded, to_id_encoded)) = split_four(key_value) {
            let from_id = match decode_component_cached(from_id_encoded, &mut decode_cache) {
                Some(from_id) => from_id,
                None => continue,
            };
            let to_id = match decode_component_cached(to_id_encoded, &mut decode_cache) {
                Some(to_id) => to_id,
                None => continue,
            };
            results.push((from_id, to_id));
        }
    }
    Ok(results)
}

fn rebuild_indexes_if_empty(db: &Database) -> Result<(), redb::Error> {
    let read_txn = db.begin_read()?;
    let node_index_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_INDEX_TABLE)?;
    let edge_index_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_INDEX_TABLE)?;
    let node_index_empty = node_index_table.iter()?.next().is_none();
    let edge_index_empty = edge_index_table.iter()?.next().is_none();
    drop(read_txn);

    if !node_index_empty && !edge_index_empty {
        return Ok(());
    }

    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;
    if node_index_empty {
        let read_txn = db.begin_read()?;
        let node_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_DATA_TABLE)?;
        let mut entries = Vec::new();
        for entry in node_data_table.iter()? {
            let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
            if let Some((node_id, data_key)) = split_two_decoded(key.value()) {
                entries.push(node_index_key(&data_key, value.value(), &node_id));
            }
        }
        drop(read_txn);

        let write_txn = db.begin_write()?;
        {
            let mut index_table = write_txn.open_table(NODE_INDEX_TABLE)?;
            for index_key in entries {
                index_table.insert(index_key.as_str(), "")?;
            }
        }
        write_txn.commit()?;
    }
    if edge_index_empty {
        let read_txn = db.begin_read()?;
        let edge_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_DATA_TABLE)?;
        let mut entries = Vec::new();
        for entry in edge_data_table.iter()? {
            let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
            if let Some((from_id, to_id, data_key)) = split_three_decoded(key.value()) {
                entries.push(edge_index_key(&data_key, value.value(), &from_id, &to_id));
            }
        }
        drop(read_txn);

        let write_txn = db.begin_write()?;
        {
            let mut index_table = write_txn.open_table(EDGE_INDEX_TABLE)?;
            for index_key in entries {
                index_table.insert(index_key.as_str(), "")?;
            }
        }
        write_txn.commit()?;
    }
    Ok(())
}

fn db_error(error: redb::Error) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip_with_separators() {
        let original = "user\x1ftype%admin";
        let encoded = encode_component(original);
        let decoded = decode_component(&encoded).expect("decode failed");
        assert_eq!(decoded, original);
    }

    #[test]
    fn split_two_decoded_restores_components() {
        let from = "user\x1ftype\x1fadmin";
        let to = "team%42";
        let key = edge_key(from, to);
        let (decoded_from, decoded_to) = split_two_decoded(&key).expect("split_two_decoded failed");
        assert_eq!(decoded_from, from);
        assert_eq!(decoded_to, to);
    }

    #[test]
    fn split_three_decoded_restores_components() {
        let from = "user\x1ftype\x1fadmin";
        let to = "team%42";
        let data_key = "owner\x1ftype%";
        let key = edge_data_key(from, to, data_key);
        let (decoded_from, decoded_to, decoded_key) =
            split_three_decoded(&key).expect("split_three_decoded failed");
        assert_eq!(decoded_from, from);
        assert_eq!(decoded_to, to);
        assert_eq!(decoded_key, data_key);
    }

    #[test]
    fn split_four_decoded_restores_components() {
        let key = "type\x1fadmin";
        let value = "yes%true";
        let from = "user\x1ftype\x1fadmin";
        let to = "team%42";
        let index_key = edge_index_key(key, value, from, to);
        let (decoded_key, decoded_value, decoded_from, decoded_to) =
            split_four_decoded(&index_key).expect("split_four_decoded failed");
        assert_eq!(decoded_key, key);
        assert_eq!(decoded_value, value);
        assert_eq!(decoded_from, from);
        assert_eq!(decoded_to, to);
    }
}
