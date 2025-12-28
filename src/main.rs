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
const GEO_INDEX_TABLE: TableDefinition<&str, &str> = TableDefinition::new("geo_index");
const SCHEMA_TABLE: TableDefinition<&str, &str> = TableDefinition::new("schema");
const STATUS_KEY: &str = "status";
const STATUS_ACTIVE: &str = "active";
const STATUS_DELETED: &str = "deleted";

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

#[derive(Clone, Debug, Default)]
struct SchemaCache {
    node_fields: HashSet<String>,
    edge_fields: HashSet<String>,
    node_defined: bool,
    edge_defined: bool,
}

impl SchemaCache {
    fn node_in_memory(&self, key: &str) -> bool {
        !self.node_defined || self.node_fields.contains(key)
    }

    fn edge_in_memory(&self, key: &str) -> bool {
        !self.edge_defined || self.edge_fields.contains(key)
    }

    fn filter_node_data(&self, data: &HashMap<String, String>) -> HashMap<String, String> {
        if !self.node_defined {
            return data.clone();
        }
        data.iter()
            .filter(|(key, _)| self.node_fields.contains(*key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    fn filter_edge_data(&self, data: &HashMap<String, String>) -> HashMap<String, String> {
        if !self.edge_defined {
            return data.clone();
        }
        data.iter()
            .filter(|(key, _)| self.edge_fields.contains(*key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
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

    fn edge_type(&self, from: &str, to: &str) -> Option<&str> {
        self.nodes
            .get(from)
            .and_then(|node| node.neighbors.iter().find(|edge| edge.to == to))
            .and_then(|edge| edge.data.get("type").map(|value| value.as_str()))
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

    fn remove_edge(&mut self, from: &str, to: &str) {
        if let Some(node) = self.nodes.get_mut(from) {
            node.neighbors.retain(|edge| edge.to != to);
        }
    }

    fn remove_node(&mut self, id: &str) {
        self.nodes.remove(id);
        for node in self.nodes.values_mut() {
            node.neighbors.retain(|edge| edge.to != id);
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
    schema: Arc<RwLock<SchemaCache>>,
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
struct BlockPayload {
    from: String,
    to: String,
    data: Option<HashMap<String, String>>,
}

#[derive(Deserialize)]
struct UpdateNodeData {
    data: HashMap<String, String>,
}

#[derive(Deserialize)]
struct UpdateEdgeData {
    from: String,
    to: String,
    data: HashMap<String, String>,
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
    hydrate: Option<bool>,
}

#[derive(Deserialize)]
struct EdgeListQuery {
    r#type: Option<String>,
    from: Option<String>,
    to: Option<String>,
    hydrate: Option<bool>,
}

#[derive(Deserialize)]
struct DeleteEdgeQuery {
    from: String,
    to: String,
}

#[derive(Deserialize)]
struct SchemaPayload {
    entity: String,
    fields: Vec<String>,
}

#[derive(Deserialize)]
struct SchemaQuery {
    entity: Option<String>,
}

#[derive(Deserialize)]
struct HydrateQuery {
    hydrate: Option<bool>,
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
    geo_key: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
    radius_km: Option<f64>,
    hydrate: Option<bool>,
    exclude_edge_types: Option<String>,
    exclude_ids: Option<String>,
}

#[derive(Serialize)]
struct Recommendation {
    id: String,
    score: f64,
    data: HashMap<String, String>,
}

#[derive(Deserialize)]
struct NearbyQuery {
    start: Option<String>,
    r#type: String,
    geo_hash_prefix: Option<String>,
    geo_hash_key: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
    radius_km: Option<f64>,
    limit: Option<usize>,
    hydrate: Option<bool>,
    exclude_edge_types: Option<String>,
    exclude_ids: Option<String>,
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
    ensure_status_defaults(db.as_ref())?;
    let schema = load_schema(db.as_ref())?;
    let graph = load_graph(db.as_ref(), &schema)?;

    let state = AppState {
        graph: Arc::new(RwLock::new(graph)),
        db,
        api_key: Arc::new(api_key),
        schema: Arc::new(RwLock::new(schema)),
    };

    let app = Router::new()
        .route("/nodes", post(create_node).get(list_nodes))
        .route(
            "/nodes/{id}",
            get(get_node).patch(update_node_data).delete(delete_node),
        )
        .route("/nodes/{id}/data", put(set_node_data))
        .route(
            "/edges",
            post(create_edge)
                .put(set_edge_data)
                .patch(update_edge_data)
                .get(list_edges)
                .delete(delete_edge),
        )
        .route("/blocks", post(create_block).delete(delete_block))
        .route("/schema", post(upsert_schema).get(get_schema))
        .route("/path", get(check_path))
        .route("/recommendations", get(recommend_nodes))
        .route("/nearby", get(list_nearby))
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
    let mut data = payload.data.unwrap_or_default();
    if !data.contains_key(STATUS_KEY) {
        data.insert(STATUS_KEY.to_string(), STATUS_ACTIVE.to_string());
    }
    run_db(state.db.clone(), move |db| insert_node(db, &node_id)).await?;
    let schema = state.schema.read().await.clone();
    let mut graph = state.graph.write().await;
    graph.add_node(&payload.id);
    let node_id = payload.id.clone();
    let data_clone = data.clone();
    run_db(state.db.clone(), move |db| insert_node_data_bulk(db, &node_id, &data_clone))
        .await?;
    for (key, value) in data {
        if schema.node_in_memory(&key) {
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
    let mut data = payload.data.unwrap_or_default();
    if !data.contains_key(STATUS_KEY) {
        data.insert(STATUS_KEY.to_string(), STATUS_ACTIVE.to_string());
    }
    let is_block = data
        .get("type")
        .map(|value| value.as_str() == "block")
        .unwrap_or(false);
    run_db(state.db.clone(), move |db| insert_edge(db, &from, &to)).await?;
    let schema = state.schema.read().await.clone();
    let mut graph = state.graph.write().await;
    graph.add_edge(&payload.from, &payload.to);
    let from_id = payload.from.clone();
    let to_id = payload.to.clone();
    let data_clone = data.clone();
    run_db(state.db.clone(), move |db| insert_edge_data_bulk(db, &from_id, &to_id, &data_clone))
        .await?;
    for (key, value) in &data {
        if schema.edge_in_memory(&key) {
            graph.set_edge_data(&payload.from, &payload.to, &key, &value);
        }
    }

    if is_block && payload.from != payload.to {
        let reverse_from = payload.to.clone();
        let reverse_to = payload.from.clone();
        let reverse_data = data.clone();
        if !graph.has_edge(&payload.to, &payload.from) {
            run_db(state.db.clone(), move |db| insert_edge(db, &reverse_from, &reverse_to)).await?;
            graph.add_edge(&payload.to, &payload.from);
        }
        let reverse_from = payload.to.clone();
        let reverse_to = payload.from.clone();
        let reverse_data_clone = reverse_data.clone();
        run_db(state.db.clone(), move |db| {
            insert_edge_data_bulk(db, &reverse_from, &reverse_to, &reverse_data_clone)
        })
        .await?;
        for (key, value) in &reverse_data {
            if schema.edge_in_memory(key) {
                graph.set_edge_data(&payload.to, &payload.from, key, value);
            }
        }
    }

    Ok(StatusCode::CREATED)
}

async fn create_block(
    State(state): State<AppState>,
    Json(payload): Json<BlockPayload>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let graph = state.graph.read().await;
        if !graph.has_node(&payload.from) || !graph.has_node(&payload.to) {
            return Err((StatusCode::NOT_FOUND, "node not found".to_string()));
        }
    }
    let from = payload.from.clone();
    let to = payload.to.clone();
    let mut data = payload.data.unwrap_or_default();
    data.insert("type".to_string(), "block".to_string());
    if !data.contains_key(STATUS_KEY) {
        data.insert(STATUS_KEY.to_string(), STATUS_ACTIVE.to_string());
    }
    run_db(state.db.clone(), move |db| insert_edge(db, &from, &to)).await?;
    let schema = state.schema.read().await.clone();
    let mut graph = state.graph.write().await;
    graph.add_edge(&payload.from, &payload.to);
    let from_id = payload.from.clone();
    let to_id = payload.to.clone();
    let data_clone = data.clone();
    run_db(state.db.clone(), move |db| insert_edge_data_bulk(db, &from_id, &to_id, &data_clone))
        .await?;
    for (key, value) in &data {
        if schema.edge_in_memory(key) {
            graph.set_edge_data(&payload.from, &payload.to, key, value);
        }
    }

    if payload.from != payload.to {
        let reverse_from = payload.to.clone();
        let reverse_to = payload.from.clone();
        let reverse_data = data.clone();
        if !graph.has_edge(&payload.to, &payload.from) {
            run_db(state.db.clone(), move |db| insert_edge(db, &reverse_from, &reverse_to)).await?;
            graph.add_edge(&payload.to, &payload.from);
        }
        let reverse_from = payload.to.clone();
        let reverse_to = payload.from.clone();
        let reverse_data_clone = reverse_data.clone();
        run_db(state.db.clone(), move |db| {
            insert_edge_data_bulk(db, &reverse_from, &reverse_to, &reverse_data_clone)
        })
        .await?;
        for (key, value) in &reverse_data {
            if schema.edge_in_memory(key) {
                graph.set_edge_data(&payload.to, &payload.from, key, value);
            }
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
    let schema = state.schema.read().await.clone();
    let mut graph = state.graph.write().await;
    if schema.node_in_memory(&payload.key) {
        graph.set_node_data(&id, &payload.key, &payload.value);
    }
    if payload.key == STATUS_KEY && payload.value == STATUS_DELETED {
        graph.remove_node(&id);
    }

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
    let schema = state.schema.read().await.clone();
    let mut graph = state.graph.write().await;
    if schema.edge_in_memory(&payload.key) {
        graph.set_edge_data(&payload.from, &payload.to, &payload.key, &payload.value);
    }
    if payload.key == STATUS_KEY && payload.value == STATUS_DELETED {
        graph.remove_edge(&payload.from, &payload.to);
    }
    if payload.key == "type" && payload.value == "block" && payload.from != payload.to {
        let reverse_from = payload.to.clone();
        let reverse_to = payload.from.clone();
        let mut reverse_data = HashMap::new();
        reverse_data.insert("type".to_string(), "block".to_string());
        reverse_data.insert(STATUS_KEY.to_string(), STATUS_ACTIVE.to_string());
        if !graph.has_edge(&payload.to, &payload.from) {
            run_db(state.db.clone(), move |db| insert_edge(db, &reverse_from, &reverse_to)).await?;
            graph.add_edge(&payload.to, &payload.from);
        }
        let reverse_from = payload.to.clone();
        let reverse_to = payload.from.clone();
        let reverse_data_clone = reverse_data.clone();
        run_db(state.db.clone(), move |db| {
            insert_edge_data_bulk(db, &reverse_from, &reverse_to, &reverse_data_clone)
        })
        .await?;
        if schema.edge_in_memory("type") {
            graph.set_edge_data(&payload.to, &payload.from, "type", "block");
        }
        if schema.edge_in_memory(STATUS_KEY) {
            graph.set_edge_data(&payload.to, &payload.from, STATUS_KEY, STATUS_ACTIVE);
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

async fn update_node_data(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(payload): Json<UpdateNodeData>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let graph = state.graph.read().await;
        if !graph.has_node(&id) {
            return Err((StatusCode::NOT_FOUND, "node not found".to_string()));
        }
    }
    let node_id = id.clone();
    let data_clone = payload.data.clone();
    run_db(state.db.clone(), move |db| {
        insert_node_data_bulk(db, &node_id, &data_clone)
    })
    .await?;
    let schema = state.schema.read().await.clone();
    let mut graph = state.graph.write().await;
    let mut deleted = false;
    for (key, value) in payload.data {
        if schema.node_in_memory(&key) {
            graph.set_node_data(&id, &key, &value);
        }
        if key == STATUS_KEY && value == STATUS_DELETED {
            deleted = true;
        }
    }
    if deleted {
        graph.remove_node(&id);
    }

    Ok(StatusCode::NO_CONTENT)
}

async fn update_edge_data(
    State(state): State<AppState>,
    Json(payload): Json<UpdateEdgeData>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let graph = state.graph.read().await;
        if !graph.has_edge(&payload.from, &payload.to) {
            return Err((StatusCode::NOT_FOUND, "edge not found".to_string()));
        }
    }
    let from = payload.from.clone();
    let to = payload.to.clone();
    let data_clone = payload.data.clone();
    let is_block = data_clone
        .get("type")
        .map(|value| value.as_str() == "block")
        .unwrap_or(false);
    let from_db = from.clone();
    let to_db = to.clone();
    run_db(state.db.clone(), move |db| {
        insert_edge_data_bulk(db, &from_db, &to_db, &data_clone)
    })
    .await?;
    let schema = state.schema.read().await.clone();
    let mut graph = state.graph.write().await;
    let mut deleted = false;
    for (key, value) in payload.data {
        if schema.edge_in_memory(&key) {
            graph.set_edge_data(&from, &to, &key, &value);
        }
        if key == STATUS_KEY && value == STATUS_DELETED {
            deleted = true;
        }
    }
    if deleted {
        graph.remove_edge(&from, &to);
    }
    if is_block && payload.from != payload.to {
        let reverse_from = payload.to.clone();
        let reverse_to = payload.from.clone();
        let mut reverse_data = payload.data.clone();
        if !reverse_data.contains_key(STATUS_KEY) {
            reverse_data.insert(STATUS_KEY.to_string(), STATUS_ACTIVE.to_string());
        }
        if !graph.has_edge(&payload.to, &payload.from) {
            run_db(state.db.clone(), move |db| insert_edge(db, &reverse_from, &reverse_to)).await?;
            graph.add_edge(&payload.to, &payload.from);
        }
        let reverse_from = payload.to.clone();
        let reverse_to = payload.from.clone();
        let reverse_data_clone = reverse_data.clone();
        run_db(state.db.clone(), move |db| {
            insert_edge_data_bulk(db, &reverse_from, &reverse_to, &reverse_data_clone)
        })
        .await?;
        for (key, value) in &reverse_data {
            if schema.edge_in_memory(key) {
                graph.set_edge_data(&payload.to, &payload.from, key, value);
            }
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

async fn delete_node(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, String)> {
    let node_id = id.clone();
    let deleted = run_db(state.db.clone(), move |db| soft_delete_node(db, &node_id)).await?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "node not found".to_string()));
    }
    let mut graph = state.graph.write().await;
    graph.remove_node(&id);
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_edge(
    State(state): State<AppState>,
    Query(query): Query<DeleteEdgeQuery>,
) -> Result<StatusCode, (StatusCode, String)> {
    let from = query.from.clone();
    let to = query.to.clone();
    let is_block = {
        let graph = state.graph.read().await;
        graph.edge_type(&from, &to) == Some("block")
    };
    let deleted = run_db(state.db.clone(), move |db| soft_delete_edge(db, &from, &to)).await?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "edge not found".to_string()));
    }
    if is_block && query.from != query.to {
        let reverse_from = query.to.clone();
        let reverse_to = query.from.clone();
        let _ = run_db(state.db.clone(), move |db| {
            soft_delete_edge(db, &reverse_from, &reverse_to)
        })
        .await?;
    }
    let mut graph = state.graph.write().await;
    graph.remove_edge(&query.from, &query.to);
    if is_block && query.from != query.to {
        graph.remove_edge(&query.to, &query.from);
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn delete_block(
    State(state): State<AppState>,
    Query(query): Query<DeleteEdgeQuery>,
) -> Result<StatusCode, (StatusCode, String)> {
    {
        let graph = state.graph.read().await;
        if !graph.has_edge(&query.from, &query.to) {
            return Err((StatusCode::NOT_FOUND, "edge not found".to_string()));
        }
    }
    let from = query.from.clone();
    let to = query.to.clone();
    let deleted = run_db(state.db.clone(), move |db| soft_delete_edge(db, &from, &to)).await?;
    if !deleted {
        return Err((StatusCode::NOT_FOUND, "edge not found".to_string()));
    }
    if query.from != query.to {
        let reverse_from = query.to.clone();
        let reverse_to = query.from.clone();
        let _ = run_db(state.db.clone(), move |db| {
            soft_delete_edge(db, &reverse_from, &reverse_to)
        })
        .await?;
    }
    let mut graph = state.graph.write().await;
    graph.remove_edge(&query.from, &query.to);
    if query.from != query.to {
        graph.remove_edge(&query.to, &query.from);
    }
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize)]
struct SchemaView {
    entity: String,
    fields: Vec<String>,
}

async fn upsert_schema(
    State(state): State<AppState>,
    Json(payload): Json<SchemaPayload>,
) -> Result<StatusCode, (StatusCode, String)> {
    let entity = payload.entity.trim().to_lowercase();
    if entity != "node" && entity != "edge" {
        return Err((
            StatusCode::BAD_REQUEST,
            "entity must be node or edge".to_string(),
        ));
    }

    let mut fields: Vec<String> = payload
        .fields
        .into_iter()
        .map(|field| field.trim().to_string())
        .filter(|field| !field.is_empty())
        .collect();
    if fields.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "fields must not be empty".to_string(),
        ));
    }
    fields.sort();
    fields.dedup();

    let fields_clone = fields.clone();
    let entity_clone = entity.clone();
    run_db(state.db.clone(), move |db| save_schema(db, &entity_clone, &fields_clone)).await?;

    {
        let mut schema = state.schema.write().await;
        if entity == "node" {
            schema.node_fields = fields.into_iter().collect();
            schema.node_defined = true;
        } else {
            schema.edge_fields = fields.into_iter().collect();
            schema.edge_defined = true;
        }
    }

    let schema_snapshot = state.schema.read().await.clone();
    let graph = run_db(state.db.clone(), move |db| load_graph(db, &schema_snapshot)).await?;
    let mut graph_lock = state.graph.write().await;
    *graph_lock = graph;

    Ok(StatusCode::NO_CONTENT)
}

async fn get_schema(
    State(state): State<AppState>,
    Query(query): Query<SchemaQuery>,
) -> Result<Json<Vec<SchemaView>>, (StatusCode, String)> {
    let schema = state.schema.read().await;
    let mut result = Vec::new();

    if let Some(entity) = query.entity.as_deref() {
        if entity != "node" && entity != "edge" {
            return Err((
                StatusCode::BAD_REQUEST,
                "entity must be node or edge".to_string(),
            ));
        }
    }

    let want_node = query
        .entity
        .as_deref()
        .map(|value| value == "node")
        .unwrap_or(true);
    let want_edge = query
        .entity
        .as_deref()
        .map(|value| value == "edge")
        .unwrap_or(true);

    if want_node {
        let mut fields: Vec<String> = schema.node_fields.iter().cloned().collect();
        fields.sort();
        result.push(SchemaView {
            entity: "node".to_string(),
            fields,
        });
    }
    if want_edge {
        let mut fields: Vec<String> = schema.edge_fields.iter().cloned().collect();
        fields.sort();
        result.push(SchemaView {
            entity: "edge".to_string(),
            fields,
        });
    }

    Ok(Json(result))
}

async fn get_node(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<HydrateQuery>,
) -> Result<Json<NodeView>, (StatusCode, String)> {
    let node_id = id.clone();
    let schema = state.schema.read().await.clone();
    let hydrate = query.hydrate.unwrap_or(true);
    let node = run_db(state.db.clone(), move |db| {
        get_node_from_db(db, &node_id, &schema, hydrate)
    })
    .await?;
    let node = node.ok_or((StatusCode::NOT_FOUND, "node not found".to_string()))?;

    Ok(Json(node))
}

async fn list_nodes(
    State(state): State<AppState>,
    Query(query): Query<NodeListQuery>,
) -> Result<Json<Vec<NodeView>>, (StatusCode, String)> {
    let node_type = query.r#type.clone();
    let hydrate = query.hydrate.unwrap_or(true);
    let schema = state.schema.read().await.clone();
    let nodes = run_db(state.db.clone(), move |db| {
        list_nodes_from_db(db, node_type.as_deref(), &schema, hydrate)
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
    let hydrate = query.hydrate.unwrap_or(true);
    let schema = state.schema.read().await.clone();
    let edges = run_db(state.db.clone(), move |db| {
        list_edges_from_db(
            db,
            edge_type.as_deref(),
            from.as_deref(),
            to.as_deref(),
            &schema,
            hydrate,
        )
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
    let mut results = {
        let graph = state.graph.read().await;
        let start = graph
            .get_node(&query.start)
            .ok_or((StatusCode::NOT_FOUND, "start node not found".to_string()))?;

        let geo_key = query.geo_key.as_deref().unwrap_or("location");
        let start_point = match (query.lat, query.lon) {
            (Some(lat), Some(lon)) => Some((lat, lon)),
            _ => start
                .data
                .get(geo_key)
                .and_then(|value| parse_geo_point(value)),
        };

        if query.radius_km.is_some() && start_point.is_none() {
            return Err((
                StatusCode::BAD_REQUEST,
                "radius_km requires lat/lon or start node location".to_string(),
            ));
        }

        let excluded_edge_types = parse_csv_set(&query.exclude_edge_types);
        let excluded_ids = parse_csv_set(&query.exclude_ids);
        let mut blocked_targets: HashSet<String> = HashSet::new();
        let mut direct_neighbors: HashSet<String> = HashSet::new();
        for edge in &start.neighbors {
            if is_edge_type_excluded(edge, &excluded_edge_types) {
                blocked_targets.insert(edge.to.clone());
                continue;
            }
            direct_neighbors.insert(edge.to.clone());
        }

        let mut scores: HashMap<String, f64> = HashMap::new();
        for edge in &start.neighbors {
            if is_edge_type_excluded(edge, &excluded_edge_types) {
                continue;
            }
            let weight1 = edge_weight(edge);
            if let Some(node) = graph.get_node(&edge.to) {
                for edge2 in &node.neighbors {
                    if is_edge_type_excluded(edge2, &excluded_edge_types) {
                        continue;
                    }
                    let candidate = &edge2.to;
                    if candidate == &query.start {
                        continue;
                    }
                    if blocked_targets.contains(candidate) || excluded_ids.contains(candidate) {
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

            if let (Some(radius_km), Some(origin)) = (query.radius_km, start_point) {
                let node_point = node
                    .data
                    .get(geo_key)
                    .and_then(|value| parse_geo_point(value));
                let node_point = match node_point {
                    Some(point) => point,
                    None => continue,
                };
                if haversine_km(origin, node_point) > radius_km {
                    continue;
                }
            }

            results.push(Recommendation {
                id: node.id.clone(),
                score,
                data: node.data.clone(),
            });
        }

        results
    };

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

    let hydrate = query.hydrate.unwrap_or(true);
    if hydrate && !results.is_empty() {
        let ids: Vec<String> = results.iter().map(|item| item.id.clone()).collect();
        let hydrated = run_db(state.db.clone(), move |db| load_node_data_for_ids(db, &ids))
            .await?;
        for item in &mut results {
            if let Some(data) = hydrated.get(&item.id) {
                item.data = data.clone();
            }
        }
    }

    Ok(Json(results))
}

async fn list_nearby(
    State(state): State<AppState>,
    Query(query): Query<NearbyQuery>,
) -> Result<Json<Vec<NodeView>>, (StatusCode, String)> {
    let geo_hash_prefix = if let Some(prefix) = query.geo_hash_prefix.as_deref() {
        if prefix.is_empty() {
            return Err((
                StatusCode::BAD_REQUEST,
                "geo_hash_prefix cannot be empty".to_string(),
            ));
        }
        prefix.to_string()
    } else {
        let (lat, lon) = match (query.lat, query.lon) {
            (Some(lat), Some(lon)) => (lat, lon),
            _ => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "geo_hash_prefix or lat/lon is required".to_string(),
                ))
            }
        };
        let radius_km = query.radius_km.unwrap_or(10.0);
        let precision = geohash_precision_for_km(radius_km);
        encode_geohash(lat, lon, precision)
    };

    let geo_hash_key = query
        .geo_hash_key
        .clone()
        .unwrap_or_else(|| "geo_hash".to_string());
    let node_type = query.r#type.clone();
    let limit = query.limit;
    let hydrate = query.hydrate.unwrap_or(true);
    let excluded_edge_types = parse_csv_set(&query.exclude_edge_types);
    let mut excluded_ids = parse_csv_set(&query.exclude_ids);
    if let Some(start_id) = query.start.as_deref() {
        let graph = state.graph.read().await;
        let start = graph
            .get_node(start_id)
            .ok_or((StatusCode::NOT_FOUND, "start node not found".to_string()))?;
        for edge in &start.neighbors {
            if is_edge_type_excluded(edge, &excluded_edge_types) {
                excluded_ids.insert(edge.to.clone());
            }
        }
    }
    let schema = state.schema.read().await.clone();
    let db_limit = if excluded_ids.is_empty() { limit } else { None };

    let mut results = run_db(state.db.clone(), move |db| {
        list_nodes_by_geo_prefix(
            db,
            node_type.as_str(),
            geo_hash_key.as_str(),
            geo_hash_prefix.as_str(),
            db_limit,
            &schema,
            hydrate,
        )
    })
    .await?;

    if !excluded_ids.is_empty() {
        results.retain(|node| !excluded_ids.contains(&node.id));
    }
    if let Some(limit) = limit {
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

fn parse_csv_set(value: &Option<String>) -> HashSet<String> {
    value
        .as_deref()
        .unwrap_or("")
        .split(',')
        .map(|item| item.trim())
        .filter(|item| !item.is_empty())
        .map(|item| item.to_string())
        .collect()
}

fn is_edge_type_excluded(edge: &Edge, excluded: &HashSet<String>) -> bool {
    match edge.data.get("type") {
        Some(value) => excluded.contains(value),
        None => false,
    }
}

fn parse_geo_point(value: &str) -> Option<(f64, f64)> {
    let mut parts = value.split(',');
    let lat = parts.next()?.trim().parse::<f64>().ok()?;
    let lon = parts.next()?.trim().parse::<f64>().ok()?;
    if lat < -90.0 || lat > 90.0 || lon < -180.0 || lon > 180.0 {
        return None;
    }
    Some((lat, lon))
}

fn haversine_km(origin: (f64, f64), point: (f64, f64)) -> f64 {
    let (lat1, lon1) = (origin.0.to_radians(), origin.1.to_radians());
    let (lat2, lon2) = (point.0.to_radians(), point.1.to_radians());
    let dlat = lat2 - lat1;
    let dlon = lon2 - lon1;
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    6371.0 * c
}

fn geohash_precision_for_km(radius_km: f64) -> usize {
    if radius_km <= 0.0 {
        return 9;
    }
    let sizes = [
        (1, 5000.0),
        (2, 1250.0),
        (3, 156.0),
        (4, 39.1),
        (5, 4.89),
        (6, 1.22),
        (7, 0.153),
        (8, 0.0382),
        (9, 0.00477),
    ];
    for (precision, size_km) in sizes {
        if size_km >= radius_km {
            return precision;
        }
    }
    1
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

fn init_db(db: &Database) -> Result<(), redb::Error> {
    let write_txn = db.begin_write()?;
    write_txn.open_table(NODES_TABLE)?;
    write_txn.open_table(NODE_DATA_TABLE)?;
    write_txn.open_table(EDGES_TABLE)?;
    write_txn.open_table(EDGE_DATA_TABLE)?;
    write_txn.open_table(NODE_INDEX_TABLE)?;
    write_txn.open_table(EDGE_INDEX_TABLE)?;
    write_txn.open_table(GEO_INDEX_TABLE)?;
    write_txn.open_table(SCHEMA_TABLE)?;
    write_txn.commit()?;
    Ok(())
}

fn serialize_schema_fields(fields: &[String]) -> String {
    let sep = KEY_SEP.to_string();
    let mut encoded: Vec<String> = fields.iter().map(|field| encode_component(field)).collect();
    encoded.sort();
    encoded.dedup();
    encoded.join(&sep)
}

fn deserialize_schema_fields(value: &str) -> Vec<String> {
    if value.is_empty() {
        return Vec::new();
    }
    value
        .split(KEY_SEP)
        .filter_map(|entry| decode_component(entry))
        .filter(|entry| !entry.is_empty())
        .collect()
}

fn load_schema(db: &Database) -> Result<SchemaCache, redb::Error> {
    let read_txn = db.begin_read()?;
    let schema_table: ReadOnlyTable<&str, &str> = read_txn.open_table(SCHEMA_TABLE)?;
    let mut schema = SchemaCache::default();

    if let Some(value) = schema_table.get("node")? {
        let fields = deserialize_schema_fields(value.value());
        if !fields.is_empty() {
            schema.node_fields = fields.into_iter().collect();
            schema.node_defined = true;
        }
    }
    if let Some(value) = schema_table.get("edge")? {
        let fields = deserialize_schema_fields(value.value());
        if !fields.is_empty() {
            schema.edge_fields = fields.into_iter().collect();
            schema.edge_defined = true;
        }
    }

    Ok(schema)
}

fn save_schema(db: &Database, entity: &str, fields: &[String]) -> Result<(), redb::Error> {
    let write_txn = db.begin_write()?;
    {
        let mut schema_table = write_txn.open_table(SCHEMA_TABLE)?;
        let value = serialize_schema_fields(fields);
        schema_table.insert(entity, value.as_str())?;
    }
    write_txn.commit()?;
    Ok(())
}

fn ensure_status_defaults(db: &Database) -> Result<(), redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;
    let mut decode_cache = LruCache::new(4096);

    let read_txn = db.begin_read()?;
    let nodes_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODES_TABLE)?;
    let node_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_DATA_TABLE)?;
    let edges_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGES_TABLE)?;
    let edge_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_DATA_TABLE)?;

    let mut nodes_with_status = HashSet::new();
    for entry in node_data_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((node_id_encoded, data_key_encoded)) = split_two(key.value()) {
            let node_id = match decode_component_cached(node_id_encoded, &mut decode_cache) {
                Some(node_id) => node_id,
                None => continue,
            };
            let data_key = match decode_component_cached(data_key_encoded, &mut decode_cache) {
                Some(data_key) => data_key,
                None => continue,
            };
            if data_key == STATUS_KEY {
                nodes_with_status.insert(node_id);
            }
        }
    }

    let mut edges_with_status = HashSet::new();
    for entry in edge_data_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
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
            if data_key == STATUS_KEY {
                edges_with_status.insert(edge_key(&from_id, &to_id));
            }
        }
    }

    let mut missing_nodes = Vec::new();
    for entry in nodes_table.iter()? {
        let (key, _): (StrGuard<'_>, StrGuard<'_>) = entry?;
        let node_id = match decode_component_cached(key.value(), &mut decode_cache) {
            Some(node_id) => node_id,
            None => continue,
        };
        if !nodes_with_status.contains(&node_id) {
            missing_nodes.push(node_id);
        }
    }

    let mut missing_edges = Vec::new();
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
            if !edges_with_status.contains(&edge_key(&from_id, &to_id)) {
                missing_edges.push((from_id, to_id));
            }
        }
    }

    if missing_nodes.is_empty() && missing_edges.is_empty() {
        return Ok(());
    }

    let write_txn = db.begin_write()?;
    {
        if !missing_nodes.is_empty() {
            let mut node_data_table = write_txn.open_table(NODE_DATA_TABLE)?;
            let mut node_index_table = write_txn.open_table(NODE_INDEX_TABLE)?;
            let mut geo_index_table = write_txn.open_table(GEO_INDEX_TABLE)?;
            for node_id in missing_nodes {
                let data_key = node_data_key(&node_id, STATUS_KEY);
                node_data_table.insert(data_key.as_str(), STATUS_ACTIVE)?;
                let index_key = node_index_key(STATUS_KEY, STATUS_ACTIVE, &node_id);
                node_index_table.insert(index_key.as_str(), "")?;
                let geo_key = geo_index_key(STATUS_KEY, STATUS_ACTIVE, &node_id);
                geo_index_table.insert(geo_key.as_str(), "")?;
            }
        }

        if !missing_edges.is_empty() {
            let mut edge_data_table = write_txn.open_table(EDGE_DATA_TABLE)?;
            let mut edge_index_table = write_txn.open_table(EDGE_INDEX_TABLE)?;
            for (from_id, to_id) in missing_edges {
                let data_key = edge_data_key(&from_id, &to_id, STATUS_KEY);
                edge_data_table.insert(data_key.as_str(), STATUS_ACTIVE)?;
                let index_key = edge_index_key(STATUS_KEY, STATUS_ACTIVE, &from_id, &to_id);
                edge_index_table.insert(index_key.as_str(), "")?;
            }
        }
    }
    write_txn.commit()?;
    Ok(())
}

fn load_graph(db: &Database, schema: &SchemaCache) -> Result<Graph, redb::Error> {
    let mut graph = Graph::new();
    let mut decode_cache = LruCache::new(4096);

    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let active_nodes = list_node_ids_by_index(db, STATUS_KEY, STATUS_ACTIVE)?;
    if active_nodes.is_empty() {
        return Ok(graph);
    }
    let active_node_set: HashSet<String> = active_nodes.iter().cloned().collect();
    for node_id in &active_nodes {
        graph.add_node(node_id);
    }

    let active_edges = list_edge_ids_by_index(db, STATUS_KEY, STATUS_ACTIVE)?;
    let mut active_edge_keys = HashSet::new();
    for (from_id, to_id) in active_edges {
        if !active_node_set.contains(&from_id) || !active_node_set.contains(&to_id) {
            continue;
        }
        graph.add_edge(&from_id, &to_id);
        active_edge_keys.insert(edge_key(&from_id, &to_id));
    }

    let read_txn = db.begin_read()?;
    let node_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_DATA_TABLE)?;
    for entry in node_data_table.iter()? {
        let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
        if let Some((node_id_encoded, data_key_encoded)) = split_two(key.value()) {
            let node_id = match decode_component_cached(node_id_encoded, &mut decode_cache) {
                Some(node_id) => node_id,
                None => continue,
            };
            if !active_node_set.contains(&node_id) {
                continue;
            }
            let data_key = match decode_component_cached(data_key_encoded, &mut decode_cache) {
                Some(data_key) => data_key,
                None => continue,
            };
            if schema.node_in_memory(&data_key) {
                graph.set_node_data(&node_id, &data_key, value.value());
            }
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
            let edge_key_value = edge_key(&from_id, &to_id);
            if !active_edge_keys.contains(&edge_key_value) {
                continue;
            }
            let data_key = match decode_component_cached(data_key_encoded, &mut decode_cache) {
                Some(data_key) => data_key,
                None => continue,
            };
            if schema.edge_in_memory(&data_key) {
                graph.set_edge_data(&from_id, &to_id, &data_key, value.value());
            }
        }
    }

    Ok(graph)
}

fn is_active_value(value: Option<&String>) -> bool {
    match value {
        Some(value) => value == STATUS_ACTIVE,
        None => true,
    }
}

fn is_active_data(data: &HashMap<String, String>) -> bool {
    is_active_value(data.get(STATUS_KEY))
}

fn get_node_from_db(
    db: &Database,
    node_id: &str,
    schema: &SchemaCache,
    hydrate: bool,
) -> Result<Option<NodeView>, redb::Error> {
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
    if !is_active_data(&data) {
        return Ok(None);
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
                if !is_active_data(&data) {
                    continue;
                }
                let data = if hydrate {
                    data
                } else {
                    schema.filter_edge_data(&data)
                };
                edges.push(EdgeView { to: to_id, data });
            }
        }
    }

    let data = if hydrate {
        data
    } else {
        schema.filter_node_data(&data)
    };

    Ok(Some(NodeView {
        id: node_id.to_string(),
        data,
        edges,
    }))
}

fn list_nodes_from_db(
    db: &Database,
    node_type: Option<&str>,
    schema: &SchemaCache,
    hydrate: bool,
) -> Result<Vec<NodeView>, redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;
    let mut decode_cache = LruCache::new(4096);

    let active_nodes = list_node_ids_by_index(db, STATUS_KEY, STATUS_ACTIVE)?;
    if active_nodes.is_empty() {
        return Ok(Vec::new());
    }
    let active_set: HashSet<String> = active_nodes.iter().cloned().collect();

    let node_ids = if let Some(node_type) = node_type {
        let typed_nodes = list_node_ids_by_index(db, "type", node_type)?;
        typed_nodes
            .into_iter()
            .filter(|node_id| active_set.contains(node_id))
            .collect::<Vec<_>>()
    } else {
        active_nodes
    };
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
        let mut data = load_node_data_for_id(&node_data_table, &node_id)?;
        if !is_active_data(&data) {
            continue;
        }
        if let Some(node_type) = node_type {
            if data.get("type").map(|value| value.as_str()) != Some(node_type) {
                continue;
            }
        }
        if !hydrate {
            data = schema.filter_node_data(&data);
        }
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
                if !active_set.contains(&to_id) {
                    continue;
                }
                let mut data = edge_data.get(&to_id).cloned().unwrap_or_default();
                if !is_active_data(&data) {
                    continue;
                }
                if !hydrate {
                    data = schema.filter_edge_data(&data);
                }
                edges.push(EdgeView { to: to_id, data });
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

fn list_nodes_by_geo_prefix(
    db: &Database,
    node_type: &str,
    geo_key: &str,
    geo_prefix: &str,
    limit: Option<usize>,
    schema: &SchemaCache,
    hydrate: bool,
) -> Result<Vec<NodeView>, redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let node_ids = list_node_ids_by_geo_prefix(db, geo_key, geo_prefix)?;
    if node_ids.is_empty() {
        return Ok(Vec::new());
    }
    let active_nodes = list_node_ids_by_index(db, STATUS_KEY, STATUS_ACTIVE)?;
    if active_nodes.is_empty() {
        return Ok(Vec::new());
    }
    let active_set: HashSet<String> = active_nodes.into_iter().collect();
    let node_ids = node_ids
        .into_iter()
        .filter(|node_id| active_set.contains(node_id))
        .collect::<Vec<_>>();
    if node_ids.is_empty() {
        return Ok(Vec::new());
    }

    let read_txn = db.begin_read()?;
    let nodes_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODES_TABLE)?;
    let node_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_DATA_TABLE)?;
    let edges_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGES_TABLE)?;
    let edge_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGE_DATA_TABLE)?;

    let mut decode_cache = LruCache::new(4096);
    let mut nodes = Vec::new();
    for node_id in node_ids {
        let encoded_node_id = encode_component(&node_id);
        if nodes_table.get(encoded_node_id.as_str())?.is_none() {
            continue;
        }
        let mut data = load_node_data_for_id(&node_data_table, &node_id)?;
        if !is_active_data(&data) {
            continue;
        }
        if data.get("type").map(|value| value.as_str()) != Some(node_type) {
            continue;
        }
        if !hydrate {
            data = schema.filter_node_data(&data);
        }
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
                if !active_set.contains(&to_id) {
                    continue;
                }
                let mut data = edge_data.get(&to_id).cloned().unwrap_or_default();
                if !is_active_data(&data) {
                    continue;
                }
                if !hydrate {
                    data = schema.filter_edge_data(&data);
                }
                edges.push(EdgeView { to: to_id, data });
            }
        }

        nodes.push(NodeView {
            id: node_id,
            data,
            edges,
        });
    }

    nodes.sort_by(|left, right| left.id.cmp(&right.id));
    if let Some(limit) = limit {
        nodes.truncate(limit);
    }

    Ok(nodes)
}

fn list_edges_from_db(
    db: &Database,
    edge_type: Option<&str>,
    from_filter: Option<&str>,
    to_filter: Option<&str>,
    schema: &SchemaCache,
    hydrate: bool,
) -> Result<Vec<EdgeListView>, redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;
    let mut decode_cache = LruCache::new(4096);
    let active_edges = list_edge_ids_by_index(db, STATUS_KEY, STATUS_ACTIVE)?;
    if active_edges.is_empty() {
        return Ok(Vec::new());
    }
    let mut active_edge_keys = HashSet::new();
    for (from_id, to_id) in active_edges {
        active_edge_keys.insert(edge_key(&from_id, &to_id));
    }

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
            if !active_edge_keys.contains(&edge_key(&from_id, &to_id)) {
                continue;
            }
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
            let mut data = load_edge_data_for_edge(&edge_data_table, &from_id, &to_id)?;
            if !hydrate {
                data = schema.filter_edge_data(&data);
            }
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
                if !active_edge_keys.contains(&edge_key(from_filter, &to_id)) {
                    continue;
                }
                let mut data = edge_data.get(&to_id).cloned().unwrap_or_default();
                if !is_active_data(&data) {
                    continue;
                }
                if !hydrate {
                    data = schema.filter_edge_data(&data);
                }
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
            if !active_edge_keys.contains(&edge_key(&from_id, &to_id)) {
                continue;
            }
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

            let mut data = edge_data
                .remove(&edge_key(&from_id, &to_id))
                .unwrap_or_default();
            if !is_active_data(&data) {
                continue;
            }
            if !hydrate {
                data = schema.filter_edge_data(&data);
            }
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

fn soft_delete_node(db: &Database, node_id: &str) -> Result<bool, redb::Error> {
    let exists = {
        let read_txn = db.begin_read()?;
        let nodes_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODES_TABLE)?;
        let encoded_node_id = encode_component(node_id);
        nodes_table.get(encoded_node_id.as_str())?.is_some()
    };
    if !exists {
        return Ok(false);
    }
    insert_node_data(db, node_id, STATUS_KEY, STATUS_DELETED)?;
    mark_edges_deleted_for_node(db, node_id)?;
    Ok(true)
}

fn soft_delete_edge(db: &Database, from: &str, to: &str) -> Result<bool, redb::Error> {
    let exists = {
        let read_txn = db.begin_read()?;
        let edges_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGES_TABLE)?;
        edges_table.get(edge_key(from, to).as_str())?.is_some()
    };
    if !exists {
        return Ok(false);
    }
    insert_edge_data(db, from, to, STATUS_KEY, STATUS_DELETED)?;
    Ok(true)
}

fn mark_edges_deleted_for_node(db: &Database, node_id: &str) -> Result<(), redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;
    let mut decode_cache = LruCache::new(4096);
    let read_txn = db.begin_read()?;
    let edges_table: ReadOnlyTable<&str, &str> = read_txn.open_table(EDGES_TABLE)?;
    let mut edges_to_delete = Vec::new();
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
            if from_id == node_id || to_id == node_id {
                edges_to_delete.push((from_id, to_id));
            }
        }
    }

    if edges_to_delete.is_empty() {
        return Ok(());
    }

    let write_txn = db.begin_write()?;
    {
        let mut edge_data_table = write_txn.open_table(EDGE_DATA_TABLE)?;
        let mut edge_index_table = write_txn.open_table(EDGE_INDEX_TABLE)?;
        for (from_id, to_id) in edges_to_delete {
            let data_key = edge_data_key(&from_id, &to_id, STATUS_KEY);
            let previous = edge_data_table
                .get(data_key.as_str())?
                .map(|value| value.value().to_string());
            edge_data_table.insert(data_key.as_str(), STATUS_DELETED)?;
            if let Some(previous) = previous {
                let previous_key = edge_index_key(STATUS_KEY, &previous, &from_id, &to_id);
                edge_index_table.remove(previous_key.as_str())?;
            }
            let index_key = edge_index_key(STATUS_KEY, STATUS_DELETED, &from_id, &to_id);
            edge_index_table.insert(index_key.as_str(), "")?;
        }
    }
    write_txn.commit()?;
    Ok(())
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
        let mut geo_index_table = write_txn.open_table(GEO_INDEX_TABLE)?;
        if let Some(previous) = previous {
            let previous_key = node_index_key(key, &previous, node_id);
            index_table.remove(previous_key.as_str())?;
            let previous_key = geo_index_key(key, &previous, node_id);
            geo_index_table.remove(previous_key.as_str())?;
        }
        let index_key = node_index_key(key, value, node_id);
        index_table.insert(index_key.as_str(), "")?;
        let geo_key = geo_index_key(key, value, node_id);
        geo_index_table.insert(geo_key.as_str(), "")?;
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
        let mut geo_index_table = write_txn.open_table(GEO_INDEX_TABLE)?;
        for (key, value) in data {
            let data_key = node_data_key(node_id, key);
            let previous = table
                .get(data_key.as_str())?
                .map(|value| value.value().to_string());
            table.insert(data_key.as_str(), value.as_str())?;
            if let Some(previous) = previous {
                let previous_key = node_index_key(key, &previous, node_id);
                index_table.remove(previous_key.as_str())?;
                let previous_key = geo_index_key(key, &previous, node_id);
                geo_index_table.remove(previous_key.as_str())?;
            }
            let index_key = node_index_key(key, value, node_id);
            index_table.insert(index_key.as_str(), "")?;
            let geo_key = geo_index_key(key, value, node_id);
            geo_index_table.insert(geo_key.as_str(), "")?;
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

fn geo_index_key(key: &str, value: &str, node_id: &str) -> String {
    format!(
        "{}{KEY_SEP}{}{KEY_SEP}{}",
        encode_component(key),
        encode_component(value),
        encode_component(node_id)
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

fn geo_index_prefix(key: &str, prefix: &str) -> String {
    format!(
        "{}{KEY_SEP}{}",
        encode_component(key),
        encode_component(prefix)
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

fn load_node_data_for_ids(
    db: &Database,
    node_ids: &[String],
) -> Result<HashMap<String, HashMap<String, String>>, redb::Error> {
    let read_txn = db.begin_read()?;
    let node_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_DATA_TABLE)?;
    let mut results = HashMap::new();
    for node_id in node_ids {
        let data = load_node_data_for_id(&node_data_table, node_id)?;
        if !is_active_data(&data) {
            continue;
        }
        results.insert(node_id.clone(), data);
    }
    Ok(results)
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

fn list_node_ids_by_geo_prefix(
    db: &Database,
    key: &str,
    prefix_value: &str,
) -> Result<Vec<String>, redb::Error> {
    type StrGuard<'a> = redb::AccessGuard<'a, &'static str>;

    let read_txn = db.begin_read()?;
    let index_table: ReadOnlyTable<&str, &str> = read_txn.open_table(GEO_INDEX_TABLE)?;
    let prefix = geo_index_prefix(key, prefix_value);
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
    let geo_index_table: ReadOnlyTable<&str, &str> = read_txn.open_table(GEO_INDEX_TABLE)?;
    let node_index_empty = node_index_table.iter()?.next().is_none();
    let edge_index_empty = edge_index_table.iter()?.next().is_none();
    let geo_index_empty = geo_index_table.iter()?.next().is_none();
    drop(read_txn);

    if !node_index_empty && !edge_index_empty && !geo_index_empty {
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
    if geo_index_empty {
        let read_txn = db.begin_read()?;
        let node_data_table: ReadOnlyTable<&str, &str> = read_txn.open_table(NODE_DATA_TABLE)?;
        let mut entries = Vec::new();
        for entry in node_data_table.iter()? {
            let (key, value): (StrGuard<'_>, StrGuard<'_>) = entry?;
            if let Some((node_id, data_key)) = split_two_decoded(key.value()) {
                entries.push(geo_index_key(&data_key, value.value(), &node_id));
            }
        }
        drop(read_txn);

        let write_txn = db.begin_write()?;
        {
            let mut index_table = write_txn.open_table(GEO_INDEX_TABLE)?;
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
