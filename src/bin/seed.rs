use redb::{Database, TableDefinition};
use std::collections::HashSet;
use std::env;
use std::fs;

const KEY_SEP: char = '\x1f';

const NODES_TABLE: TableDefinition<&str, &str> = TableDefinition::new("nodes");
const EDGES_TABLE: TableDefinition<&str, &str> = TableDefinition::new("edges");
const NODE_DATA_TABLE: TableDefinition<&str, &str> = TableDefinition::new("node_data");
const EDGE_DATA_TABLE: TableDefinition<&str, &str> = TableDefinition::new("edge_data");
const NODE_INDEX_TABLE: TableDefinition<&str, &str> = TableDefinition::new("node_index");
const EDGE_INDEX_TABLE: TableDefinition<&str, &str> = TableDefinition::new("edge_index");

struct Lcg {
    state: u64,
}

impl Lcg {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u32(&mut self) -> u32 {
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
        (self.state >> 32) as u32
    }

    fn gen_range(&mut self, max: u32) -> u32 {
        if max == 0 { 0 } else { self.next_u32() % max }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = env::var("ELO_DB_PATH").unwrap_or_else(|_| "elo.redb".to_string());
    let reset = env::var("SEED_RESET").ok().as_deref() == Some("true");
    if reset {
        let _ = fs::remove_file(&db_path);
    }

    let num_users = env::var("SEED_USERS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(100_000);
    let num_teams = env::var("SEED_TEAMS")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(10_000);
    let user_edges = env::var("SEED_USER_EDGES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(5);
    let team_edges = env::var("SEED_TEAM_EDGES")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(5);
    let rating_min = env::var("SEED_RATING_MIN")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(300);
    let rating_max = env::var("SEED_RATING_MAX")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(900);
    let batch_size = env::var("SEED_BATCH")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(10_000);
    let rng_seed = env::var("SEED_RANDOM")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(42);

    let db = Database::open(db_path.as_str()).or_else(|_| Database::create(db_path.as_str()))?;
    init_db(&db)?;

    let mut rng = Lcg::new(rng_seed);
    seed_nodes(
        &db, num_users, num_teams, rating_min, rating_max, batch_size, &mut rng,
    )?;
    seed_edges(
        &db, num_users, num_teams, user_edges, team_edges, batch_size, &mut rng,
    )?;

    println!(
        "Seeded users={}, teams={}, user_edges={}, team_edges={}",
        num_users, num_teams, user_edges, team_edges
    );
    Ok(())
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

fn seed_nodes(
    db: &Database,
    num_users: u32,
    num_teams: u32,
    rating_min: u32,
    rating_max: u32,
    batch_size: u32,
    rng: &mut Lcg,
) -> Result<(), redb::Error> {
    let mut current = 0;
    while current < num_users {
        let end = (current + batch_size).min(num_users);
        let write_txn = db.begin_write()?;
        {
            let mut nodes = write_txn.open_table(NODES_TABLE)?;
            let mut node_data = write_txn.open_table(NODE_DATA_TABLE)?;
            let mut node_index = write_txn.open_table(NODE_INDEX_TABLE)?;
            for idx in current..end {
                let node_id = format!("user:{idx}");
                nodes.insert(encode_component(&node_id).as_str(), "")?;
                insert_node_data(&node_id, "type", "user", &mut node_data, &mut node_index)?;
            }
        }
        write_txn.commit()?;
        current = end;
    }

    let mut current = 0;
    while current < num_teams {
        let end = (current + batch_size).min(num_teams);
        let write_txn = db.begin_write()?;
        {
            let mut nodes = write_txn.open_table(NODES_TABLE)?;
            let mut node_data = write_txn.open_table(NODE_DATA_TABLE)?;
            let mut node_index = write_txn.open_table(NODE_INDEX_TABLE)?;
            for idx in current..end {
                let node_id = format!("team:{idx}");
                nodes.insert(encode_component(&node_id).as_str(), "")?;
                insert_node_data(&node_id, "type", "team", &mut node_data, &mut node_index)?;
                let rating = rating_min + rng.gen_range(rating_max - rating_min + 1);
                insert_node_data(
                    &node_id,
                    "rating",
                    rating.to_string().as_str(),
                    &mut node_data,
                    &mut node_index,
                )?;
            }
        }
        write_txn.commit()?;
        current = end;
    }

    Ok(())
}

fn seed_edges(
    db: &Database,
    num_users: u32,
    num_teams: u32,
    user_edges: u32,
    team_edges: u32,
    batch_size: u32,
    rng: &mut Lcg,
) -> Result<(), redb::Error> {
    let mut current = 0;
    while current < num_users {
        let end = (current + batch_size).min(num_users);
        let write_txn = db.begin_write()?;
        {
            let mut edges = write_txn.open_table(EDGES_TABLE)?;
            let mut edge_data = write_txn.open_table(EDGE_DATA_TABLE)?;
            let mut edge_index = write_txn.open_table(EDGE_INDEX_TABLE)?;
            for user_idx in current..end {
                let user_id = format!("user:{user_idx}");
                let mut chosen = HashSet::new();
                while (chosen.len() as u32) < user_edges && chosen.len() < num_teams as usize {
                    let team_idx = rng.gen_range(num_teams);
                    if chosen.insert(team_idx) {
                        let team_id = format!("team:{team_idx}");
                        insert_edge(&user_id, &team_id, &mut edges)?;
                        insert_edge_data(
                            &user_id,
                            &team_id,
                            "type",
                            "owner",
                            &mut edge_data,
                            &mut edge_index,
                        )?;
                    }
                }
            }
        }
        write_txn.commit()?;
        current = end;
    }

    let mut current = 0;
    while current < num_teams {
        let end = (current + batch_size).min(num_teams);
        let write_txn = db.begin_write()?;
        {
            let mut edges = write_txn.open_table(EDGES_TABLE)?;
            let mut edge_data = write_txn.open_table(EDGE_DATA_TABLE)?;
            let mut edge_index = write_txn.open_table(EDGE_INDEX_TABLE)?;
            for team_idx in current..end {
                let from_id = format!("team:{team_idx}");
                let mut chosen = HashSet::new();
                while (chosen.len() as u32) < team_edges && chosen.len() < num_teams as usize {
                    let to_idx = rng.gen_range(num_teams);
                    if to_idx == team_idx {
                        continue;
                    }
                    if chosen.insert(to_idx) {
                        let to_id = format!("team:{to_idx}");
                        insert_edge(&from_id, &to_id, &mut edges)?;
                        let weight = 0.5 + (rng.gen_range(150) as f64 / 100.0);
                        insert_edge_data(
                            &from_id,
                            &to_id,
                            "weight",
                            format!("{weight:.2}").as_str(),
                            &mut edge_data,
                            &mut edge_index,
                        )?;
                    }
                }
            }
        }
        write_txn.commit()?;
        current = end;
    }

    Ok(())
}

fn insert_node_data(
    node_id: &str,
    key: &str,
    value: &str,
    node_data: &mut redb::Table<&str, &str>,
    node_index: &mut redb::Table<&str, &str>,
) -> Result<(), redb::Error> {
    let data_key = node_data_key(node_id, key);
    node_data.insert(data_key.as_str(), value)?;
    let index_key = node_index_key(key, value, node_id);
    node_index.insert(index_key.as_str(), "")?;
    Ok(())
}

fn insert_edge(
    from: &str,
    to: &str,
    edges: &mut redb::Table<&str, &str>,
) -> Result<(), redb::Error> {
    let key = edge_key(from, to);
    edges.insert(key.as_str(), "")?;
    Ok(())
}

fn insert_edge_data(
    from: &str,
    to: &str,
    key: &str,
    value: &str,
    edge_data: &mut redb::Table<&str, &str>,
    edge_index: &mut redb::Table<&str, &str>,
) -> Result<(), redb::Error> {
    let data_key = edge_data_key(from, to, key);
    edge_data.insert(data_key.as_str(), value)?;
    let index_key = edge_index_key(key, value, from, to);
    edge_index.insert(index_key.as_str(), "")?;
    Ok(())
}

fn edge_key(from: &str, to: &str) -> String {
    format!(
        "{}{KEY_SEP}{}",
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

fn nibble_to_hex(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'A' + (value - 10)) as char,
        _ => '0',
    }
}
