use std::collections::{HashMap, HashSet};

#[derive(Debug)]
struct Node {
    id: String,
    neighbors: Vec<String>,
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
        if self.nodes.contains_key(from) && self.nodes.contains_key(to) {
            if let Some(node) = self.nodes.get_mut(from) {
                node.neighbors.push(to.to_string());
            }
        } else {
            println!("One or both nodes not found in the graph.");
        }
    }

    // fn get_neighbors(&self, id: &str) -> Option<&Vec<String>> {
    //     self.nodes.get(id)
    // }

    // fn has_node(&self, id: &str) -> bool {
    //     self.nodes.contains_key(id)
    // }

    // fn has_edge(&self, from: &str, to: &str) -> bool {
    //     if let Some(neighbors) = self.nodes.get(from) {
    //         neighbors.contains(&to.to_string())
    //     } else {
    //         false
    //     }
    // }

    // fn remove_node(&mut self, id: &str) {
    //     self.nodes.remove(id);
    //     for neighbors in self.nodes.values_mut() {
    //         neighbors.retain(|neighbor| neighbor != id);
    //     }
    // }

    // fn remove_edge(&mut self, from: &str, to: &str) {
    //     if let Some(neighbors) = self.nodes.get_mut(from) {
    //         neighbors.retain(|neighbor| neighbor != to);
    //     }
    // }

    fn set_node_data(&mut self, id: &str, key: &str, value: &str) {
        if let Some(node) = self.nodes.get_mut(id) {
            node.data.insert(key.to_string(), value.to_string());
        }
    }

    fn get_node_data(&self, id: &str, key: &str) -> Option<&String> {
        if let Some(node) = self.nodes.get(id) {
            return node.data.get(key);
        }
        None
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
                if !visited.contains(neighbor) {
                    if self.dfs(neighbor, target, visited) {
                        return true;
                    }
                }
            }
        }
        false
    }
}

fn main() {
    let mut graph = Graph::new();
    graph.add_node("Eu");
    graph.set_node_data("Eu", "name", "Bruno");
    graph.add_node("Rust");
    graph.add_node("Elo");
    graph.add_node("Python");

    graph.add_edge("Eu", "Rust");
    graph.add_edge("Eu", "Elo");
    graph.add_edge("Elo", "Python");

    let question = "Python";

    if graph.exist_path("Eu", question) {
        println!("There is a path from Eu to {}.", question);
    } else {
        println!("No path exists from Eu to {}.", question);
    }

    if graph.exist_path("Rust", question) {
        println!("There is a path from Rust to {}.", question);
    } else {
        println!("No path exists from Rust to {}.", question);
    }

    println!("Name of Eu: {:?}", graph.get_node_data("Eu", "name"));

    println!("Graph: {:?}", graph);
}
