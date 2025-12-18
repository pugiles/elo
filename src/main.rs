use std::collections::HashMap;

#[derive(Debug)]
struct Graph {
    nodes: HashMap<String, Vec<String>>,
}

impl Graph {
    fn new() -> Self {
        Graph {
            nodes: HashMap::new(),
        }
    }

    fn add_node(&mut self, id: &str) {
        self.nodes.entry(id.to_string()).or_insert(Vec::new());
    }

    fn add_edge(&mut self, from: &str, to: &str) {
        if self.nodes.contains_key(from) && self.nodes.contains_key(to) {
            if let Some(neighbors) = self.nodes.get_mut(from) {
                neighbors.push(to.to_string());
            }
        } else {
            println!("One or both nodes not found in the graph.");
        }
    }
}


fn main() {
    let mut graph = Graph::new();
    graph.add_node("Eu");
    graph.add_node("Rust");
    graph.add_node("Elo");
    graph.add_node("Python");

    graph.add_edge("Eu", "Rust");
    graph.add_edge("Eu", "Elo");
    graph.add_edge("Elo", "Python");

    println!("{:#?}", graph);
}