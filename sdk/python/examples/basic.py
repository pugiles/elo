from elo import EloClient


def main() -> None:
    client = EloClient(base_url="http://127.0.0.1:3000", api_key="seu_token")

    client.create_node("user:123", data={"type": "user"})
    client.create_node("team:42", data={"type": "team", "rating": "520"})
    client.create_edge("user:123", "team:42", data={"type": "owner"})

    node = client.get_node("user:123")
    print(node.id, node.data, node.edges)

    teams = client.list_nodes(node_type="team")
    edges = client.list_edges(edge_type="owner")
    print(teams, edges)

    recs = client.recommendations(
        start="user:123",
        node_type="team",
        num_key="rating",
        min_value=300,
        max_value=900,
        limit=5,
    )
    print(recs)


if __name__ == "__main__":
    main()
