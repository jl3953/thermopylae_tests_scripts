import populate_crdb_data, snapshot_creator


def main():
    for i in [8, 16, 32, 64, 128, 5, 10, 25, 50, 100]:
        config = {
            "warm_nodes": [{
                "ip": "192.168.1.{}".format(i), "store": "/data",
                "region": "singapore",
            } for i in range(1, 11)], "commit_hash": "new-cloudlab",
            "name": "tpcc", "warehouses": i,
        }

        snapshot_creator.initialize_crdb(config)

        server_node = config["warm_nodes"][0]
        populate_crdb_data.snapshot(
            server_node, "snapshot/tpcc{}".format(
                config["warehouses"]
            ), "tpcc"
        )

    return 0
