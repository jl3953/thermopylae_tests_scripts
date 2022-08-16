import sys, populate_crdb_data, snapshot_creator


def main():
    for i in [1, 500, 1000]:
        config = {
            "warm_nodes": [{
                "ip": "192.168.1.{}".format(i), "store": "/data",
                "region": "singapore",
            } for i in range(1, 11)], "commit_hash": "new-cloudlab",
            "name": "tpcc", "warehouses": i,
            "commit_hash": "staging-20.1.9-jenn",
        }

        snapshot_creator.initialize_crdb(config)

        server_node = config["warm_nodes"][0]
        populate_crdb_data.snapshot(
            server_node["ip"], "snapshot/tpcc{}".format(
                config["warehouses"]
            ), "tpcc"
        )

    return 0

 
if __name__ == "__main__":
    sys.exit(main())
