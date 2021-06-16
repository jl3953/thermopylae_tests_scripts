import unittest

import config_object
import node


class ConfigObjectTest(unittest.TestCase):

    def setUp(self) -> None:
        self.config_object = config_object.ConfigObject()

        # default
        self.config_object.logs_dir = ["test"]
        self.config_object.store_dir = ["kv-skew"]
        self.config_object.trials = [1]

        # cluster
        self.config_object.cockroach_commit = ["master"]
        self.config_object.num_warm_nodes = [4]
        self.config_object.num_workload_nodes = [6]
        self.config_object.driver_node_ip_enum = [1]

        # self.config_object.workload_nodes = [] # to be populated
        # self.config_object.warm_nodes = [] # to be populated
        self.config_object.hot_key_threshold = [-1]
        self.config_object.should_create_partition = [False]
        self.config_object.disable_cores = [2, 4, 6]

        # benchmark
        self.config_object.keyspace = [1000000]
        # self.config_object.concurrency = [] # to be populated
        self.config_object.warm_up_duration = [10]  # in seconds
        self.config_object.duration = [2]  # in seconds
        self.config_object.read_percent = [100]  # percentage
        self.config_object.n_keys_per_statement = [6]
        self.config_object.use_original_zipfian = [False]
        self.config_object.distribution_type = ["zipf"]
        self.config_object.skews = [0, 0.9]

    def test_generate_ini_filename(self):
        ini = config_object.ConfigObject.generate_ini_filename("suffix", "uniqueprefix")
        self.assertEqual(
            "/root/thermopylae_tests/config/uniqueprefix_suffix.ini",
            ini)

    def test_enumerate_workload_nodes(self):
        workload_nodes, _ = config_object.ConfigObject.enumerate_workload_nodes(1, 6)
        correct_workload_nodes = [node.Node(1), node.Node(2), node.Node(3), node.Node(4),
                                  node.Node(5), node.Node(6)]

        self.assertCountEqual(correct_workload_nodes, workload_nodes)

    def test_enumerate_warm_nodes(self):
        warm_nodes = config_object.ConfigObject.enumerate_warm_nodes(4, 7)

        correct_warm_nodes = [node.Node(7, "newyork", "/data"),
                              node.Node(8, "london", "/data"),
                              node.Node(9, "tokyo", "/data"),
                              node.Node(10, "singapore", "/data")]

        self.assertCountEqual(correct_warm_nodes, warm_nodes)

    def test_generate_config_combinations(self):
        config_dicts = self.config_object.generate_config_combinations()

        self.assertEqual(6, len(config_dicts))
        self.assertEqual(3, sum(c["skews"] == 0 for c in config_dicts))
        self.assertEqual(2, sum(c["disable_cores"] == 2 for c in config_dicts))
        self.assertEqual(2, sum(c["disable_cores"] == 2 for c in config_dicts))
        self.assertEqual(6, sum(c["keyspace"] == 1000000 for c in config_dicts))


if __name__ == '__main__':
    unittest.main()
