import datetime
import itertools
import os

import config_io
import constants
import node


class ConfigObject:
    """Represents different combinations of configuration parameters."""

    def __init__(self):

        ##### JENN, WARNING: IF YOU ADD A KEY HERE, ADD IT TO THE SQLITE
        # TABLE TOO ####

        # default
        self.logs_dir = ["test"]
        self.store_dir = ["kv-skew"]
        self.trials = [1]

        # cluster
        self.cockroach_commit = ["cloudlab"]
        self.num_warm_nodes = [3]
        self.num_workload_nodes = [3]
        self.driver_node_ip_enum = [1]
        self.generate_latency_throughput = [True]
        self.warm_nodes_eq_workload_nodes = [True]
        self.discrete_warmup_and_trial = [True]

        # self.workload_nodes = [] # to be populated
        # self.warm_nodes = [] # to be populated
        self.hot_node = [vars(node.Node(12))]  # 192.168.1.?? of hotnode
        self.hot_node_port = [50051]
        self.hot_node_commit_branch = ["cloudlab"]
        self.hot_node_concurrency = [16]
        self.crdb_grpc_port = [50055]
        self.prepromote_min = [0]
        self.prepromote_max = [40000000]
        self.hot_key_threshold = [-1]
        self.should_create_partition = [False]
        self.disable_cores = [0]
        self.warm_up_duration = [120]  # in seconds
        self.duration = [60]  # in seconds

        # benchmark
        self.hash_randomize_keyspace = [True]
        self.enable_fixed_sized_encoding = [True]
        self.name = ["kv"]
        self.keyspace = [100000000]
        # self.concurrency = [] # to be populated
        self.read_percent = [95]  # percentage
        self.n_keys_per_statement = [1]
        self.use_original_zipfian = [False]
        self.distribution_type = ["zipf"]
        self.skews = [0.01, 0.99, 1.2]

        # self.name = ["tpcc"]
        # self.warehouses = [10]
        # self.mix = ["newOrder=10,payment=10,orderStatus=1,delivery=1,"
        #             "stockLevel=1"]
        # self.init_with_fixture = [False]
        # self.wait = [False]
        #### notes to run tpcc
        # just comment out the hot_node for now

    def generate_config_combinations(self):
        """Generates the trial configuration parameters for a single run,
        lists all in a list of dicts.

    :return: a list of dictionaries of combinations
    """
        temp_dict = vars(self)

        all_field_values = list(temp_dict.values())
        values_combinations = list(itertools.product(*all_field_values))

        combinations = []
        for combo in values_combinations:
            config_dict = dict(zip(temp_dict.keys(), combo))
            combinations.append(config_dict)

        for config_dict in combinations:

            # see if this is a scalability experiment
            num_workload_nodes = config_dict["num_workload_nodes"]
            if config_dict["warm_nodes_eq_workload_nodes"]:
                num_workload_nodes = config_dict["num_warm_nodes"]

            driver_node_ip_enum = config_dict["driver_node_ip_enum"]
            workload_nodes, ending_enum = ConfigObject.enumerate_workload_nodes(
                driver_node_ip_enum, num_workload_nodes, 12
            )
            config_dict["workload_nodes"] = [vars(n) for n in workload_nodes]

            num_warm_nodes = config_dict["num_warm_nodes"]
            starting_server_node = ending_enum + 1
            warm_nodes = ConfigObject.enumerate_warm_nodes(
                num_warm_nodes, starting_server_node, 12
            )
            config_dict["warm_nodes"] = [vars(n) for n in warm_nodes]

        return combinations

    def generate_all_config_files(self):
        """Generates all configuration files with different combinations of
        parameters.
    :return:
    """
        ini_fpaths = []
        config_combos = self.generate_config_combinations()
        for config_dict in config_combos:
            ini_fpath = ConfigObject.generate_ini_filename(
                suffix=config_dict["logs_dir"]
            )
            ini_fpaths.append(
                config_io.write_config_to_file(config_dict, ini_fpath)
            )

        return ini_fpaths

    @staticmethod
    def generate_ini_filename(suffix=None, custom_unique_prefix=None):
        """Generates a filename for ini using datetime as unique id.

    :param suffix: (str) suffix for human readability
    :param custom_unique_prefix: use a custom prefix. If none, use datetime.
    :return: (str) full filepath for config file
    """

        unique_prefix = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        if custom_unique_prefix:
            unique_prefix = custom_unique_prefix
        ini = unique_prefix + "_" + suffix + ".ini"
        return os.path.join(constants.TEST_CONFIG_PATH, ini)

    @staticmethod
    def enumerate_workload_nodes(
        driver_node_ip_enum, num_workload_nodes, *args
        ):
        """ Populates workload nodes.
    :rtype: List(Node)
    :param driver_node_ip_enum: (int) enum that driver node starts at
    :param num_workload_nodes: (int) number of workload nodes wanted
    :param args the ip_enum of nodes to exclude if CloudLab crashes them
    :return: list of Node objects
    """
        # set of all ip numbers to exclude
        excluded_ip_enums = set()
        for excluded_ip_enum in args:
            excluded_ip_enums.add(excluded_ip_enum)

        start_ip = driver_node_ip_enum
        result = []
        booster = 0
        ending_enum = 0
        for i in range(num_workload_nodes):
            ip_enum = i + start_ip
            while (ip_enum + booster) in excluded_ip_enums:
                booster += 1
            ip_enum += booster
            n = node.Node(ip_enum)
            result.append(n)
            ending_enum = ip_enum

        return result, ending_enum

    @staticmethod
    def enumerate_warm_nodes(num_warm_nodes, start_ip_enum, *args):
        """ Populates warm nodes.

    :param num_warm_nodes: (int)
    :param driver_node_ip_enum: (int)
    :param num_already_enumerated_nodes: (int)
    :param args the ip_enum of nodes to exclude if CloudLab crashes them
    :return: list of Node objects, the first couple of which have regions
    """

        # set of excluded enums
        excluded_ip_enums = set()
        for ip_enum in args:
            excluded_ip_enums.add(ip_enum)

        # regioned nodes
        while start_ip_enum in excluded_ip_enums:
            start_ip_enum += 1
        regioned_nodes = [node.Node(start_ip_enum, "newyork", "/data")]

        second_enum = start_ip_enum + 1
        if num_warm_nodes >= 2:
            while second_enum in excluded_ip_enums:
                second_enum += 1
            regioned_nodes.append(node.Node(second_enum, "london", "/data"))

        third_enum = second_enum + 1
        if num_warm_nodes >= 3:
            while third_enum in excluded_ip_enums:
                third_enum += 1
            regioned_nodes.append(node.Node(third_enum, "tokyo", "/data"))

        # nodes that don't have regions
        remaining_nodes_start_ip = third_enum + 1
        remaining_num_warm_nodes = num_warm_nodes - 3
        remaining_nodes = []
        booster = 0
        for i in range(remaining_num_warm_nodes):
            ip_enum = i + remaining_nodes_start_ip
            while (ip_enum + booster) in excluded_ip_enums:
                booster += 1
            ip_enum += booster
            n = node.Node(ip_enum, "singapore", "/data")
            remaining_nodes.append(n)

        return regioned_nodes + remaining_nodes


def main():
    config_object = ConfigObject()
    config_object.generate_all_config_files()


if __name__ == "__main__":
    main()
