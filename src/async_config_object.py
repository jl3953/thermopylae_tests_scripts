import itertools

import config_io
import config_object as co
import node


class ConfigObject:

    def __init__(self):

        # default YOU MUST POPULATE THESE FIELDS
        self.trials = [i for i in range(1)]
        self.logs_dir = ["test"]
        self.store_dir = ["async_server"]

        # server
        self.server_concurrency = [1]
        self.server_commit_branch = ["async"]
        self.server_node_ip_enum = [2]  # 196.168.1.???
        # self.server_node = [some Node object]

        # client
        self.client_commit_branch = ["async"]
        self.num_workload_nodes = [2]
        self.concurrency = [10] # YOU MUST CALL CLIENT CONCURRENCY "CONCURRENCY"
        self.driver_node_ip_enum = [i + 1 for i in self.server_node_ip_enum]  # 192.168.1.???
        self.duration = [3]  # duration of trial in seconds
        # self.workload_nodes [some Node objects]

        # workload
        self.batch = [1]  # keys per rpc
        self.read_percent = [95]

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

        # populating node information. MAKE SURE THIS PART IS CORRECTLY WRITTEN
        for config_dict in combinations:
            driver_node_ip_enum = config_dict["driver_node_ip_enum"]
            num_workload_nodes = config_dict["num_workload_nodes"]
            workload_nodes, _ = co.ConfigObject.enumerate_workload_nodes(
                driver_node_ip_enum, num_workload_nodes)
            config_dict["workload_nodes"] = [vars(n) for n in workload_nodes]

            server_node_ip_enum = config_dict["server_node_ip_enum"]
            server_node = ConfigObject.create_server_node(server_node_ip_enum)
            config_dict["server_node"] = vars(server_node)

        return combinations

    @staticmethod
    def create_server_node(server_node_ip_enum):
        return node.Node(server_node_ip_enum)

    def generate_all_config_files(self):
        """Generates all configuration files with different combinations of parameters.
        :return:
        """
        ini_fpaths = []
        config_combos = self.generate_config_combinations()
        for config_dict in config_combos:
            ini_fpath = co.ConfigObject.generate_ini_filename(suffix=config_dict["logs_dir"])
            ini_fpaths.append(config_io.write_config_to_file(config_dict, ini_fpath))

        return ini_fpaths


def main():
    config_object = ConfigObject()
    print(config_object.generate_config_combinations())


if __name__ == "__main__":
    main()
