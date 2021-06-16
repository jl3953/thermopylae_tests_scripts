import os

ROOT = "/root"
COCKROACHDB_DIR = os.path.join(ROOT, "go", "src", "github.com", "cockroachdb", "cockroach")
TEST_PATH = os.path.join(ROOT, "thermopylae_tests")
TEST_CONFIG_PATH = os.path.join(TEST_PATH, "config")
TEST_SRC_PATH = os.path.join(TEST_PATH, "src")
SCRATCH_DIR = os.path.join(TEST_PATH, "scratch")
CONFIG_FPATH_KEY = "config_fpath"

GRPC_GO_DIR = os.path.join(ROOT, "grpc-go")
