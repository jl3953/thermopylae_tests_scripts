import io
import os
import subprocess
import unittest

import system_utils


class SystemUtilsTest(unittest.TestCase):

  def setUp(self) -> None:
    self.mock_stdout = io.StringIO()
    os.stdout = self.mock_stdout

    self.mock_stderr = io.StringIO()
    os.stderr = self.mock_stderr

  def test_basic_stdout(self):
    cmd = "echo 'testing stdout'"
    expected_output = "testing stdout\n"

    out = system_utils.call(cmd)
    self.assertEqual(expected_output, out)

    with open("stdout.txt", "w") as test_out:
      system_utils.call(cmd, stdout=test_out)

    with open("stdout.txt", "r") as test_in:
      lines = test_in.read()
      self.assertEqual(expected_output, lines)

  def test_basic_stderr(self):
    cmd = "echo 'testing stderr' 1>&2"
    expected_output = "testing stderr\n"

    out = system_utils.call(cmd)
    self.assertEqual(expected_output, out)

    with open("stdout.txt", "w") as test_out:
      system_utils.call(cmd, stdout=test_out)

    with open("stdout.txt", "r") as test_in:
      self.assertEqual(expected_output, test_in.read())

  def test_separate_stdout_stderr(self):
    cmd = "echo 'testing stdout'; echo 'testing stderr' 1>&2"

    with open("stderr.txt", "w") as test_err:
      out = system_utils.call(cmd, stderr=test_err)
      self.assertEqual("testing stdout\n", out)

    with open("stderr.txt", "r") as test_in:
      self.assertEqual("testing stderr\n", test_in.read())

  def test_bad_return_code(self):
    cmd = "./intentionally_create_bad_return_code.sh"
    self.assertRaises(subprocess.CalledProcessError, system_utils.call, cmd)

  # def test_remote_stdout(self):
  #   cmd = "echo testing remote stdout"
  #
  #   out = system_utils.call_remote("localhost", cmd)
  #   self.assertEqual("testing remote stdout", out.strip())


if __name__ == '__main__':
  unittest.main()
