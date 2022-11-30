import shlex
import subprocess


def call(cmd, stdout=subprocess.STDOUT, stderr=subprocess.STDOUT):
  """
  Calls a command in the shell.

  :param cmd: (str)
  :param stdout: set by default to subprocess.PIPE (which is standard stream)
  :param stderr: set by default subprocess.STDOUT (combines with stdout)
  :return: if successful, stdout stream of command.
  """
  print(cmd)
  p = subprocess.run(cmd, stdout=stdout, stderr=stderr, shell=True, check=True, universal_newlines=True)
  return p.stdout


def call_remote(host, cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT):
  """
  Makes a remote call of a command.
  :param host: (str)
  :param cmd:  (str)
  :param stdout:  set by default to subprocess.PIPE (which is the standard stream)
  :param stderr: set by default to subprocess.STDOUT (combines with stdout)
  :return: if successful, stdout stream of command
  """
  cmd = "sudo ssh {0} '{1}'".format(host, cmd)
  return call(cmd, stdout, stderr)


def modify_core(node, core_num, is_enable=False):
  if core_num >= 16:
    raise AssertionError("Cannot specify core larger than 15")
  elif core_num <= 0:
    raise AssertionError("Cannot specify core 0 or less")

  cmd = "echo {0} | tee /sys/devices/system/cpu/cpu{1}/online".format(1 if is_enable else 0, core_num)
  cmd = "sudo ssh {0} '{1}'".format(node, cmd)
  print(cmd)
  return subprocess.Popen(shlex.split(cmd))
  # call_remote(node, cmd)
