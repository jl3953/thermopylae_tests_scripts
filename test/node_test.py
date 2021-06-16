import unittest

from node import Node


class NodeTest(unittest.TestCase):

  def test_list_of_nodes(self):
    nodes = []
    for ip_enum in range(1, 4):
      nodes.append(Node(ip_enum))

    self.assertEqual(str(nodes[0]), str({"ip": "192.168.1.1"}))
    self.assertEqual(str(nodes[1]), str({"ip": "192.168.1.2"}))
    self.assertEqual(str(nodes[2]), str({"ip": "192.168.1.3"}))

  def test_list_of_regioned_nodes(self):
    node = Node(1, "newyork", "/data")

    self.assertEqual(str(node), str({
      "ip": "192.168.1.1",
      "region": "newyork",
      "store": "/data"
    }))

  def test_eq(self):
    self.assertEqual(Node(1), Node(1))
    self.assertNotEqual(Node(1), Node(1, "newyork", "/data"))
    self.assertNotEqual(Node(1), Node(2))
    self.assertNotEqual(Node(1, "newyork", "/data"), Node(1, "london", "/data"))


if __name__ == '__main__':
  unittest.main()
