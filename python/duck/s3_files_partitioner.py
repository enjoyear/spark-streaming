import time

import boto3

s3 = boto3.client('s3', aws_access_key_id="a",
                  aws_secret_access_key="b",
                  aws_session_token="c")


class TrieNode:
    def __init__(self, value=None):
        self.value = value
        self.children = {}
        self.is_file = False
        self.total_nodes_inclusive = 0


class InvalidTrieStructureException(Exception):
    pass


class Trie:
    def __init__(self, prefixes):
        self.root = TrieNode()
        self.build_trie(prefixes)

    def build_trie(self, prefixes):
        for p in prefixes:
            self.insert(p)

    def insert(self, prefix):
        node = self.root
        for char in prefix:
            if node.is_file:
                raise InvalidTrieStructureException(
                    f"On the path {prefix}, find a non-leaf node ending in '{node.value}' marked as a file.")
            node.total_nodes_inclusive += 1
            if char not in node.children:
                node.children[char] = TrieNode(char)
            node = node.children[char]
        node.total_nodes_inclusive += 1  # Increment for the last node
        node.is_file = True

    @staticmethod
    def group_prefixes_by_limit(trie, group_limit):
        return Trie._collect_common_prefixes(trie.root, '', group_limit)

    @staticmethod
    def _collect_common_prefixes(node, prefix, group_limit):
        # Base case: if the number of nodes is within the group limit, return the prefix and all file paths
        if node.total_nodes_inclusive <= group_limit:
            return {prefix: Trie._get_all_file_paths(node, prefix)}

        common_prefixes = {}
        for char, child in node.children.items():
            child_prefixes = Trie._collect_common_prefixes(child, prefix + char, group_limit)
            common_prefixes.update(child_prefixes)
        return common_prefixes

    @staticmethod
    def _get_all_file_paths(node, prefix):
        file_paths = []
        if node.is_file:
            file_paths.append(prefix)
        for char, child in node.children.items():
            file_paths.extend(Trie._get_all_file_paths(child, prefix + char))
        return file_paths


def list_s3_prefixes(bucket, prefix):
    paginator = s3.get_paginator('list_objects_v2')

    prefixes = []
    response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in response_iterator:
        for obj in page.get('Contents', []):
            key = obj['Key']
            prefixes.append(key)

    return prefixes


def get_s3_prefix_groups(bucket, prefix, group_limit):
    time_start = time.time()
    prefixes = list_s3_prefixes(bucket, prefix)
    time_prefix = time.time()
    print("Seconds taken to fetch all prefixes: ", time_prefix - time_start)

    trie = Trie(prefixes)
    prefix_groups = Trie.group_prefixes_by_limit(trie, group_limit)
    time_groups = time.time()
    print("Seconds taken to group prefixes: ", time_groups - time_prefix)
    return prefix_groups


def unit_tests():
    trie_empty = Trie([])
    assert trie_empty.root.total_nodes_inclusive == 0
    assert trie_empty.root.value is None

    trie_a = Trie(["a"])
    assert trie_a.root.total_nodes_inclusive == 1
    assert trie_a.root.value is None
    assert trie_a.root.is_file is False
    assert trie_a.root.children['a'].total_nodes_inclusive == 1
    assert trie_a.root.children['a'].value == 'a'
    assert trie_a.root.children['a'].is_file is True

    prefixes1 = ["a/b/1", "a/b/2", "a/c/1", "a/c/2"]
    groups1_1 = Trie.group_prefixes_by_limit(Trie(prefixes1), 1)
    assert groups1_1 == {'a/b/1': ['a/b/1'], 'a/b/2': ['a/b/2'], 'a/c/1': ['a/c/1'], 'a/c/2': ['a/c/2']}

    groups1_2 = Trie.group_prefixes_by_limit(Trie(prefixes1), 2)
    assert groups1_2 == {'a/b': ['a/b/1', 'a/b/2'], 'a/c': ['a/c/1', 'a/c/2']}

    groups1_3 = Trie.group_prefixes_by_limit(Trie(prefixes1), 3)
    assert groups1_3 == {'a/b': ['a/b/1', 'a/b/2'], 'a/c': ['a/c/1', 'a/c/2']}

    groups1_4 = Trie.group_prefixes_by_limit(Trie(prefixes1), 4)
    assert groups1_4 == {'': ["a/b/1", "a/b/2", "a/c/1", "a/c/2"]}

    prefixes2 = ["a/b", "a/b/1", "a/b/2", "a/c/1", "a/c/2"]
    empty = None
    try:
        empty = Trie.group_prefixes_by_limit(Trie(prefixes2), 1)
    except InvalidTrieStructureException as e:
        # make sure the InvalidTrieStructureException is thrown
        assert str(e) == "On the path a/b/1, find a non-leaf node ending in 'b' marked as a file."
    assert empty is None


if __name__ == '__main__':
    unit_tests()

    # Example usage:
    bucket = 'bucket_name'
    prefix = 'prefix_name'
    group_limit = 5000
    result = get_s3_prefix_groups(bucket, prefix, group_limit)

    total_file_paths = 0
    for prefix, file_paths in result.items():
        total_file_paths += len(file_paths)
        print(f"Prefix: {prefix}, File Paths Count: {len(file_paths)}")
    print(f"Total File Paths: {total_file_paths}")
