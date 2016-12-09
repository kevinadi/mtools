import os
import json
import sys
import unittest
import inspect
import shutil

from mtools.mlaunch.mlaunch import MLaunchTool, shutdown_host
from nose.tools import *
from nose.plugins.attrib import attr
from nose.plugins.skip import Skip, SkipTest
from pprint import pprint


# cmdline stored in self.startup_info

class TestMLaunch(unittest.TestCase):

    def setUp(self):
        # self.port = 33333
        self.base_dir = 'data_test_mlaunch'
        self.tool = MLaunchTool(test=True)

    def tearDown(self):
        self.tool = None
        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)

    def run_tool(self, arg_str):
        """ wrapper to call self.tool.run() with or without auth """
        # name data directory according to test method name
        caller = inspect.stack()[1][3]
        self.data_dir = os.path.join(self.base_dir, caller)

        # add data directory to arguments for all commands
        arg_str += ' --dir %s' % self.data_dir

        with self.assertRaises(SystemExit):
            self.tool.run(arg_str)

    def read_config(self):
        fp = open(self.data_dir + '/.mlaunch_startup', 'r')
        cfg = json.load(fp)
        cmd = [cfg['startup_info'][x] for x in cfg['startup_info'].keys()]
        return cfg, cmd

    def assert_cmd(self, should_include, should_exclude):
        elem1 = lambda x: x[0]
        cfg, cmdlist = self.read_config()

        for cmd in cmdlist:
            cmdset = set(cmd.split())
            
            result = []
            for incl in should_include:
                result.append((incl.issubset(cmdset), '{0} should be present in "{1}"'.format(incl, cmd)))
            resultbool = [x[0] for x in result]
            self.assertTrue(any(resultbool), '{0}'.format([x[1] for x in result if not x[0]]))

            result = []
            for excl in should_exclude:
                result.append((not excl.issubset(cmdset), '{0} should not be present in "{1}"'.format(excl, cmd)))
            resultbool = [x[0] for x in result]
            self.assertTrue(any(resultbool), '{0}'.format([x[1] for x in result if not x[0]]))


    # @unittest.skip('')
    def test_single(self):
        self.run_tool('init --single')
        should_include = [
            {'mongod', '--dbpath', '--port', '27017', '--logpath', '--fork'}
        ]
        should_exclude = [
            {'mongod', '--replSet'}
        ]
        self.assert_cmd(should_include, should_exclude)

    # @unittest.skip('')
    def test_replicaset(self):
        self.run_tool("init --replicaset")
        should_include = [
            {'mongod', '--replSet', '--port'}
        ]
        should_exclude = [
            {'mongos'}
        ]
        self.assert_cmd(should_include, should_exclude)

    # @unittest.skip('')
    def test_sharded_single(self):
        self.run_tool("init --sharded 2 --single")
        should_include = [
            {'mongod', '--shardsvr'},
            {'mongod', '--configsvr'},
            {'mongos', '--port', '27017', '--configdb'}
        ]
        should_exclude = [
            {'mongod', '--replSet', '--shardsvr'}
        ]
        self.assert_cmd(should_include, should_exclude)


    def test_sharded_replicaset(self):
        self.run_tool("init --sharded 2 --replicaset")
        should_include = [
            {'mongod', '--replSet', '--shardsvr'},
            {'mongod', '--configsvr'},
            {'mongos', '--port', '27017', '--configdb'}
        ]
        should_exclude = [
            {None}
        ]
        self.assert_cmd(should_include, should_exclude)


    def test_sharded_replicaset_sccc(self):
        self.run_tool("init --sharded 2 --replicaset --config 3")
        should_include = [
            {'mongod', '--replSet', '--shardsvr'},
            {'mongod', '--configsvr'},
            {'mongos', '--port', '27017', '--configdb'}
        ]
        should_exclude = [
            {None}
        ]
        self.assert_cmd(should_include, should_exclude)


    def test_sharded_replicaset_csrs(self):
        self.run_tool("init --sharded 2 --replicaset --config 3 --csrs")
        should_include = [
            {'mongod', '--replSet', '--shardsvr'},
            {'mongod', '--replSet', '--configsvr'},
            {'mongos', '--port', '27017', '--configdb'}
        ]
        should_exclude = [
            {None}
        ]
        self.assert_cmd(should_include, should_exclude)


    # self.run_tool("init --sharded 2 --single")

    # self.run_tool("init --sharded 2 --replicaset --config 3 --mongos 2 --verbose")

    # self.run_tool("init --sharded tic tac toe --replicaset")

    # self.run_tool("init --single -v")

    # self.run_tool("init --sharded 2 --single --config 1 --mongos 1")

    # self.run_tool("init --sharded 2 --single --config 1 --mongos 2")

    # self.run_tool("init --replicaset --nodes 6 --arbiter")

    # self.run_tool("init --replicaset --nodes 7")

    # self.run_tool("init --sharded 2 --replicaset --config 3 --mongos 3")

    # self.run_tool("init --sharded 2 --single --mongos 0 --auth")

    # self.run_tool("init --single --auth --username corben --password fitzroy --auth-roles dbAdminAnyDatabase readWriteAnyDatabase userAdminAnyDatabase")

    # self.run_tool("init --sharded foo --single")

    # self.run_tool("init --sharded 2 --replicaset --mongos 2")

    # self.run_tool("init --single --binarypath %s" % path)

    # self.run_tool("init --single --arbiter")

    # self.run_tool("init --sharded 1 --single --oplogSize 19 --verbose")

# New

