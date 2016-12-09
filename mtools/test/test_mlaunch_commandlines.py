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

    def cmdlist_filter(self, cmdlist):
        return sorted([[x for x in cmd.split() if x.startswith('mongo') or x.startswith('--')] for cmd in cmdlist])

    def cmdlist_print(self):
        cfg, cmdlist = self.read_config()
        print '\n'
        cmdset = self.cmdlist_filter(cmdlist)
        for cmd in cmdset:
            print cmd

    def cmdlist_assert(self, cmdlisttest):
        cfg, cmdlist = self.read_config()
        cmdset = [set(x) for x in self.cmdlist_filter(cmdlist)]
        # cmdlisttest = [set(x) for x in cmdlisttest]
        self.assertEqual(len(cmdlist), len(cmdlisttest), 'number of command lines is {0}, should be {1}'.format(len(cmdlist), len(cmdlisttest)))
        for cmd in zip(cmdset,cmdlisttest):
            self.assertSetEqual(cmd[0], cmd[1])


    def test_single(self):
        ''' mlaunch init --single should start 1 node '''
        self.run_tool('init --single')
        cmdlist = [
            {'mongod', '--dbpath', '--logpath', '--port', '--logappend', '--fork'}
        ]
        self.cmdlist_assert(cmdlist)

    def test_replicaset(self):
        ''' mlaunch init --replicaset should start 3 nodes '''
        self.run_tool("init --replicaset")
        cmdlist = (
            [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork'} ] * 3
        )
        self.cmdlist_assert(cmdlist)
    
    def test_replicaset(self):
        ''' mlaunch init --replicaset --nodes 7 should start 7 nodes '''
        self.run_tool("init --replicaset --nodes 7")
        cmdlist = (
            [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork'} ] * 7
        )
        self.cmdlist_assert(cmdlist)
    
    def test_replicaset(self):
        ''' mlaunch init --replicaset --nodes 6 --arbiter should start 7 nodes '''
        self.run_tool("init --replicaset --nodes 6 --arbiter")
        cmdlist = (
            [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork'} ] * 7
        )
        self.cmdlist_assert(cmdlist)

    def test_sharded_single(self):
        ''' mlaunch init --sharded 2 --single should start 1 config, 2 single shards 1 mongos '''
        self.run_tool("init --sharded 2 --single")
        cmdlist = (
            [ {'mongod', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--configsvr'} ]
          + [ {'mongod', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--shardsvr'} ] * 2
          + [ {'mongos', '--logpath', '--port', '--configdb', '--logappend', '--fork'} ] )
        self.cmdlist_assert(cmdlist)

    def test_sharded_replicaset_sccc_1(self):
        ''' mlaunch init --sharded 2 --replicaset should start 1 config, 2 shards (3 nodes each), 1 mongos '''
        self.run_tool("init --sharded 2 --replicaset")
        cmdlist = (
            [ {'mongod', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--configsvr'} ]
          + [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--shardsvr'} ] * 6
          + [ {'mongos', '--logpath', '--port', '--configdb', '--logappend', '--fork'} ]
        )
        self.cmdlist_assert(cmdlist)

    def test_sharded_replicaset_sccc_2(self):
        ''' mlaunch init --sharded 2 --replicaset --config 2 should start 1 config, 2 shards (3 nodes each), 1 mongos '''
        self.run_tool("init --sharded 2 --replicaset --config 2")
        cmdlist = (
            [ {'mongod', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--configsvr'} ]
          + [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--shardsvr'} ] * 6
          + [ {'mongos', '--logpath', '--port', '--configdb', '--logappend', '--fork'} ]
        )
        self.cmdlist_assert(cmdlist)

    def test_sharded_replicaset_sccc_3(self):
        ''' mlaunch init --sharded 2 --replicaset --config 3 should start 3 config, 2 shards (3 nodes each), 1 mongos '''
        self.run_tool("init --sharded 2 --replicaset --config 3")
        cmdlist = (
            [ {'mongod', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--configsvr'} ] * 3
          + [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--shardsvr'} ] * 6
          + [ {'mongos', '--logpath', '--port', '--configdb', '--logappend', '--fork'} ]
        )
        self.cmdlist_assert(cmdlist)

    def test_sharded_replicaset_sccc_4(self):
        ''' mlaunch init --sharded 2 --replicaset --config 4 should start 3 config, 2 shards (3 nodes each), 1 mongos '''
        self.run_tool("init --sharded 2 --replicaset --config 4")
        cmdlist = (
            [ {'mongod', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--configsvr'} ] * 3
          + [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--shardsvr'} ] * 6
          + [ {'mongos', '--logpath', '--port', '--configdb', '--logappend', '--fork'} ]
        )
        self.cmdlist_assert(cmdlist)

    def test_sharded_replicaset_csrs_1(self):
        ''' mlaunch init --sharded 2 --replicaset --config 1 --csrs should start 1 replicaset config, 2 shards (3 nodes each), 1 mongos '''
        self.run_tool("init --sharded 2 --replicaset --config 1 --csrs")
        cmdlist = (
            [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--configsvr'} ]
          + [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--shardsvr'} ] * 6
          + [ {'mongos', '--logpath', '--port', '--configdb', '--logappend', '--fork'} ]
        )
        self.cmdlist_assert(cmdlist)

    def test_sharded_replicaset_csrs_2(self):
        ''' mlaunch init --sharded 2 --replicaset --config 2 --csrs should start 2 replicaset config, 2 shards (3 nodes each), 1 mongos '''
        self.run_tool("init --sharded 2 --replicaset --config 2 --csrs")
        cmdlist = (
            [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--configsvr'} ] * 2
          + [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--shardsvr'} ] * 6
          + [ {'mongos', '--logpath', '--port', '--configdb', '--logappend', '--fork'} ]
        )
        self.cmdlist_assert(cmdlist)

    def test_sharded_replicaset_csrs_3(self):
        ''' mlaunch init --sharded 2 --replicaset --config 3 --csrs should start 3 replicaset config, 2 shards (3 nodes each), 1 mongos '''
        self.run_tool("init --sharded 2 --replicaset --config 3 --csrs")
        cmdlist = (
            [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--configsvr'} ] * 3
          + [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--shardsvr'} ] * 6
          + [ {'mongos', '--logpath', '--port', '--configdb', '--logappend', '--fork'} ]
        )
        self.cmdlist_assert(cmdlist)

    def test_sharded_replicaset_csrs_4(self):
        ''' mlaunch init --sharded 2 --replicaset --config 4 --csrs should start 4 replicaset config, 2 shards (3 nodes each), 1 mongos '''
        self.run_tool("init --sharded 2 --replicaset --config 4 --csrs")
        cmdlist = (
            [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--configsvr'} ] * 4
          + [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--shardsvr'} ] * 6
          + [ {'mongos', '--logpath', '--port', '--configdb', '--logappend', '--fork'} ]
        )
        self.cmdlist_assert(cmdlist)
    
    def test_sharded_replicaset_csrs_mmapv1(self):
        ''' mlaunch init --sharded 2 --replicaset --csrs --storageEngine mmapv1 should not change config server storage engine '''
        self.run_tool("init --sharded 2 --replicaset --csrs --storageEngine mmapv1")
        cmdlist = (
            [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--configsvr'} ]
          + [ {'mongod', '--replSet', '--dbpath', '--logpath', '--port', '--logappend', '--fork', '--storageEngine', '--shardsvr'} ] * 6
          + [ {'mongos', '--logpath', '--port', '--configdb', '--logappend', '--fork'} ]
        )
        self.cmdlist_assert(cmdlist)


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

