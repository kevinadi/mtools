import shutil
import os
import unittest
import sys
import subprocess
import pymongo
import psutil

class MlaunchTest(unittest.TestCase):

    DOTEST = [
        'test_standalone_default',
        'test_replset_default',
        'test_storageengine_mmapv1',
        'test_wiredtigercachesizegb_5',
        'test_replset',
        'test_replset_mmapv1',
        'test_replset_single',
        'test_replset_single_mmapv1',
        'test_replset_arbiter',
        'test_replset_mmapv1_arbiter',
        'test_sharded',
        'test_sharded_replicaset',
        'test_sharded_mmapv1',
        'test_sharded_config_3',
        'test_sharded_config_3_mmapv1',
        'test_sharded_replset_config_3_mmapv1',
        'test_replset_localhost',
        'test_sharded_replicaset_localhost',
        'test_standalone_ssl',
        'test_replset_ssl',
        'test_sharded_ssl',
        'test_sharded_replicaset_ssl'
    ]


    @staticmethod
    def check_cmdline(parameters, proc):
        ''' return the process' command line '''
        return [x for x in proc.cmdline() if x in parameters]

    @staticmethod
    def get_processes(process):
        ''' get the process '''
        procs = [p for p in psutil.process_iter() if p.name() == process]
        if len(procs) <= 0:
            return []
        return procs

    @staticmethod
    def terminate_process(process):
        ''' spin until process is terminated '''
        procs = [p for p in psutil.process_iter() if p.name() == process]
        for proc in procs:
            proc.terminate()
        psutil.wait_procs(procs)
        return True

    def assert_matching_sets(self, subset_array, superset_array):
        ''' test subset array vs. superset array '''
        matches = []
        for this in [set(x) for x in subset_array]:
            for that in [set(x) for x in superset_array]:
                if this.issubset(that):
                    matches.append((this, that))
                    self.assertSetEqual(this, that.intersection(this))
                    self.assertTrue(that.issuperset(this))
        self.assertEqual(len(subset_array), len(superset_array))
        self.assertEqual(len(matches), len(superset_array))
        #######
        # print sorted(superset_array)
        # for expected, observed in zip([set(x) for x in sorted(subset_array)], [set(x) for x in sorted(superset_array)]):
        #     print expected, '>------>', observed
        #     self.assertSetEqual(expected, expected.intersection(observed))


    @classmethod
    def setUpClass(cls):
        cls.MONGOD = 'mongod'
        cls.MONGOS = 'mongos'
        if os.name == 'nt':
            cls.MONGOD = 'mongod.exe'
            cls.MONGOS = 'mongos.exe'

    def setUp(self):
        MlaunchTest.terminate_process(self.MONGOD)
        MlaunchTest.terminate_process(self.MONGOS)
        try:
            shutil.rmtree('data')
        except OSError:
            pass

    def tearDown(self):
        self.setUp()


    def run_standalone(self, cmdline):
        ''' run mlaunch and check its exit status '''
        process = subprocess.check_call(cmdline.split())
        self.assertEqual(process, 0)
        proc = MlaunchTest.get_processes(self.MONGOD)
        running_cmdline = proc[0].cmdline()
        return running_cmdline

    def run_replset(self, cmdline, node_count, arbiter_count=0, ssl=False):
        ''' run replset '''
        process = subprocess.check_call(cmdline.split())
        self.assertEqual(process, 0)
        running_cmdline = []
        for observed in MlaunchTest.get_processes(self.MONGOD):
            running_cmdline.append(observed.cmdline())
        # check replset config
        self.check_replset_config(node_count, arbiter_count, ssl)
        return running_cmdline

    def check_replset_config(self, node_count, arbiter_count, ssl):
        ''' use pymongo to check replset config '''
        # check replset config
        conn = None
        if ssl:
            conn = pymongo.MongoClient('mongodb://localhost:27017/?replicaset=replset', ssl=True, ssl_ca_certs='ca.crt', ssl_certfile='client.pem')
        else:
            conn = pymongo.MongoClient('mongodb://localhost:27017/?replicaset=replset')
        conf = conn.admin.command('replSetGetConfig', 1)
        members = conf.get('config').get('members')
        self.assertEqual(len(members), node_count)
        # check for arbiters
        arbiters = [x for x in members if x['arbiterOnly']]
        self.assertEqual(len(arbiters), arbiter_count)
        # close the connection
        conn.close()

    def run_sharded(self, cmdline):
        ''' run sharded cluster '''
        process = subprocess.check_call(cmdline.split())
        self.assertEqual(process, 0)
        running_cmdline = []
        for observed in MlaunchTest.get_processes(self.MONGOD):
            running_cmdline.append(observed.cmdline())
        for observed in MlaunchTest.get_processes(self.MONGOS):
            running_cmdline.append(observed.cmdline())
        return running_cmdline



    @unittest.skipUnless('test_standalone_default' in DOTEST, 'Skipped')
    def test_standalone_default(self):
        ''' mlaunch init --single '''
        cmdline = 'mlaunch init --single'
        expected = ['mongod', '--wiredTigerCacheSizeGB', '1']
        observed = self.run_standalone(cmdline)
        self.assertSetEqual(set(expected), set(observed).intersection(expected))
        self.assertTrue(set(observed).issuperset(expected))

    @unittest.skipUnless('test_storageengine_mmapv1' in DOTEST, 'Skipped')
    def test_storageengine_mmapv1(self):
        ''' mlaunch init --single --storageEngine mmapv1 '''
        cmdline = 'mlaunch init --single --storageEngine mmapv1'
        expected = ['mongod', '--storageEngine', 'mmapv1']
        observed = self.run_standalone(cmdline)
        self.assertSetEqual(set(expected), set(observed).intersection(expected))
        self.assertTrue(set(observed).issuperset(expected))

    @unittest.skipUnless('test_wiredtigercachesizegb_5' in DOTEST, 'Skipped')
    def test_wiredtigercachesizegb_1(self):
        ''' mlaunch init --single --wiredTigerCacheSizeGB 5 '''
        cmdline = 'mlaunch init --single --wiredTigerCacheSizeGB 5'
        expected = ['mongod', '--wiredTigerCacheSizeGB', '5']
        observed = self.run_standalone(cmdline)
        self.assertSetEqual(set(expected), set(observed).intersection(expected))
        self.assertTrue(set(observed).issuperset(expected))

    @unittest.skipUnless('test_replset_default' in DOTEST, 'Skipped')
    def test_replset_default(self):
        ''' mlaunch init --replicaset '''
        cmdline = 'mlaunch init --replicaset'
        expected_arr = [
            ['mongod', '--replSet', 'replset', '--port', '27017', '--wiredTigerCacheSizeGB', '1'],
            ['mongod', '--replSet', 'replset', '--port', '27018', '--wiredTigerCacheSizeGB', '1'],
            ['mongod', '--replSet', 'replset', '--port', '27019', '--wiredTigerCacheSizeGB', '1']
        ]
        observed_arr = self.run_replset(cmdline, node_count=len(expected_arr), arbiter_count=0)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_replset' in DOTEST, 'Skipped')
    def test_replset(self):
        ''' mlaunch init --replicaset '''
        cmdline = 'mlaunch init --replicaset'
        expected_arr = [
            ['mongod', '--replSet', 'replset', '--port', '27017'],
            ['mongod', '--replSet', 'replset', '--port', '27018'],
            ['mongod', '--replSet', 'replset', '--port', '27019']
        ]
        observed_arr = self.run_replset(cmdline, node_count=len(expected_arr), arbiter_count=0)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_replset_localhost' in DOTEST, 'Skipped')
    def test_replset_localhost(self):
        ''' mlaunch init --replicaset '''
        cmdline = 'mlaunch init --replicaset'
        expected_arr = [
            ['mongod', '--replSet', 'replset', '--port', '27017'],
            ['mongod', '--replSet', 'replset', '--port', '27018'],
            ['mongod', '--replSet', 'replset', '--port', '27019']
        ]
        observed_arr = self.run_replset(cmdline, node_count=len(expected_arr), arbiter_count=0)
        self.assert_matching_sets(expected_arr, observed_arr)
        # check replset config
        conn = pymongo.MongoClient('mongodb://localhost:27017/?replicaset=replset')
        conf = conn.admin.command('replSetGetConfig', 1)
        members = [m.get('host') for m in conf.get('config').get('members')]
        self.assertTrue(all(m.startswith('localhost') for m in members))

    @unittest.skipUnless('test_replset_mmapv1' in DOTEST, 'Skipped')
    def test_replset_mmapv1(self):
        ''' mlaunch init --replicaset --storageEngine mmapv1 '''
        cmdline = 'mlaunch init --replicaset --storageEngine mmapv1'
        expected_arr = [
            ['mongod', '--replSet', 'replset', '--port', '27017', '--storageEngine', 'mmapv1'],
            ['mongod', '--replSet', 'replset', '--port', '27018', '--storageEngine', 'mmapv1'],
            ['mongod', '--replSet', 'replset', '--port', '27019', '--storageEngine', 'mmapv1']
        ]
        observed_arr = self.run_replset(cmdline, node_count=len(expected_arr), arbiter_count=0)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_replset_single' in DOTEST, 'Skipped')
    def test_replset_single(self):
        ''' mlaunch init --replicaset --nodes 1 '''
        cmdline = 'mlaunch init --replicaset --nodes 1'
        expected_arr = [
            ['mongod', '--replSet', 'replset', '--port', '27017']
        ]
        observed_arr = self.run_replset(cmdline, node_count=len(expected_arr), arbiter_count=0)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_replset_single_mmapv1' in DOTEST, 'Skipped')
    def test_replset_single_mmapv1(self):
        ''' mlaunch init --replicaset --nodes 1 --storageEngine mmapv1 '''
        cmdline = 'mlaunch init --replicaset --nodes 1 --storageEngine mmapv1'
        expected_arr = [
            ['mongod', '--replSet', 'replset', '--port', '27017', '--storageEngine', 'mmapv1']
        ]
        observed_arr = self.run_replset(cmdline, node_count=len(expected_arr), arbiter_count=0)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_replset_arbiter' in DOTEST, 'Skipped')
    def test_replset_arbiter(self):
        ''' mlaunch init --replicaset --nodes 2 --arbiter '''
        cmdline = 'mlaunch init --replicaset --nodes 2 --arbiter'
        expected_arr = [
            ['mongod', '--replSet', 'replset', '--port', '27017'],
            ['mongod', '--replSet', 'replset', '--port', '27018'],
            ['mongod', '--replSet', 'replset', '--port', '27019']
        ]
        observed_arr = self.run_replset(cmdline, node_count=len(expected_arr), arbiter_count=1)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_replset_mmapv1_arbiter' in DOTEST, 'Skipped')
    def test_replset_mmapv1_arbiter(self):
        ''' mlaunch init --replicaset --nodes 2 --arbiter --storageEngine mmapv1 '''
        cmdline = 'mlaunch init --replicaset --nodes 2 --arbiter --storageEngine mmapv1'
        expected_arr = [
            ['mongod', '--replSet', 'replset', '--port', '27017', '--storageEngine', 'mmapv1'],
            ['mongod', '--replSet', 'replset', '--port', '27018', '--storageEngine', 'mmapv1'],
            ['mongod', '--replSet', 'replset', '--port', '27019', '--storageEngine', 'mmapv1']
        ]
        observed_arr = self.run_replset(cmdline, node_count=len(expected_arr), arbiter_count=1)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_sharded' in DOTEST, 'Skipped')
    def test_sharded(self):
        ''' mlaunch init --sharded 2 --single '''
        cmdline = 'mlaunch init --sharded 2 --single'
        expected_arr = [
            ['mongod', '--shardsvr', '27018'],
            ['mongod', '--shardsvr', '27019'],
            ['mongod', '--configsvr', '27020'],
            ['mongos', '--port', '27017']
        ]
        observed_arr = self.run_sharded(cmdline)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_sharded_replicaset' in DOTEST, 'Skipped')
    def test_sharded_replicaset(self):
        ''' mlaunch init --sharded 2 --replicaset '''
        cmdline = 'mlaunch init --sharded 2 --replicaset'
        expected_arr = [
            ['mongod', '--shardsvr', '27018'],
            ['mongod', '--shardsvr', '27019'],
            ['mongod', '--shardsvr', '27020'],
            ['mongod', '--shardsvr', '27021'],
            ['mongod', '--shardsvr', '27022'],
            ['mongod', '--shardsvr', '27023'],
            ['mongod', '--configsvr', '27024'],
            ['mongos', '--port', '27017']
        ]
        observed_arr = self.run_sharded(cmdline)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_sharded_replicaset_localhost' in DOTEST, 'Skipped')
    def test_sharded_replicaset_localhost(self):
        ''' mlaunch init --sharded 2 --replicaset (localhost) '''
        cmdline = 'mlaunch init --sharded 2 --replicaset'
        expected_arr = [
            ['mongod', '--shardsvr', '27018'],
            ['mongod', '--shardsvr', '27019'],
            ['mongod', '--shardsvr', '27020'],
            ['mongod', '--shardsvr', '27021'],
            ['mongod', '--shardsvr', '27022'],
            ['mongod', '--shardsvr', '27023'],
            ['mongod', '--configsvr', '27024'],
            ['mongos', '--port', '27017']
        ]
        observed_arr = self.run_sharded(cmdline)
        self.assert_matching_sets(expected_arr, observed_arr)
        # check replset config
        conn = pymongo.MongoClient('mongodb://localhost:27017/config')
        shards = [s.get('host') for s in conn.config.shards.find({}, {'_id': 0, 'host': 1})]
        print >>sys.stderr, shards
        self.assertEqual(shards,
                         [u'shard01/localhost:27018,localhost:27019,localhost:27020',
                          u'shard02/localhost:27021,localhost:27022,localhost:27023'])

    @unittest.skipUnless('test_sharded_mmapv1' in DOTEST, 'Skipped')
    def test_sharded_mmapv1(self):
        ''' mlaunch init --sharded 2 --single --storageEngine mmapv1 '''
        cmdline = 'mlaunch init --sharded 2 --single --storageEngine mmapv1'
        expected_arr = [
            ['mongod', '--shardsvr', '--storageEngine', 'mmapv1', '27018'],
            ['mongod', '--shardsvr', '--storageEngine', 'mmapv1', '27019'],
            ['mongod', '--configsvr', '27020'],
            ['mongos', '--port', '27017']
        ]
        observed_arr = self.run_sharded(cmdline)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_sharded_config_3' in DOTEST, 'Skipped')
    def test_sharded_config_3(self):
        ''' mlaunch init --sharded 2 --single --config 3 '''
        cmdline = 'mlaunch init --sharded 2 --single --config 3'
        expected_arr = [
            ['mongod', '--shardsvr', '27018'],
            ['mongod', '--shardsvr', '27019'],
            ['mongod', '--configsvr', '27020'],
            ['mongod', '--configsvr', '27021'],
            ['mongod', '--configsvr', '27022'],
            ['mongos', '--port', '27017']
        ]
        observed_arr = self.run_sharded(cmdline)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_sharded_config_3_mmapv1' in DOTEST, 'Skipped')
    def test_sharded_config_3_mmapv1(self):
        ''' mlaunch init --sharded 2 --single --config 3 --storageEngine mmapv1 '''
        cmdline = 'mlaunch init --sharded 2 --single --config 3 --storageEngine mmapv1'
        expected_arr = [
            ['mongod', '--shardsvr', '--storageEngine', 'mmapv1', '27018'],
            ['mongod', '--shardsvr', '--storageEngine', 'mmapv1', '27019'],
            ['mongod', '--configsvr', '27020'],
            ['mongod', '--configsvr', '27021'],
            ['mongod', '--configsvr', '27022'],
            ['mongos', '--port', '27017']
        ]
        observed_arr = self.run_sharded(cmdline)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_sharded_replset_config_3_mmapv1' in DOTEST, 'Skipped')
    def test_sharded_replset_config_3_mmapv1(self):
        ''' mlaunch init --sharded 2 --replicaset --config 3 --storageEngine mmapv1 '''
        cmdline = 'mlaunch init --sharded 2 --replicaset --config 3 --storageEngine mmapv1'
        expected_arr = [
            ['mongod', '--shardsvr', '--storageEngine', 'mmapv1', '27018'],
            ['mongod', '--shardsvr', '--storageEngine', 'mmapv1', '27019'],
            ['mongod', '--shardsvr', '--storageEngine', 'mmapv1', '27020'],
            ['mongod', '--shardsvr', '--storageEngine', 'mmapv1', '27021'],
            ['mongod', '--shardsvr', '--storageEngine', 'mmapv1', '27022'],
            ['mongod', '--shardsvr', '--storageEngine', 'mmapv1', '27023'],
            ['mongod', '--configsvr', '27024'],
            ['mongod', '--configsvr', '27025'],
            ['mongod', '--configsvr', '27026'],
            ['mongos', '--port', '27017']
        ]
        observed_arr = self.run_sharded(cmdline)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_standalone_ssl' in DOTEST, 'Skipped')
    @unittest.skipUnless(os.path.isfile('ca.crt') and os.path.isfile('server.pem') and os.path.isfile('client.pem'), 'No SSL certs found')
    def test_standalone_ssl(self):
        ''' mlaunch init --single --sslMode requireSSL --sslCAFile ca.crt --sslPEMKeyFile server.pem --sslClientCertificate client.pem '''
        cmdline = 'mlaunch init --single --sslMode requireSSL --sslCAFile ca.crt --sslPEMKeyFile server.pem --sslClientCertificate client.pem'
        expected = ['mongod', '--wiredTigerCacheSizeGB', '1', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem']
        observed = self.run_standalone(cmdline)
        self.assertSetEqual(set(expected), set(observed).intersection(expected))
        self.assertTrue(set(observed).issuperset(expected))

    @unittest.skipUnless('test_replset_ssl' in DOTEST, 'Skipped')
    @unittest.skipUnless(os.path.isfile('ca.crt') and os.path.isfile('server.pem') and os.path.isfile('client.pem'), 'No SSL certs found')
    def test_replset_ssl(self):
        ''' mlaunch init --replicaset --sslMode requireSSL --sslCAFile ca.crt --sslPEMKeyFile server.pem --sslClientCertificate client.pem '''
        cmdline = 'mlaunch init --replicaset --sslMode requireSSL --sslCAFile ca.crt --sslPEMKeyFile server.pem --sslClientCertificate client.pem'
        expected_arr = [
            ['mongod', '--replSet', 'replset', '--port', '27017', '--wiredTigerCacheSizeGB', '1', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem'],
            ['mongod', '--replSet', 'replset', '--port', '27018', '--wiredTigerCacheSizeGB', '1', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem'],
            ['mongod', '--replSet', 'replset', '--port', '27019', '--wiredTigerCacheSizeGB', '1', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem']
        ]
        observed_arr = self.run_replset(cmdline, node_count=len(expected_arr), arbiter_count=0, ssl=True)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_sharded_ssl' in DOTEST, 'Skipped')
    @unittest.skipUnless(os.path.isfile('ca.crt') and os.path.isfile('server.pem') and os.path.isfile('client.pem'), 'No SSL certs found')
    def test_sharded_ssl(self):
        ''' mlaunch init --sharded 2 --single --sslMode requireSSL --sslCAFile ca.crt --sslPEMKeyFile server.pem --sslClientCertificate client.pem '''
        cmdline = 'mlaunch init --sharded 2 --single --sslMode requireSSL --sslCAFile ca.crt --sslPEMKeyFile server.pem --sslClientCertificate client.pem'
        expected_arr = [
            ['mongod', '--shardsvr', '27018', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem'],
            ['mongod', '--shardsvr', '27019', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem'],
            ['mongod', '--configsvr', '27020', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem'],
            ['mongos', '--port', '27017', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem']
        ]
        observed_arr = self.run_sharded(cmdline)
        self.assert_matching_sets(expected_arr, observed_arr)

    @unittest.skipUnless('test_sharded_replicaset_ssl' in DOTEST, 'Skipped')
    @unittest.skipUnless(os.path.isfile('ca.crt') and os.path.isfile('server.pem') and os.path.isfile('client.pem'), 'No SSL certs found')
    def test_sharded_replicaset_ssl(self):
        ''' mlaunch init --sharded 2 --replicaset --sslMode requireSSL --sslCAFile ca.crt --sslPEMKeyFile server.pem --sslClientCertificate client.pem'''
        cmdline = 'mlaunch init --sharded 2 --replicaset --sslMode requireSSL --sslCAFile ca.crt --sslPEMKeyFile server.pem --sslClientCertificate client.pem'
        expected_arr = [
            ['mongod', '--shardsvr', '27018', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem'],
            ['mongod', '--shardsvr', '27019', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem'],
            ['mongod', '--shardsvr', '27020', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem'],
            ['mongod', '--shardsvr', '27021', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem'],
            ['mongod', '--shardsvr', '27022', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem'],
            ['mongod', '--shardsvr', '27023', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem'],
            ['mongod', '--configsvr', '27024', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem'],
            ['mongos', '--port', '27017', '--sslCAFile', 'ca.crt', '--sslMode', 'requireSSL', '--sslPEMKeyFile', 'server.pem']
        ]
        observed_arr = self.run_sharded(cmdline)
        self.assert_matching_sets(expected_arr, observed_arr)