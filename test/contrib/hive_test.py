# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from unittest import TestCase

import luigi
from luigi.contrib import hive
import mock


class MyHiveTask(hive.HiveQueryTask):
    param = luigi.Parameter()
    
    def query(self):
        return 'banana banana %s' % self.param


class TestHiveTask(TestCase):

    @mock.patch('luigi.hadoop.run_and_track_hadoop_job')
    def test_run(self, run_and_track_hadoop_job):
        success = luigi.run(['MyHiveTask', '--param', 'foo', '--local-scheduler', '--no-lock'])
        self.assertTrue(success)
        self.assertEquals('hive', run_and_track_hadoop_job.call_args[0][0][0])
