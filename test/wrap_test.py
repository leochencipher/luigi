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

import datetime
import unittest

import luigi
import luigi.notifications
from luigi.mock import MockFile
from luigi.util import inherits

luigi.notifications.DEBUG = True
File = MockFile


class A(luigi.Task):

    def output(self):
        return File('/tmp/a.txt')

    def run(self):
        f = self.output().open('w')
        print >>f, 'hello, world'
        f.close()


class B(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return File(self.date.strftime('/tmp/b-%Y-%m-%d.txt'))

    def run(self):
        f = self.output().open('w')
        print >>f, 'goodbye, space'
        f.close()


def XMLWrapper(cls):
    @inherits(cls)
    class XMLWrapperCls(luigi.Task):

        def requires(self):
            return self.clone_parent()

        def run(self):
            f = self.input().open('r')
            g = self.output().open('w')
            print >>g, '<?xml version="1.0" ?>'
            for line in f:
                print >>g, '<dummy-xml>' + line.strip() + '</dummy-xml>'
            g.close()

    return XMLWrapperCls


class AXML(XMLWrapper(A)):

    def output(self):
        return File('/tmp/a.xml')


class BXML(XMLWrapper(B)):

    def output(self):
        return File(self.date.strftime('/tmp/b-%Y-%m-%d.xml'))


class WrapperTest(unittest.TestCase):

    ''' This test illustrates how a task class can wrap another task class by modifying its behavior.

    See instance_wrap_test.py for an example of how instances can wrap each other. '''
    workers = 1

    def setUp(self):
        MockFile.fs.clear()

    def test_a(self):
        luigi.build([AXML()], local_scheduler=True, no_lock=True, workers=self.workers)
        self.assertEqual(MockFile.fs.get_data('/tmp/a.xml'), '<?xml version="1.0" ?>\n<dummy-xml>hello, world</dummy-xml>\n')

    def test_b(self):
        luigi.build([BXML(datetime.date(2012, 1, 1))], local_scheduler=True, no_lock=True, workers=self.workers)
        self.assertEqual(MockFile.fs.get_data('/tmp/b-2012-01-01.xml'), '<?xml version="1.0" ?>\n<dummy-xml>goodbye, space</dummy-xml>\n')


class WrapperWithMultipleWorkersTest(WrapperTest):
    workers = 7


if __name__ == '__main__':
    luigi.run()
