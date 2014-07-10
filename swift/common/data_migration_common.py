# Copyright 2014 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


class DataMigrationDriver(object):
    """
    A common base class for all drivers.
    Method get_object should be implemented by each
    derived class
    """

    def get_object(self, object_name):
        """
        :param object_name: the object name
        """
        pass

    def finalize(self):
        pass


class DataMigrationDriverError(Exception):

    def __init__(self, msg):
        Exception.__init__(self, msg)
