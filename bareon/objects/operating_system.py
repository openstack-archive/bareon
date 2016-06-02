# Copyright 2015 Mirantis, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from bareon.objects import users


class OperatingSystem(object):
    def __init__(self, repos, packages, major='unknown', minor='unknown',
                 proxies=None, user_accounts=None):
        self.repos = repos
        self.packages = packages
        self.major = major
        self.minor = minor
        self.proxies = proxies
        self.user_accounts = user_accounts or []

    def add_user_account(self, **kwargs):
        self.user_accounts.append(users.User(**kwargs))

    def to_dict(self):
        return {'major': self.major,
                'minor': self.minor,
                'name': self.__class__.__name__}


class Ubuntu(OperatingSystem):
    pass


class Centos(OperatingSystem):
    pass
