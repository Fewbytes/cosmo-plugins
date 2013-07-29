#/*******************************************************************************
# * Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
# *
# * Licensed under the Apache License, Version 2.0 (the "License");
# * you may not use this file except in compliance with the License.
# * You may obtain a copy of the License at
# *
# *       http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.
# *******************************************************************************/

"""
Celery tasks for running recipes through chef-client.

This file implements the MiddleWareServer.installer interface, where for each of tasks, we check that chef is configured
and run the relevant runlist using the chef_client module.
"""

from cosmo.celery import celery
import celery.utils.log
from chef_client import set_up_chef_client, run_chef

logger = celery.utils.log.get_task_logger(__name__)


@celery.task
@set_up_chef_client
def install(chef_install_runlist, chef_attributes, **kwargs):
    run_chef(chef_install_runlist, chef_attributes)


@celery.task
@set_up_chef_client
def start(chef_start_runlist, chef_attributes, **kwargs):
    run_chef(chef_start_runlist, chef_attributes)


@celery.task
@set_up_chef_client
def stop(chef_stop_runlist, chef_attributes, **kwargs):
    run_chef(chef_stop_runlist, chef_attributes)


@celery.task
@set_up_chef_client
def restart(chef_start_runlist, chef_stop_runlist, chef_attributes, **kwargs):
    run_chef(chef_stop_runlist, chef_attributes)
    run_chef(chef_start_runlist, chef_attributes)


@celery.task
@set_up_chef_client
def uninstall(chef_uninstall_runlist, chef_attributes, **kwargs):
    run_chef(chef_uninstall_runlist, chef_attributes)
