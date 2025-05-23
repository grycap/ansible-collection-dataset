#!/usr/bin/python

# Copyright: (c) 2024, Antonio Sanchez <asanchez@i3m.upv.es>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

from ansible.module_utils.basic import AnsibleModule
import datahugger
import os
import pwd


DOCUMENTATION = r'''
---
module: download_dataset

short_description: This module downloads a dataset and saves it locally.

description: This module takes a dataset URL/DOI and an output directory, and downloads the dataset to the specified directory using the Datahugger library.

options:
    dataset_url:
        description: The URL or DOI of the dataset to download
        required: true
        type: str
    output_dir:
        description: The directory where the dataset should be saved.
        required: true
        type: str
    owner:
        description: The user owner of the directory where the dataset should be saved.
        required: false
        type: str

requirements:
  - "python >= 3.8"
  - "datahugger >= 0.12"

author:
    - asanchez (@AntonioSanch3z)
'''

EXAMPLES = r'''
- name: Download dataset
  grycap.dataset.download_dataset:
    dataset_url: 10.5061/dryad.x3ffbg7m8
    output_dir: /usr/local/datasets
    owner: username
'''

RETURN = r'''
changed:
    description: Whether the module made any changes.
    type: bool
    returned: always
message:
    description: The output message that the test module generates.
    type: str
    returned: always
    sample: "Dataset downloaded successfully to /usr/local/datasets"
'''


def change_owner(path, owner):
    """
    Change the owner of the downloaded dataset directory.
    """
    try:
        # Get the user ID of the specified owner
        if owner.isdigit():
            uid = int(owner)
        else:
            uid = pwd.getpwnam(owner).pw_uid

        for root, dirs, files in os.walk(path):
            for name in dirs + files:
                os.chown(os.path.join(root, name), uid, -1)

        os.chown(path, uid, -1)  # Change owner of the main directory as well
    except Exception as e:
        raise Exception(f"Failed to change owner: {str(e)}")


def download_dataset(module):
    dataset_url = module.params['dataset_url']
    output_dir = module.params['output_dir']
    owner = module.params.get('owner')
    dataset_name = dataset_url.split('/')[-1].replace('.', '_')
    dataset_path = os.path.join(output_dir, dataset_name)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    if os.path.exists(dataset_path):
        result = {
            'changed': False,
            'msg': f"Dataset already exists at {dataset_path}"
        }
        module.exit_json(**result)

    try:
        os.makedirs(dataset_path, exist_ok=True)
        # Download dataset using datahugger
        datahugger.get(dataset_url, dataset_path)
        result = {
            'changed': True,
            'msg': f"Dataset downloaded successfully to {output_dir}"
        }
        if owner:
            # Change ownership if specified
            change_owner(dataset_path, owner)
        module.exit_json(**result)
    except Exception as e:
        module.fail_json(msg=f"Failed to download dataset: {str(e)}")


def main():
    module_args = dict(
        dataset_url=dict(type='str', required=True),
        output_dir=dict(type='str', required=True),
        owner=dict(type='str', required=False, default=None)
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    download_dataset(module)


if __name__ == '__main__':
    main()
