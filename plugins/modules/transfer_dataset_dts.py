#!/usr/bin/python

# Copyright: (c) 2025, Miguel Caballer <micafer1@upv.es>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: transfer_dataset_dts

short_description: This module transfers a dataset to an storage endpoint
using the EOSC Beyond DTS service.

description: This module takes a dataset URL/DOI and an storage endpoint directory, and
uses the EOSC Beyond DTS to transfer the dataset to the specified storage system.

options:
    dataset_doi:
        description: The DOI of the dataset to download
        required: true
        type: str
    dts_token:
        description: The access token to authenticate with the EOSC Beyond
        required: true
        type: str
    destination:
        description: The URL of the destination storage system
        required: true
        type: str
    destination_type:
        description: The type of the destination storage system (e.g., s3, dcache, storm ...). See DTS documentation for supported types
        required: true
        type: str
    dest_authorization:
        description: The remote authorization token if needed by the destination
        required: false
        default: None
        type: str
    dts_endpoint:
        description: The URL of the DTS endpoint
        required: false
        default: https://data-transfer.service.eosc-beyond.eu
        type: str
    overwrite:
        description: Flag to specify if the destination files will be overwritten
        required: false
        default: false
        type: bool

requirements:
  - "python >= 3.8"
  - "requests >= 2.20"

author:
    - micafer (@micafer)
'''

EXAMPLES = r'''
- name: Transfer dataset
  grycap.dataset.transfer_dataset_dts:
    dataset_doi: doi:10.5281/zenodo.10157504
    dts_endpoint: https://data-transfer.service.eosc-beyond.eu
    dts_token: access_token
    destination: https://play.min.io/test
    destination_type: s3
    dest_authorization:
        token_type: password
        user: miniouser
        token: miniopassword
    overwrite: true
'''

RETURN = r'''
changed:
    description: Whether the module made any changes.
    type: bool
    returned: always
jobid:
    description: The job ID of the created DTS transference
    type: str
    returned: always
    sample: 1dab1f08-4db4-11f0-8c8d-fa163edd14cf"
'''

import json
import requests
import base64
from ansible.module_utils.basic import AnsibleModule


def resolve_doi(dts_endpoint, dataset_doi):
    """
    Resolve a DOI to its final URL using the Data Transfer Service parser endpoint.
    """
    try:
        response = requests.get(f'{dts_endpoint}/parser?doi={dataset_doi}',
                                headers={'Accept': 'application/json'},
                                timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get('elements', [])
    except requests.RequestException as e:
        raise ValueError(f"Failed to resolve DOI {dataset_doi}: {str(e)}")


def create_transfer_request(sources, dest, overwrite):
    params = {
        "verifyChecksum": True,
        "overwrite": overwrite,
        "retry": 0,
        "priority": 0
    }

    if not dest.endswith('/'):
        dest += '/'
    files = []
    for item in sources:
        files.append({
            "sources": [item.get('downloadUrl')],
            "destinations": [dest + item.get('name')]
        })
    return json.dumps({"files": files, "params": params})


def transfer_dataset_fts(module):
    dataset_doi = module.params['dataset_doi']
    dts_token = module.params['dts_token']
    destination = module.params['destination']
    destination_type = module.params['destination_type']
    dts_endpoint = module.params['dts_endpoint']
    overwrite = module.params['overwrite']
    dest_authorization = module.params.get('dest_authorization', {})

    # Submit file transfer request to the EOSC data transfer API
    try:
        elements = resolve_doi(dts_endpoint, dataset_doi)
        payload = create_transfer_request(elements, destination, overwrite)

        headers = {
            'Authorization': f'Bearer {dts_token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        if dest_authorization:
            auth_storage = None
            token_type = dest_authorization.get('token_type', 'password')
            if token_type == 'password':
                user = dest_authorization.get('user', '')
                token = dest_authorization.get('token', '')
                auth_storage = base64.b64encode(f'{user}:{token}'.encode()).decode()
            elif token_type in ['token', 'bearer']:
                token = dest_authorization.get('token', '')
                auth_storage = token

            if auth_storage:
                headers['Authorization-Storage'] = auth_storage

        response = requests.post(f"{dts_endpoint}/transfers?dest={destination_type}",
                                 headers=headers, data=payload, timeout=30)
        response.raise_for_status()
        response_data = response.json()
        joibid = response_data.get('jobId', None)

        if joibid:
            module.exit_json(
                changed=True,
                jobid=joibid
            )
        else:
            module.fail_json(msg=f"Failed to retrieve job ID from the transfer response: {response.text}")
    except Exception as e:
        module.fail_json(msg=f"Failed to create the transfer: {str(e)}")


def main():
    module_args = dict(
        dataset_doi=dict(type='str', required=True),
        dts_token=dict(type='str', required=True),
        destination=dict(type='str', required=True),
        destination_type=dict(type='str', required=True),
        dest_authorization=dict(type='dict', required=False, default={}),
        dts_endpoint=dict(type='str', required=False,
                          default="https://data-transfer.service.eosc-beyond.eu"),
        overwrite=dict(type='bool', required=False, default=False)
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    transfer_dataset_fts(module)


if __name__ == '__main__':
    main()
