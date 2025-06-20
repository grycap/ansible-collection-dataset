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
  - "eosc-data-transfer-client"

author:
    - micafer (@micafer)
'''

EXAMPLES = r'''
- name: Transfer dataset
  grycap.dataset.transfer_dataset_dts:
    dataset_doi: doi:10.5281/zenodo.10157504
    dts_endpoint: https://data-transfer.service.eosc-beyond.eu
    dts_token: access_token
    destination: s3s://play.min.io/test
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

from ansible.module_utils.basic import AnsibleModule
from eosc_data_transfer_client.client import EOSCClient
from eosc_data_transfer_client.models import TransferRequest, FileTransfer, TransferParameters
from eosc_data_transfer_client.endpoints import parse_doi, create_transfer
from eosc_data_transfer_client.exceptions import EOSCError


def transfer_dataset_fts(module):
    dataset_doi = module.params['dataset_doi']
    dts_token = module.params['dts_token']
    destination = module.params['destination']
    dts_endpoint = module.params['dts_endpoint']
    overwrite = module.params['overwrite']

    client = EOSCClient(dts_endpoint, token=dts_token)

    try:
        storage_elements = parse_doi(client, dataset_doi)
    except EOSCError as e:
        module.fail_json(msg=f"Failed to parse de DOI: {str(e)}")

    # Create transfer for the data transfer service
    transfers = []
    for element in storage_elements.elements:
        transfers.append(FileTransfer(
            sources=[element.downloadUrl],
            destinations=[destination + element.name],
            checksum=element.checksum,
            filesize=element.size
        ))

    # Create tranfer paramenters
    params = TransferParameters(
        verifyChecksum=True,
        overwrite=overwrite,
        retry=0
    )

    # Create the file transfer request
    request = TransferRequest(
        files=transfers,
        params=params
    )

    # Submit file transfer request to the EOSC data transfer API
    try:
        response = create_transfer(client, request)
        module.exit_json(
            changed=True,
            jobid=response.jobId
        )
    except EOSCError as e:
        module.fail_json(msg=f"Failed to create the transfer: {str(e)}")


def main():
    module_args = dict(
        dataset_doi=dict(type='str', required=True),
        dts_token=dict(type='str', required=True),
        destination=dict(type='str', required=True),
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
