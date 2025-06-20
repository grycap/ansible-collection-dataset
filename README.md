# Ansible Collection - grycap.dataset

A collection to manage datasets in the cloud

## Requirements

You can install all the requirements using `pip install -r requirements.txt`.

* To run the `download_dataset` module, install [datahugger](https://github.com/grycap/datahugger) python package.

* To run the `transfer_dataset_fts` module install [eosc-data-transfer-client](https://gitlab.cern.ch/batistal/eosc-data-transfer-client)
  python package.

## Installation

* From [GitHub](https://github.com/grycap/ansible-collection-dataset):
  Install the Ansible collection from the GitHub repository:

```sh
  ansible-galaxy collection install git+https://github.com/grycap/ansible-collection-dataset
```

## Usage

How to use the `download_dataset` module:

```yaml
- name: Download dataset
  grycap.dataset.download_dataset:
    dataset_url: 10.5061/dryad.x3ffbg7m8
    output_dir: /usr/local/datasets
```

How to use the `transfer_dataset_fts` module:

```yaml
- name: Transfer dataset
  grycap.dataset.transfer_dataset_dts:
    dataset_doi: doi:10.5281/zenodo.10157504
    dts_endpoint: https://data-transfer.service.eosc-beyond.eu
    dts_token: access_token
    destination: s3s://play.min.io/test
```
