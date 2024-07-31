# Ansible Collection - grycap.dataset

A collection to manage datasets in the cloud

## Requirements

* To run the `download_dataset` module, install [datahugger](https://github.com/J535D165/datahugger) python package.
  Using either `pip install datahugger` or `pip install -r requirements.txt`.

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
