#ColCura

ColCura is a Python-based database audit tool that discovers database schema information and generates metadata for database columns. The project is designed to support multiple database types, SQLite and CSV are provided as examples, and allows for extensible metadata generation.
Features

Support for multiple database types (SQLite, CSV, and more can be added).
Extensible metadata configuration.
Easy-to-use YAML configuration files to specify databases and metadata.

Getting Started

Clone the repository:

```
git clone https://github.com/mattjoyce/ColCura.git
```
Install the required dependencies:

```
pip install -r requirements.txt
```
Configure the YAML file with your database settings and desired metadata.

Run the audit script:


```
python Audit.py --config your_config.yaml
```

Example Configuration

An example YAML configuration file is provided below:

```
Test SQLite DB 1:
  type: sqlite
  connection_string: test1.db
  metadata: DiscoveryDate, MyTag1, FindTable, GPTPII

Test SQLite DB 2:
  type: sqlite
  connection_string: test2.db
  metadata: DiscoveryDate, MyTag1, FindColumn
```

##Contributing

Feel free to submit pull requests or open issues to contribute to the project. Ensure that your code is well-documented and follows the PEP8 style guide.
License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for more information.