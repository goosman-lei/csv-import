# csv-import: A simple tool for import data from csv file to database

## Usage:

1. modify dbconfig.default, config to your mysql server

2. like this:

```bash
python3 csv-import.py --input your-csv-file-path --db your-db-name --table your-table-name --batch 10000 --skip 1
```
