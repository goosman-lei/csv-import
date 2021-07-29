# -*- coding:utf-8 -*-

# Usage:
#   1. modify dbconfig.default, config to your mysql server
#   2. like this: python3 csv-import.py --input your-csv-file-path --db your-db-name --table your-table-name --batch 10000 --skip 1

import os
import sys, getopt
import pymysql
import datetime

def debug_lineno_incr(opts):
    opts['debug']['lineno'] += 1
def debug_lineno(opts):
    return opts['debug']['lineno']
def debug_usetime_start(flag):
    opts['debug']['usetime'][flag] = datetime.datetime.now()
def debug_usetime(flag):
    return (datetime.datetime.now() - opts['debug']['usetime'][flag]).total_seconds()
def debug_print(*args):
    print(*args, file = sys.stderr, flush = True)

def init_opts():
    try:
        # --input: input csv file path
        # --target: key of opts_map.dbconfig.
        # --db: import into which database
        # --table: import into which table
        # --batch: how many rows package into a SQL
        # --skip: skip some row from input file head
        opts, args = getopt.getopt(sys.argv[1:], '', longopts = ['input=', 'target=', 'db=', 'table=', 'batch=', 'skip='])
    except getopt.GetoptError:
        debug_print("get option error")
        sys.exit(1)
    opts_map = {
        'action': 'import',
        'target': 'default',
        'batch': 10000,
        'skip': 0,
        'dt_now': datetime.datetime.now(),
        'dbconfig': {
            'default': {
                'host': 'host-of-mysql-server',
                'port': 3306,
                'user': 'user-of-mysql-server',
                'password': 'password-of-mysql-server',
            }
        },
        'debug': {
            'lineno': 0,
            'usetime': {},
        },
    }
    for opt_k, opt_v in opts:
        opts_map[opt_k[2:]] = opt_v
    if 'input' not in opts_map:
        debug_print('must specify --input')
        sys.exit(1)
    if 'db' not in opts_map:
        debug_print('must specify --db')
        sys.exit(1)
    if 'table' not in opts_map:
        debug_print('must specify --table')
        sys.exit(1)
    opts_map['batch'] = int(opts_map['batch'])
    opts_map['skip'] = int(opts_map['skip'])

    opts_map['fields'] = db_fields(opts_map)
    return opts_map

def db_fields(opts):
    with pymysql.connect(**opts['dbconfig'][opts['target']]) as conn:
        conn.execute('SELECT * FROM %s.%s LIMIT 0' % (opts['db'], opts['table']))
        fields = []
        for field in conn.description:
            fields.append('`%s`' % field[0])
        return fields

def db_import(opts, rows):
    values = []
    for row in rows:
        values.append('(%s)' % ','.join(row))
    sql = 'INSERT INTO %s.%s (%s) VALUES %s' % (opts['db'], opts['table'], ','.join(opts['fields']), ','.join(values))
    with pymysql.connect(**opts['dbconfig'][opts['target']]) as conn:
        try:
            conn.execute(sql)
            debug_print('import success(lineno: %d, time: %0.2fs)' % (debug_lineno(opts), debug_usetime('batch-process')))
        except Exception as err:
            debug_print('import error(lineno: %d, time: %0.2fs)' % (debug_lineno(opts), debug_usetime('batch-process')), err)
            debug_print(sql)
    debug_usetime_start('batch-process')

def do_skip(opts, fp):
    skip = opts['skip']
    while skip > 0:
        fp.readline()
        debug_lineno_incr(opts)
        skip -= 1
    debug_print('skip %d line' % opts['skip'])

def do_import(opts, fp):
    rows = []
    debug_usetime_start('batch-process')
    while True:
        line = fp.readline()
        debug_lineno_incr(opts)
        if not line:
            break
        cols = line[:-1].split('\t')
        cols = ['"%s"' % col.replace('"', '\\"') for col in cols]
        rows.append(cols)
        if len(rows) == opts['batch']:
            db_import(opts, rows)
            rows = []

    if rows:
        db_import(opts, rows)

if __name__ == '__main__':
    opts = init_opts()
    debug_print('init opts success')

    fp = open(opts['input'], 'r')

    do_skip(opts, fp)

    do_import(opts, fp)

