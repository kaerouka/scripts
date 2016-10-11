# coding: utf-8

import sqlite3
import re
import hashlib
import argparse
import datetime
import os
import os.path
from prettytable import PrettyTable
import sys


def diff_sqlite_tables(db_path1, db_path2, ignore, ignore_tables, ignore_tbl_col, unique, output_path, memory=1024):
    u"""
    sqliteのテーブルの差分を標準出力に出力します。
    @param db_path1: sqliteのデータファイルへのパス
    @param db_path2: sqliteのデータファイルへのパス
    @param ignore: 無視するカラム名
    @param ignore_tables: 無視するテーブル名
    @param ignore-tbl-col: 無視するテーブル名とカラムの組み合わせ
    @param unique: 差分比較の際に使用するテーブルとキーの組み合わせ
    @param output_path: 差分DBの保存先ディレクトリ
    @param memory: sqliteで使用するメモリ（単位：MB）
    """
    # 存在確認
    if not os.path.exists(db_path1):
        raise FileNotFoundError("{0} does not exists.".format(db_path1))
    if not os.path.exists(db_path2):
        raise FileNotFoundError("{0} does not exists.".format(db_path2))

    ignore = [x.strip() for x in ignore] if ignore is not None else None
    ignore_tables = [x.strip() for x in ignore_tables] if ignore_tables is not None else None
    ignore_tbl_col_dic = None if ignore_tbl_col is None else parse_table_and_key(ignore_tbl_col)
    unidic = None if unique is None else parse_table_and_key(unique)
    db_dic1 = tables_dic(db_path1, ignore, ignore_tbl_col_dic, unidic, memory)
    db_dic2 = tables_dic(db_path2, ignore, ignore_tbl_col_dic, unidic, memory)

    fields = ['old_tbl', 'new_tbl', 'status']
    pt = PrettyTable(fields)
    # とりあえず全て左寄せにする
    pt.align = 'l'
    # statusだけ中央寄せ
    pt.align["status"] = 'c'

    #差分があったテーブル名
    diff_tables = []

    for key in sorted(db_dic1.keys()):
        exists = True if key in db_dic2 else False

        if exists:
            if ignore_tables is not None and key in ignore_tables:
                pt.add_row([key, key, 'Ignore'])
            elif db_dic1[key].md5 == db_dic2[key].md5:
                pt.add_row([key, key, 'OK'])
            elif db_dic1[key].sql != db_dic2[key].sql:
                pt.add_row([key, key, '*Invalid*'])
            else:
                pt.add_row([key, key, '*NG*'])
                diff_tables.append(db_dic1[key])
        else:
            if ignore_tables is not None and key in ignore_tables:
                pt.add_row([key, '', 'ignore'])
            else:
                pt.add_row([key, '', ''])

    for key in sorted(db_dic2.keys()):
        exists = True if key in db_dic1 else False

        if not exists:
            pt.add_row(['', key, ''])

    #結果出力
    print(pt)

    #差分があった場合、SQLiteに差異を出力
    if len(diff_tables) != 0:
        db_name = diff_table(db_path1, db_path2, diff_tables, output_path)
        print(u"%sに差分を保存しました。" % db_name)


def diff_table(db_path1, db_path2, diff_tables, output_path=None):
    u"""
    db_path1とdb_path2のうち、diff_tablesで含まれるテーブルの差分を取得して
    sqliteのデータファイルとして保存します。
    @rtype : str 保存したDB（ファイル）名
    @param db_path1: SQLiteのデータベースへのパス
    @param db_path2: SQLiteのデータベースへのパス
    @param diff_tables: TableDefクラスの配列。
    @param output_path: 差分出力先ディレクトリパス
    """
    if output_path is None:
        output_path = '.'

    # 結果保存用DB
    db_name = None
    i = 0
    while True:
        db_name = "%s%s%s_diff%s.db" % (output_path, os.sep, os.path.splitext(os.path.basename(db_path1))[0], "" if i == 0 else str(i))
        if not os.path.exists(db_name):
            break
        i += 1

    conn = sqlite3.connect(db_name)
    conn.row_factory = sqlite3.Row

    #元ネタのDBに接続
    conn.execute('attach "%s" as db_1' % db_path1)
    conn.execute('attach "%s" as db_2' % db_path2)

    for table_def in diff_tables:
        state_col = ''
        i = 0
        while True:
            state_col = 'status%s' % ('' if i == 0 else str(i))
            if state_col not in table_def.cols:
                break
            i += 1

        #結果保存テーブル作成
        conn.execute("create table %s as select '' as %s, * from db_1.%s where 1=2"
                     % (table_def.name, state_col, table_def.name))

        #差分を取得
        #temp1 = old - new
        sql = "create temporary table _%s1 as select * from (select * from db_1.%s except select * from db_2.%s)" \
              % tuple([table_def.name] * 3)
        conn.execute(sql)
        #temp2 = new - old
        sql = "create temporary table _%s2 as select * from (select * from db_2.%s except select * from db_1.%s)" \
              % tuple([table_def.name] * 3)
        conn.execute(sql)

        # unique指定されていれば、テーブル定義でPK指定されているカラムを使用せずに
        # テーブル同士の差分取得にunique指定されたカラムを使用する。
        if table_def.unique is not None:
            pks = table_def.unique
        # テーブル定義でプライマリーキー指定されている列の配列
        elif table_def.pks:
            pks = table_def.pks
        # ユニーク指定されておらず、PKも存在しない場合、全部のカラムで複合PK扱いにして差分を取得する
        else:
            pks = table_def.cols

        # 重複確認
        # unique指定されているテーブルに関しては、重複確認も行う
        if table_def.unique is not None:
            # oldDBのテーブルは、[table_name]1、newDBのテーブルは[table_name]2となるので、繰り返す
            for i in [1, 2]:
                sql = "insert into %s select 'dup_%s', %s from _%s%d a " \
                      "where exists (select 1 from _%s%d b where %s and a.rowid <> b.rowid)" \
                      % (table_def.name, 'old' if i == 1 else 'new',
                         ",".join(table_def.cols), table_def.name, i, table_def.name, i,
                         " and ".join("a.%s = b.%s" % (x, x) for x in pks))
                conn.execute(sql)

        # temp1.pk - temp2.pk
        # temp2.pk - temp1.pk
        for i, j in [(1, 2), (2, 1)]:
            sql = "select %s from _%s%d except select %s from _%s%d" \
                  % (",".join(pks),
                     table_def.name,
                     i,
                     ",".join(pks),
                     table_def.name,
                     j)
            r = conn.execute(sql)
            for data in r:
                sql = "select %s from _%s%d where %s" \
                      % (", ".join(table_def.cols),
                         table_def.name,
                         i,
                         "and ".join([x + " = ? " for x in pks]))
                r2 = conn.execute(sql, tuple(data))
                row = r2.fetchone()

                v = "'%s', %s" % ('-' if i == 1 else '+', ",".join(['?'] * len(table_def.cols)))
                ins_sql = "insert into %s (%s, %s) values (%s)" \
                          % (table_def.name, state_col, ", ".join(table_def.cols), v)

                conn.execute(ins_sql, tuple(row))

        # old_dbにもnew_dbにも存在する行
        sql = "select %s from _%s1 intersect select %s from _%s2" \
              % (",".join(pks), table_def.name, ",".join(pks), table_def.name)

        r = conn.execute(sql)

        for data in r:
            rows = []
            for i in [1, 2]:
                sql = "select * from _%s%d where %s" \
                      % (table_def.name, i, "and ".join([x + " = ? " for x in pks]))
                rows.append(conn.execute(sql, tuple(data)).fetchone())

            #差異があるか？
            is_ins = False
            for key in rows[0].keys():
                if (key not in table_def.ignore_cols) and rows[0][key] != rows[1][key]:
                    is_ins = True
                    break

            if is_ins:
                for i in [1, 2]:
                    v = "'u%s', %s" % ('-' if i == 1 else '+', ",".join(['?'] * len(table_def.cols)))
                    ins_sql = "insert into %s (%s, %s) values (%s)" \
                              % (table_def.name, state_col, ", ".join(table_def.cols), v)
                    conn.execute(ins_sql, tuple(rows[i-1]))
    conn.commit()

    return db_name


def parse_table_and_key(table_and_key):
    u"""
    table_name1:key1[,key2 ...] [[table_name2:key1[,key2]]]...の文字列を解析し、テーブル名とユニークキーの辞書を返します。
    @param unique: テーブル名とユニークキーの文字列
    @rtype : dictionary : {table_name1:[unique_key1, unique_key2...], table_name2:[unique_key1, ...]}
    """
    d = {}

    #[table_name1:key1,key2,key3]
    for u in table_and_key:
        (t, keys) = u.split(':')
        d[t] = [x.strip() for x in keys.split(',')]

    return d


def tables_dic(db_path, ignore=None, ignore_tbl_col_dic=None, unidic=None, memory=1024):
    # sqlite_masterテーブルのsqlカラムから各種情報を取得するための正規表現
    # 想定するsql列の内容
    #
    # CREATE TABLE [t001_black_history]
    # ([file_no] INTEGER NOT NULL,[collate_no] INTEGER NOT NULL,PRIMARY KEY(file_no,collate_no))

    # カラム名が1つずつmatcherオブジェクトになる
    col_pat = re.compile(r'(?<!CREATE TABLE )\[([^\]]+)\]')
    # file_no,collate_no がまとめて1つのmatcherとして返される。
    pk_pat = re.compile(r'PRIMARY KEY\((.+)\)')

    conn = sqlite3.connect(db_path)
    # カラム名アクセス
    conn.row_factory = sqlite3.Row

    # 使用メモリ調整。
    # dbのpage_size取得
    r = conn.execute("pragma page_size")
    page_size = r.fetchone()[0]

    # cache_size * page_size = memory(単位：MB)
    cache_size = memory * 1024 * 1024 / page_size
    conn.execute("pragma cache_size = %s" % cache_size)

    results = conn.execute("select name, sql from sqlite_master where type='table'")

    #共通ignore列と、テーブルごとのignore列をまとめて保持する
    ignore_all = {}

    db_tbls = {}
    for row in results:
        print("%s" % row['name'])
        
        ignore_all = []
        
        if ignore is not None:
            ignore_all += ignore
        if ignore_tbl_col_dic is not None and row['name'] in ignore_tbl_col_dic:
            ignore_all += ignore_tbl_col_dic[row['name']]

        col_iter = col_pat.finditer(row['sql'].replace("\n", ""))
        cols = [m.group(1) for m in col_iter]

        pk_iter = pk_pat.finditer(row['sql'])
        pks = [m.group(1) for m in pk_iter]

        # hash値計算
        not_ignore_cols = ','.join([x for x in cols if x not in ignore_all])
        hash_str = None
        if not_ignore_cols:
            t_results = conn.execute("select %s from %s order by %s" % (not_ignore_cols, row['name'], not_ignore_cols))
            m = hashlib.md5()
            exists_rows = False
            for i, t_result in enumerate(t_results, 1):
                if i % 10000 == 1:
                    print(u'calculating the hash value of %d row...' % i)
                exists_rows = True
                for data in t_result:
                    m.update(str(data).encode('utf-8'))

            if not exists_rows:
                print('no rows.')
            else:
                hash_str = m.digest()
        else:
            print('all ignore.')

        # TableDef作成
        t_def = TableDef()
        t_def.name = row['name']
        t_def.cols = cols
        t_def.pks = pks[0].split(',') if pks and 0 < len(pks[0]) else []
        t_def.sql = row['sql']
        t_def.md5 = hash_str
        t_def.unique = None if unidic is None or row['name'] not in unidic else unidic[row['name']]
        t_def.ignore_cols = ignore_all
        db_tbls[t_def.name] = t_def
        print()

    conn.close()

    return db_tbls


class TableDef:
    u"""
    テーブルの定義を示すクラス。
    """
    name = None
    pks = []
    cols = []
    sql = None
    md5 = None

    def __repr__(self):
        return "name:%s, pks:%s, cols:%s, ignore_cols:%s" % (self.name, self.pks, self.cols, self.ignore_cols)

    def __str__(self):
        return "name:%s, pks:%s, cols:%s, ignore_cols:%s" % (self.name, self.pks, self.cols, self.ignore_cols)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='''
It will get the difference between the data file of sqlite.
A result is outputted to standard output as a table.
+--------------------------------+--------------------------------+--------+
| old_tbl                        | new_tbl                        | status |
+--------------------------------+--------------------------------+--------+
| t060_filtering_collate_history | t060_filtering_collate_history |   OK   |
| t022_text                      | t022_text                      |   OK   |
| t027_alias_num                 | t027_alias_num                 |  *NG*  |
| t013_num                       | t013_num                       |   OK   |
+--------------------------------+--------------------------------+--------+
    old_tbl : table name of old_db_path
    new_tbl : table name of new_db_path
    result  : OK means those without difference.
              *NG* means those with difference.
              *Invalid* means that the definitions of a table differ.
              ignore means not compare.

difference of data:
When data has a difference, it is saved with the data file of sqlite of the name of diff.db.
Only a row with the difference in the different table is saved.
The column of the name of 'status' is added to the table.
The meaning of the data of the column
    +       : The row which is not in 'old_tbl' and exists in 'new_tbl'
    -       : The row which is not in 'new_tbl' and exists in 'old_tbl'
    u+      : The row which exists in both of tables if primary key compares. The data of old_tbl itself.
    u-      : The row which exists in both of tables if primary key compares. The data of new_tbl itself.
    dup_new : When the unique option is specified and a duplication row is checked by new_db.
    dup_old : When the unique option is specified and a duplication row is checked by old_db.

examples:
    ---------- example 1 ----------
    A.db compared with B.db.
        >>>python sqlite_tools.py A.db B.db
        An absolute path may describe.
        >>>python sqlite_tools.py c:\\tmp\A.db c:\\tmp\B.db

    ---------- example 2 ----------
    id column is disregarded and compared.
        >>>python sqlite_tools.py A.db B.db --ignore id

    ---------- example 3 ----------
    test table is disregarded and compared.
    It is displayed on the status column of a result as a 'ignore'.
        >>>python sqlite_tools.py A.db B.db --ignore-tbl test

    ---------- example 4 ----------
    When performing a differential comparison, when compared using the specified column.
    This is mainly used to the table which is using the surrogate key.
        >>>python sqlite_tools.py A.db B.db --ignore id collate_date ^
        >>>--unique t063_filtering_collate_result:parent_table_id,cif_person_id,black_pk

    ---------- example 5 ----------
    id of test table is disregarded and compared.
        >>>python sqlite_tools.py A.db B.db --ignore-tbl-col test:id
''')
    parser.add_argument('old_db_path')
    parser.add_argument('new_db_path')
    parser.add_argument('--memory', type=int,
                        help="Memory to allocate to sqlite(MByte). As python, "
                             "it uses a maximum of 3 about times. "
                             "Depending on the size of the table, the amount also changes.")
    parser.add_argument('--ignore', nargs='*', help="Column names to ignore. It is applied to few the tables.")
    parser.add_argument('--ignore-tbl', nargs='*', help="Table names to ignore.")
    parser.add_argument('--ignore-tbl-col', nargs='*', help="Column names to ignore. It is applied to specified the tables.")
    parser.add_argument('--unique', nargs='*',
                        help="When acquiring difference, it is used as a key. "
                             "--unique format is table_name1:key1[,key2 ... ]")
    parser.add_argument('--silent', action='store_true', default=False, help='It does not output to stdout.')
    parser.add_argument('--output-path', help='The output place of difference.')

    args = parser.parse_args()

    if args.silent:
        # 標準出力の退避
        stdout = sys.stdout
        # 標準出力を破棄
        f = open(os.devnull, "w")
        sys.stdout = f

    start = datetime.datetime.today()

    if args.memory is None:
        diff_sqlite_tables(args.old_db_path, args.new_db_path, args.ignore, args.ignore_tbl, args.ignore_tbl_col, args.unique,
                           args.output_path)
    else:
        diff_sqlite_tables(args.old_db_path, args.new_db_path, args.ignore, args.ignore_tbl, args.ignore_tbl_col, args.unique,
                           args.output_path, args.memory)

    print(u'Processing time: %s' % (datetime.datetime.today() - start))

    if args.silent:
        # 標準出力をプログラム実行前に戻す
        sys.stdout = stdout

