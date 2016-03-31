BEGIN;

CREATE EXTENSION sys_syn_dblink;

CREATE SCHEMA reader_data
        AUTHORIZATION postgres;

SELECT dblink_connect('sys_syn_test', 'dbname=contrib_regression');
SELECT dblink_exec('sys_syn_test', 'BEGIN');

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('in');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('out');

SELECT sys_syn_dblink.reading_table_add (
        schema          => 'reader_data',
        in_table_id     => 'test_table',
        out_group_id    => 'out',
        in_group_id     => 'in',
        dblink_connname => 'sys_syn_test');


SELECT * FROM reader_data.test_table_out_claim();

SELECT * FROM reader_data.test_table_out_pull();


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
