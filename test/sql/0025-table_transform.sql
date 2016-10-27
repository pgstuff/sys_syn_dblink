BEGIN;

CREATE EXTENSION sys_syn_dblink;

CREATE SCHEMA processor_data
        AUTHORIZATION postgres;

CREATE SCHEMA put
        AUTHORIZATION postgres;

CREATE SCHEMA sys_syn_proc
        AUTHORIZATION postgres;

SELECT  dblink_connect('sys_syn_test', 'dbname=contrib_regression host=' ||
        quote_literal(split_part((SELECT pg_settings.setting FROM pg_settings WHERE pg_settings.name = 'unix_socket_directories'), ', ', 1)));
SELECT dblink_exec('sys_syn_test', 'BEGIN');

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('sys_syn_test', 'in');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('sys_syn_test', 'out');
INSERT INTO sys_syn_dblink.put_groups_def VALUES ('put');

INSERT INTO sys_syn_dblink.put_table_transforms (
        priority,       put_group_id_like,      new_put_schema
) VALUES (
        150,            'put',                  'put'
);

INSERT INTO sys_syn_dblink.put_table_transforms (
        priority,       dblink_connname_like,   in_group_id_like,       new_proc_schema
) VALUES (
        160,            'sys_syn_test',         'in',                   'sys_syn_proc'
);

SELECT sys_syn_dblink.processing_table_create (
        proc_schema     => 'processor_data',
        in_table_id     => 'test_table',
        out_group_id    => 'out',
        put_group_id    => 'put',
        dblink_connname => 'sys_syn_test');


SELECT * FROM sys_syn_proc.test_table_out_claim();

SELECT * FROM sys_syn_proc.test_table_out_pull();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    sys_syn_proc.test_table_out_processing
ORDER BY id, attributes;

SELECT * FROM sys_syn_proc.test_table_out_process();

SELECT  *
FROM    put.test_table_out
ORDER BY test_table_id, test_table_text;

SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    sys_syn_proc.test_table_out_processed
ORDER BY id;



SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
