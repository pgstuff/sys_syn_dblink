BEGIN;

CREATE EXTENSION sys_syn_dblink;

CREATE SCHEMA processor_data
        AUTHORIZATION postgres;

SELECT  dblink_connect('sys_syn_test', 'dbname=contrib_regression host=' ||
        quote_literal(split_part((SELECT pg_settings.setting FROM pg_settings WHERE pg_settings.name = 'unix_socket_directories'), ', ', 1)) ||
        'port=' || current_setting('port'));
SELECT dblink_exec('sys_syn_test', 'BEGIN');

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('sys_syn_dblink-test', 'in');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('sys_syn_dblink-test', 'out');
INSERT INTO sys_syn_dblink.put_groups_def VALUES ('put');



SELECT sys_syn_dblink.proc_table_create (
        proc_schema     => 'processor_data',
        in_table_id     => 'parent_table',
        out_group_id    => 'out',
        put_group_id    => 'put',
        dblink_connname => 'sys_syn_test');

SELECT * FROM processor_data.parent_table_claim_1();

SELECT * FROM processor_data.parent_table_pull_1();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.parent_table_processing_1
ORDER BY id, attributes;

SELECT * FROM processor_data.parent_table_process_1();

SELECT  *
FROM    public.parent_table
ORDER BY parent_table_id, parent_table_text;

SELECT * FROM processor_data.parent_table_push_status_1();

SELECT * FROM dblink('sys_syn_test', 'SELECT * FROM user_data.parent_table_out_processed_1()') AS result(result boolean);

SELECT dblink_exec('sys_syn_test', 'UPDATE sys_syn.trans_id_mod SET trans_id_mod = trans_id_mod + 1');

SELECT dblink_exec('sys_syn_test', 'SET LOCAL sys_syn.trans_id_curr TO 2');

SELECT * FROM dblink('sys_syn_test', 'SELECT * FROM user_data.child_table_pull(FALSE)') AS result(result boolean);

SELECT * FROM dblink('sys_syn_test', 'SELECT * FROM user_data.child_table_out_move_1()') AS result(result boolean);



SELECT sys_syn_dblink.proc_table_create (
        proc_schema     => 'processor_data',
        in_table_id     => 'child_table',
        out_group_id    => 'out',
        put_group_id    => 'put',
        dblink_connname => 'sys_syn_test');

SELECT  cluster_id,             foreign_proc_table_id,  foreign_key_index,
        primary_in_table_id,    foreign_column_name,    primary_column_name
FROM    sys_syn_dblink.proc_foreign_keys
ORDER BY cluster_id, foreign_proc_table_id, foreign_key_index, foreign_column_name;

SELECT * FROM processor_data.child_table_claim_1();

SELECT * FROM processor_data.child_table_pull_1();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.child_table_processing_1
ORDER BY id, attributes;

SELECT * FROM processor_data.child_table_process_1();

SELECT  *
FROM    public.child_table
ORDER BY child_table_id, parent_table_id;

SELECT * FROM processor_data.child_table_push_status_1();



SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
