BEGIN;

CREATE EXTENSION sys_syn_dblink;
CREATE EXTENSION temporal_tables;
CREATE EXTENSION btree_gist;

INSERT INTO sys_syn_dblink.put_table_transforms (
        priority,       in_group_id_like,       proc_table_id_like,       new_put_table_name,
        add_columns
) VALUES (
        100,            'in',                   'test_table',           'test_table_temporal',
        ARRAY[
                --      column_name,            data_type,                      in_column_type,
                --      value_expression,
                --      array_order,    pos_method,             pos_before,     pos_ref_column_names_like,
                --      pos_in_column_type
                ROW(    'system_timestamp',     'timestamp with time zone',     'Attribute',
                        $$'-infinity'::timestamp with time zone$$,
                        1,              'InColumnType',         FALSE,          ARRAY[]::TEXT[],
                        'Attribute')
        ]::sys_syn_dblink.create_put_column[]
);

CREATE SCHEMA processor_data
        AUTHORIZATION postgres;

CREATE SCHEMA put_data
        AUTHORIZATION postgres;

SELECT  dblink_connect('sys_syn_test', 'dbname=contrib_regression host=' ||
        quote_literal(split_part((SELECT pg_settings.setting FROM pg_settings WHERE pg_settings.name = 'unix_socket_directories'), ', ', 1)));
SELECT dblink_exec('sys_syn_test', 'BEGIN');

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('sys_syn_dblink-test', 'in');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('sys_syn_dblink-test', 'out');
INSERT INTO sys_syn_dblink.put_groups_def VALUES ('put');

SELECT sys_syn_dblink.proc_table_create (
        proc_schema     => 'processor_data',
        in_table_id     => 'test_table',
        out_group_id    => 'out',
        put_group_id    => 'put',
        put_schema      => 'put_data',
        table_type_id   => 'sys_syn-temporal',
        dblink_connname => 'sys_syn_test');


SELECT * FROM processor_data.test_table_claim_1();

SELECT * FROM processor_data.test_table_pull_1();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_processing_1
ORDER BY id, attributes;

SELECT * FROM processor_data.test_table_process_1();

SELECT  test_table_id, system_timestamp, test_table_text
FROM    put_data.test_table_temporal
ORDER BY test_table_id, system_timestamp, test_table_text;

SELECT  test_table_id, system_timestamp, test_table_text
FROM    put_data.test_table_temporal_history
ORDER BY test_table_id, system_timestamp, test_table_text;

SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    processor_data.test_table_processed_1
ORDER BY id;


SELECT * FROM processor_data.test_table_push_status_1();


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
