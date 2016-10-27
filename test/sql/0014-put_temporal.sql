BEGIN;

CREATE EXTENSION sys_syn_dblink;
CREATE EXTENSION temporal_tables;
CREATE EXTENSION btree_gist;

/*INSERT INTO sys_syn_dblink.put_column_transforms(
        rule_group_id,  priority,       data_type_like,
        in_table_id_like,       out_group_id_like,      in_group_id_like,
        schema_like,    in_column_type,                         column_name_like,
        new_data_type,  new_in_column_type,                     new_column_name,
        expression,
        create_put_columns,
        omit,   final_ids,                      final_rule)
VALUES (NULL,           100,            NULL,
        'test_table_array',     NULL,                   NULL,
        NULL,           NULL,                                   'test_table_array_updated',
        'tstzrange',    NULL,                                   NULL,
        'tstzrange(%1, null)',
        NULL,
        NULL,   ARRAY[]::TEXT[],                FALSE);*/

CREATE SCHEMA processor_data
        AUTHORIZATION postgres;

CREATE SCHEMA put_data
        AUTHORIZATION postgres;

SELECT  dblink_connect('sys_syn_test', 'dbname=contrib_regression host=' ||
        quote_literal(split_part((SELECT pg_settings.setting FROM pg_settings WHERE pg_settings.name = 'unix_socket_directories'), ', ', 1)));
SELECT dblink_exec('sys_syn_test', 'BEGIN');

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('sys_syn_test', 'in');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('sys_syn_test', 'out');
INSERT INTO sys_syn_dblink.put_groups_def VALUES ('put');

SELECT sys_syn_dblink.processing_table_create (
        proc_schema     => 'processor_data',
        in_table_id     => 'test_table_array',
        out_group_id    => 'out',
        put_group_id    => 'put',
        put_schema      => 'put_data',
        table_type_id   => 'sys_syn-temporal',
        dblink_connname => 'sys_syn_test');


SELECT * FROM processor_data.test_table_array_out_claim();

SELECT * FROM processor_data.test_table_array_out_pull();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_array_out_processing
ORDER BY id, attributes;

SELECT * FROM processor_data.test_table_array_out_process();

SELECT  test_table_array_id, test_table_array_updated, test_table_array_text
FROM    put_data.test_table_array_out
ORDER BY test_table_array_id, test_table_array_text;

SELECT  test_table_array_id, test_table_array_updated, test_table_array_text
FROM    put_data.test_table_array_out_history
ORDER BY test_table_array_id, test_table_array_text;

SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    processor_data.test_table_array_out_processed
ORDER BY id;


SELECT * FROM processor_data.test_table_array_out_push_status();


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
