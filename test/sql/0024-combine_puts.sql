BEGIN;

CREATE EXTENSION sys_syn_dblink;

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

INSERT INTO sys_syn_dblink.put_column_transforms (
        priority,       out_group_id_like,      in_group_id_like,
        in_table_id_like,       column_name_like,               new_column_name,        new_data_type,
        expression
) VALUES (
        150,            'out',                  'in',
        'test_table_array',     'test_table_array_id',          'combined_id',          'text',
        $$'A'||%1$$
);

INSERT INTO sys_syn_dblink.put_column_transforms (
        priority,       out_group_id_like,      in_group_id_like,
        in_table_id_like,       column_name_like,               new_column_name
) VALUES (
        150,            'out',                  'in',
        'test_table_array',     'test_table_array_updated',     'data_updated'
);

INSERT INTO sys_syn_dblink.put_column_transforms (
        priority,       out_group_id_like,      in_group_id_like,
        in_table_id_like,       column_name_like,               new_column_name
) VALUES (
        150,            'out',                  'in',
        'test_table_array',     'test_table_array_text',        'text_value'
);


INSERT INTO sys_syn_dblink.put_column_transforms (
        priority,       out_group_id_like,      in_group_id_like,
        in_table_id_like,       column_name_like,               new_column_name,        new_data_type,
        expression,
        add_columns
) VALUES (
        150,            'out',                  'in',
        'test_table',           'test_table_id',                'combined_id',          'text',
        $$'B'||%1$$,
        ARRAY[
                ROW(    'data_updated',         'text',         'Id',
                        $$'-infinity'::timestamp with time zone$$,
                        NULL,   'InColumnType',         FALSE,  ARRAY[]::TEXT[],
                        'Id')
        ]::sys_syn_dblink.create_put_column[]
);

INSERT INTO sys_syn_dblink.put_column_transforms (
        priority,       out_group_id_like,      in_group_id_like,
        in_table_id_like,       column_name_like,               new_column_name
) VALUES (
        150,            'out',                  'in',
        'test_table',           'test_table_text',              'text_value'
);


SELECT sys_syn_dblink.processing_table_add (
        proc_schema     => 'processor_data',
        in_table_id     => 'test_table_array',
        out_group_id    => 'out',
        put_group_id    => 'put',
        put_schema      => 'put_data',
        put_table_name  => 'combined_data',
        dblink_connname => 'sys_syn_test');

SELECT sys_syn_dblink.processing_table_add (
        proc_schema     => 'processor_data',
        in_table_id     => 'test_table',
        out_group_id    => 'out',
        put_group_id    => 'put',
        put_schema      => 'put_data',
        put_table_name  => 'combined_data',
        dblink_connname => 'sys_syn_test');


SELECT * FROM processor_data.test_table_array_out_claim();
SELECT * FROM processor_data.test_table_out_claim();

SELECT * FROM processor_data.test_table_array_out_pull();
SELECT * FROM processor_data.test_table_out_pull();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_array_out_processing
ORDER BY id, attributes;

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_out_processing
ORDER BY id, attributes;

SELECT * FROM processor_data.test_table_array_out_process();
SELECT * FROM processor_data.test_table_out_process();

SELECT  *
FROM    put_data.combined_data
ORDER BY 1, 2;

SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    processor_data.test_table_array_out_processed
ORDER BY id;

SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    processor_data.test_table_out_processed
ORDER BY id;

SELECT * FROM processor_data.test_table_array_out_push_status();


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
