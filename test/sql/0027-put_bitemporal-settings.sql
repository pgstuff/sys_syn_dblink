BEGIN;

CREATE EXTENSION sys_syn_dblink;
CREATE EXTENSION temporal_tables;
CREATE EXTENSION btree_gist;

INSERT INTO sys_syn_dblink.put_table_transforms(
        rule_group_id,          priority,       final_ids,              in_table_id_like,
        table_settings)
VALUES (NULL,                   25,             '{}',                   'test_table_bitemporal',
        $$
        sys_syn.bitemporal.range_1.column_name          => trans_period,
        sys_syn.bitemporal.range_1.lower.column_name    => trans_time,
        sys_syn.bitemporal.range_2.column_name          => valid_period,
        sys_syn.bitemporal.range_2_active.column_name   => active_valid_periods
        $$);

INSERT INTO sys_syn_dblink.put_column_transforms(
        rule_group_id,          priority,       final_ids,              data_type_like,                 in_table_id_like,
        column_name_like,       new_data_type,          new_column_name,
        expression)
VALUES (NULL,                   25,             '{date_infinity}',      'date',                         'test_table_bitemporal',
        null,                   null,                   null,
     $$CASE WHEN %1 <= '0001-01-01'::DATE THEN NULL::DATE WHEN %1 >= '9999-12-31'::DATE THEN NULL::DATE ELSE %1 END$$);


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

SELECT sys_syn_dblink.processing_table_add (
        proc_schema     => 'processor_data',
        in_table_id     => 'test_table_bitemporal',
        out_group_id    => 'out',
        put_group_id    => 'put',
        put_schema      => 'put_data',
        table_type_id   => 'sys_syn-bitemporal',
        dblink_connname => 'sys_syn_test');


SELECT * FROM processor_data.test_table_bitemporal_out_claim();

SELECT * FROM processor_data.test_table_bitemporal_out_pull();

SELECT  trans_id_in, delta_type, queue_priority, hold_updated, prior_hold_reason_count, prior_hold_reason_id, prior_hold_reason_text, id, attributes, no_diff
FROM    processor_data.test_table_bitemporal_out_processing
ORDER BY id, attributes;

SELECT * FROM processor_data.test_table_bitemporal_out_process();

SELECT  test_table_bitemporal_id, trans_period, valid_period, test_table_bitemporal_text
FROM    put_data.test_table_bitemporal_out
ORDER BY test_table_bitemporal_id, trans_period, valid_period, test_table_bitemporal_text;

SELECT  test_table_bitemporal_id, trans_period, valid_period, test_table_bitemporal_text
FROM    put_data.test_table_bitemporal_out_history
ORDER BY test_table_bitemporal_id, trans_period, valid_period, test_table_bitemporal_text;

SELECT  test_table_bitemporal_id
FROM    put_data.test_table_bitemporal_out_immutable
ORDER BY test_table_bitemporal_id;

SELECT  test_table_bitemporal_id, trans_time, valid_period, test_table_bitemporal_text
FROM    put_data.test_table_bitemporal_out_active
ORDER BY test_table_bitemporal_id, trans_time, valid_period, test_table_bitemporal_text;

SELECT  test_table_bitemporal_id, trans_time, valid_period, test_table_bitemporal_text
FROM    put_data.test_table_bitemporal_out_current
ORDER BY test_table_bitemporal_id, trans_time, valid_period, test_table_bitemporal_text;


SELECT  hold_reason_id, hold_reason_text, queue_priority
FROM    processor_data.test_table_bitemporal_out_processed
ORDER BY id;


SELECT * FROM processor_data.test_table_bitemporal_out_push_status();


SELECT dblink_exec('sys_syn_test', 'ROLLBACK');
SELECT dblink_disconnect('sys_syn_test');

ROLLBACK;
