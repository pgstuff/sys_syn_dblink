BEGIN;

CREATE EXTENSION sys_syn_dblink;

CREATE SCHEMA "Processor Schema"
        AUTHORIZATION postgres;

SELECT  dblink_connect('SysSyn Data', 'dbname=contrib_regression host=' ||
        quote_literal(split_part((SELECT pg_settings.setting FROM pg_settings WHERE pg_settings.name = 'unix_socket_directories'), ', ', 1)));
SELECT dblink_exec('SysSyn Data', 'BEGIN');

INSERT INTO sys_syn_dblink.in_groups_def VALUES ('SysSyn Data', 'In Group');
INSERT INTO sys_syn_dblink.out_groups_def VALUES ('SysSyn Data', 'Out Group');
INSERT INTO sys_syn_dblink.put_groups_def VALUES ('Put Group');

SELECT sys_syn_dblink.processing_table_create (
        proc_schema     => '"Processor Schema"'::regnamespace,
        in_table_id     => 'Test Table',
        out_group_id    => 'Out Group',
        put_group_id    => 'Put Group',
        dblink_connname => 'SysSyn Data');


SELECT * FROM "Processor Schema"."Test Table_Out Group_0_claim"();

SELECT * FROM "Processor Schema"."Test Table_Out Group_0_pull"();

SELECT * FROM "Processor Schema"."Test Table_Out Group_0_process"();

SELECT * FROM "Processor Schema"."Test Table_Out Group_0_push_status"();


SELECT dblink_exec('SysSyn Data', 'ROLLBACK');
SELECT dblink_disconnect('SysSyn Data');

ROLLBACK;
