CREATE EXTENSION sys_syn;

CREATE SCHEMA user_data
    AUTHORIZATION postgres;

CREATE TABLE user_data.test_table (
        test_table_id integer NOT NULL,
        test_table_text text,
        CONSTRAINT test_table_pid PRIMARY KEY (test_table_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in');

DO $$BEGIN
        EXECUTE sys_syn.in_table_add_sql('user_data.test_table'::regclass, 'in');
END$$;

INSERT INTO user_data.test_table(
        test_table_id, test_table_text)
VALUES  (1,              'test_data 1'),
        (2,              'test_data 2'),
        (3,              'test_data 3');

INSERT INTO sys_syn.out_groups_def VALUES ('out');

SELECT sys_syn.out_table_add('user_data', 'test_table', 'out', data_view => TRUE);

SELECT user_data.test_table_pull(FALSE);
SELECT user_data.test_table_out_move();

SELECT id, delta_type, queue_state FROM user_data.test_table_out_queue;

CREATE EXTENSION dblink;



CREATE TABLE public.test_data (
        test_data_id integer NOT NULL,
        test_data_text text,
        CONSTRAINT test_data_pid PRIMARY KEY (test_data_id));

INSERT INTO sys_syn.in_groups_def VALUES ('in_group');

DO $$BEGIN
        EXECUTE sys_syn.in_table_add_sql('public.test_data'::regclass, 'in_group');
END$$;

INSERT INTO public.test_data(
        test_data_id, test_data_text)
VALUES  (1,              'test_data 1'),
        (2,              'test_data 2'),
        (3,              'test_data 3');

INSERT INTO sys_syn.out_groups_def VALUES ('group');

SELECT sys_syn.out_table_add('public', 'test_data', 'group', data_view => TRUE);

SELECT public.test_data_pull(FALSE);
SELECT public.test_data_group_move();

SELECT id, delta_type, queue_state FROM public.test_data_group_queue;



CREATE SCHEMA "User Data"
    AUTHORIZATION postgres;

CREATE TABLE "User Data"."Test Table" (
        "Test Table ID" integer NOT NULL,
        "Test Table Text" text,
        CONSTRAINT "Test Table_pid" PRIMARY KEY ("Test Table ID"));

INSERT INTO sys_syn.in_groups_def VALUES ('In Group');

SELECT sys_syn.in_table_add_sql('"User Data"."Test Table"'::regclass, 'In Group');

DO $$BEGIN
        EXECUTE sys_syn.in_table_add_sql('"User Data"."Test Table"'::regclass, 'In Group');
END$$;

INSERT INTO "User Data"."Test Table"(
        "Test Table ID", "Test Table Text")
VALUES (1,              'test_data v1');

INSERT INTO sys_syn.out_groups_def VALUES ('Out Group');

DO $$BEGIN
        EXECUTE sys_syn.out_table_add_sql('"User Data"'::regnamespace, 'Test Table', 'Out Group', data_view => TRUE);
END$$;

SELECT "User Data"."Test Table_pull"(FALSE);
SELECT "User Data"."Test Table_Out Group_move"();



CREATE TABLE user_data.test_table_array (
        test_table_array_id integer NOT NULL,
        test_table_array_updated timestamp with time zone,
        test_table_array_text text,
        CONSTRAINT test_table_array_pid PRIMARY KEY (test_table_array_id, test_table_array_updated));

SELECT sys_syn.in_table_add (
                'user_data',
                'test_table_array',
                'in',
                NULL,
                ARRAY[
                       $COL$("test_table_array_id","integer",ID,"in_source.test_table_array_id",,,,)$COL$,
                       $COL$("test_table_array_updated","timestamp with time zone",Attribute,"in_source.test_table_array_updated",1,,,)$COL$,
                       $COL$("test_table_array_text","text",Attribute,"in_source.test_table_array_text",,,,)$COL$
                ]::sys_syn.create_in_column[],
                'user_data.test_table_array',
                NULL
        );

INSERT INTO user_data.test_table_array(
        test_table_array_id, test_table_array_updated,             test_table_array_text)
VALUES  (1,              '2009-01-02 03:04:05-00',       'test_data1 v1'),
        (1,              '2010-01-02 03:04:05-00',       'test_data1 v2'),
        (2,              '2011-01-02 03:04:05-00',       'test_data2 v1'),
        (2,              '2012-01-02 03:04:05-00',       'test_data2 v2');

SELECT sys_syn.out_table_add('user_data', 'test_table_array', 'out', data_view => TRUE);

SELECT user_data.test_table_array_pull(FALSE);
SELECT user_data.test_table_array_out_move();

SELECT * FROM user_data.test_table_array_out_queue_data;
