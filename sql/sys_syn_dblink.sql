SET client_min_messages = warning;

CREATE SCHEMA sys_syn_dblink;
COMMENT ON SCHEMA sys_syn_dblink IS '';

CREATE TYPE sys_syn_dblink.delta_type AS ENUM (
        'Add',
        'Change',
        'Delete');
COMMENT ON TYPE sys_syn_dblink.delta_type IS '';

CREATE TYPE sys_syn_dblink.in_column_type AS ENUM (
        'Id',
        'Attribute',
        'NoDiff',
        'TransIdIn'
);
COMMENT ON TYPE sys_syn_dblink.in_column_type IS '';

CREATE TYPE sys_syn_dblink.column_position_method AS ENUM (
        'Here',
        'Column',
        'InColumnType');
COMMENT ON TYPE sys_syn_dblink.column_position_method IS '';

CREATE TYPE sys_syn_dblink.processed_status AS (
        hold_reason_id          integer,
        hold_reason_text        text,
        queue_priority          smallint,
        processed_time          timestamp with time zone);
COMMENT ON TYPE sys_syn_dblink.processed_status IS '';

CREATE TYPE sys_syn_dblink.create_put_column AS (
        column_name             text,
        data_type               text,
        in_column_type          sys_syn_dblink.in_column_type,
        value_expression        text,
        array_order             smallint,
        pos_method              sys_syn_dblink.column_position_method,
        pos_before              boolean,
        pos_ref_column_names_like text[],
        pos_in_column_type      sys_syn_dblink.in_column_type
);
COMMENT ON TYPE sys_syn_dblink.create_put_column IS '';

CREATE TYPE sys_syn_dblink.create_proc_column AS (
        column_name             text,
        data_type               text,
        in_column_type          sys_syn_dblink.in_column_type,
        array_order             smallint
);
COMMENT ON TYPE sys_syn_dblink.create_proc_column IS '';

CREATE TYPE sys_syn_dblink.put_code_sql AS (
        declarations_sql       text,
        logic_sql              text
);
COMMENT ON TYPE sys_syn_dblink.put_code_sql IS '';

CREATE TYPE sys_syn_dblink.exception_trap AS (
        when_conditions text[],
        statements_sql  text);
COMMENT ON TYPE sys_syn_dblink.exception_trap IS '';


CREATE TABLE sys_syn_dblink.out_groups_def (
        cluster_id              text NOT NULL,
        out_group_id            text NOT NULL,
        parent_cluster_id       text,
        parent_out_group_id     text,
        rule_group_ids          text[],
        comments                text DEFAULT '' NOT NULL
);
COMMENT ON TABLE sys_syn_dblink.out_groups_def IS '';
ALTER TABLE ONLY sys_syn_dblink.out_groups_def
        ADD CONSTRAINT out_groups_def_pkey PRIMARY KEY (cluster_id, out_group_id);
ALTER TABLE sys_syn_dblink.out_groups_def
        ADD CONSTRAINT out_groups_def_parent_fkey FOREIGN KEY (parent_cluster_id, parent_out_group_id)
                REFERENCES sys_syn_dblink.out_groups_def (cluster_id, out_group_id) MATCH SIMPLE
                ON UPDATE RESTRICT ON DELETE NO ACTION;

CREATE TABLE sys_syn_dblink.in_groups_def (
        cluster_id              text NOT NULL,
        in_group_id             text NOT NULL,
        parent_cluster_id       text,
        parent_in_group_id      text,
        rule_group_ids          text[],
        comments                text DEFAULT '' NOT NULL
);
COMMENT ON TABLE sys_syn_dblink.in_groups_def IS '';
ALTER TABLE ONLY sys_syn_dblink.in_groups_def
        ADD CONSTRAINT in_groups_def_pkey PRIMARY KEY (cluster_id, in_group_id);
ALTER TABLE sys_syn_dblink.in_groups_def
        ADD CONSTRAINT in_groups_def_parent_fkey FOREIGN KEY (parent_cluster_id, parent_in_group_id)
                REFERENCES sys_syn_dblink.in_groups_def (cluster_id, in_group_id) MATCH SIMPLE
                ON UPDATE RESTRICT ON DELETE NO ACTION;

CREATE TABLE sys_syn_dblink.put_groups_def (
        put_group_id            text NOT NULL,
        parent_put_group_id     text,
        rule_group_ids          text[],
        comments                text DEFAULT '' NOT NULL
);
COMMENT ON TABLE sys_syn_dblink.put_groups_def IS '';
ALTER TABLE ONLY sys_syn_dblink.put_groups_def
        ADD CONSTRAINT put_groups_def_pkey PRIMARY KEY (put_group_id);
ALTER TABLE sys_syn_dblink.put_groups_def
        ADD CONSTRAINT put_groups_def_parent_fkey FOREIGN KEY (parent_put_group_id)
                REFERENCES sys_syn_dblink.put_groups_def (put_group_id) MATCH SIMPLE
                ON UPDATE RESTRICT ON DELETE NO ACTION;

CREATE TABLE sys_syn_dblink.table_types_def (
        table_type_id                   text NOT NULL,
        attributes_array                boolean NOT NULL,
        table_create_proc_schema        regnamespace NOT NULL,
        table_create_proc_name          text NOT NULL,
        table_drop_proc_schema          regnamespace NOT NULL,
        table_drop_proc_name            text NOT NULL,
        put_sql_proc_schema             regnamespace NOT NULL,
        put_sql_proc_name               text NOT NULL,
        initial_table_settings          hstore DEFAULT ''::hstore NOT NULL,
        comments                        text DEFAULT '' NOT NULL,
        CONSTRAINT table_types_def_pkey PRIMARY KEY (table_type_id, attributes_array)
);
COMMENT ON TABLE sys_syn_dblink.table_types_def IS '';

CREATE TABLE sys_syn_dblink.put_table_transforms (
        rule_group_id           text,
        priority                smallint NOT NULL,
        proc_table_id_like      text,
        cluster_id_like         text,
        in_table_id_like        text,
        out_group_id_like       text,
        in_group_id_like        text,
        put_group_id_like       text,
        proc_schema_like        text,
        put_schema_like         text,
        put_table_name_like     text,
        table_type_id_like      text,
        attributes_array        boolean,
        dblink_connname_like    text,
        remote_schema_like      text,
/*      queue_id                smallint,*/
        new_proc_schema         text,
        new_put_schema          text,
        new_put_table_name      text,
        new_table_type_id       text,
        table_settings          hstore DEFAULT ''::hstore NOT NULL,
        new_dblink_connname     text,
--        new_hold_cache_min_rows int,
        new_records_per_proc    int,
        new_remote_sql_len_max  int,
/*      new_queue_id            smallint,*/
        add_columns             sys_syn_dblink.create_put_column[] DEFAULT ARRAY[]::sys_syn_dblink.create_put_column[] NOT NULL,
        omit                    boolean,
        final_ids               text[] DEFAULT '{}'::text[] NOT NULL,
        final_rule              boolean DEFAULT FALSE NOT NULL,
        comments                text DEFAULT '' NOT NULL
);
COMMENT ON TABLE sys_syn_dblink.put_table_transforms IS '';
ALTER TABLE sys_syn_dblink.put_table_transforms
        ADD CONSTRAINT priority_disallow_sign CHECK (priority >= 0);
CREATE UNIQUE INDEX ON sys_syn_dblink.put_table_transforms (
        priority, proc_table_id_like, cluster_id_like, in_table_id_like, out_group_id_like, in_group_id_like, proc_schema_like,
        put_schema_like, put_table_name_like, table_type_id_like, attributes_array, dblink_connname_like, remote_schema_like/*,
        queue_id*/);

CREATE TABLE sys_syn_dblink.put_column_transforms (
        rule_group_id           text,
        priority                smallint NOT NULL,
        proc_table_id_like      text,
        cluster_id_like         text,
        in_table_id_like        text,
        out_group_id_like       text,
        in_group_id_like        text,
        put_group_id_like       text,
        proc_schema_like        text,
        put_schema_like         text,
        put_table_name_like     text,
        table_type_id_like      text,
        attributes_array        boolean,
        dblink_connname_like    text,
        remote_schema_like      text,
/*      queue_id                smallint,*/
        in_column_type          sys_syn_dblink.in_column_type,
        column_name_like        text,
        data_type_like          text,
        primary_in_table_id_like text,
        primary_column_name_like text,
        new_data_type           text,
        new_in_column_type      sys_syn_dblink.in_column_type,
        new_column_name         text,
        pos_method              sys_syn_dblink.column_position_method,
        pos_before              boolean,
        pos_ref_column_names_like text[],
        pos_in_column_type      sys_syn_dblink.in_column_type,
        variable_name           text,
        variable_delta_types    sys_syn_dblink.delta_type[],
        variable_exception_traps sys_syn_dblink.exception_trap[],
        expression              text,
        add_columns             sys_syn_dblink.create_put_column[] DEFAULT ARRAY[]::sys_syn_dblink.create_put_column[] NOT NULL,
        omit                    boolean,
        final_ids               text[] DEFAULT '{}'::text[] NOT NULL,
        final_rule              boolean DEFAULT FALSE NOT NULL,
        delta_types             sys_syn_dblink.delta_type[] DEFAULT ARRAY['Add','Change','Delete']::sys_syn_dblink.delta_type[],
        comments                text DEFAULT '' NOT NULL
);
COMMENT ON TABLE sys_syn_dblink.put_column_transforms IS '';
ALTER TABLE sys_syn_dblink.put_column_transforms
        ADD CONSTRAINT priority_disallow_sign CHECK (priority >= 0);
CREATE UNIQUE INDEX ON sys_syn_dblink.put_column_transforms (
        priority, proc_table_id_like, cluster_id_like, out_group_id_like, in_group_id_like, proc_schema_like, put_schema_like,
        put_table_name_like, table_type_id_like, attributes_array, dblink_connname_like, remote_schema_like, /*queue_id,*/
        in_column_type, column_name_like, data_type_like);

CREATE TABLE sys_syn_dblink.proc_tables_def (
        proc_schema             regnamespace    NOT NULL,
        proc_table_id           text            NOT NULL,
        cluster_id              text            NOT NULL,
        dblink_connname         text            NOT NULL,
        remote_schema           text            NOT NULL,
        in_table_id             text            NOT NULL,
        in_group_id             text            NOT NULL,
        attributes_array        boolean         NOT NULL,
        out_group_id            text            NOT NULL,
        put_group_id            text            NOT NULL,
        put_schema              regnamespace    NOT NULL,
        put_table_name          text            NOT NULL,
        table_type_id           text            NOT NULL,
        table_settings          hstore          NOT NULL,
--      hold_cache_min_rows     int             NOT NULL,
        records_per_proc        int             NOT NULL,
        remote_sql_len_max      int             NOT NULL,
        queue_count             smallint,
/*      queue_id                smallint,*/
        comments                text DEFAULT '' NOT NULL,
        CONSTRAINT proc_tables_def_pkey PRIMARY KEY (proc_table_id)
);
COMMENT ON TABLE sys_syn_dblink.proc_tables_def IS '';
ALTER TABLE ONLY sys_syn_dblink.proc_tables_def
        ADD CONSTRAINT proc_tables_def_in_group_id_fkey FOREIGN KEY (cluster_id, in_group_id)
                REFERENCES sys_syn_dblink.in_groups_def(cluster_id, in_group_id)
                ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY sys_syn_dblink.proc_tables_def
        ADD CONSTRAINT proc_tables_def_out_group_id_fkey FOREIGN KEY (cluster_id, out_group_id)
                REFERENCES sys_syn_dblink.out_groups_def(cluster_id, out_group_id)
                ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY sys_syn_dblink.proc_tables_def
        ADD CONSTRAINT proc_tables_def_put_group_id_fkey FOREIGN KEY (put_group_id)
                REFERENCES sys_syn_dblink.put_groups_def(put_group_id)
                ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE ONLY sys_syn_dblink.proc_tables_def
        ADD CONSTRAINT proc_tables_def_table_type_id_fkey FOREIGN KEY (table_type_id, attributes_array)
                REFERENCES sys_syn_dblink.table_types_def(table_type_id, attributes_array)
                ON UPDATE RESTRICT ON DELETE RESTRICT;

CREATE TABLE sys_syn_dblink.proc_partitions_def (
        proc_table_id           text            NOT NULL,
        partition_id            smallint        NOT NULL,
        comments                text            NOT NULL DEFAULT ''
);
COMMENT ON TABLE sys_syn_dblink.proc_partitions_def IS '';
ALTER TABLE ONLY sys_syn_dblink.proc_partitions_def
        ADD CONSTRAINT proc_partitions_def_pkey PRIMARY KEY (proc_table_id, partition_id);
ALTER TABLE sys_syn_dblink.proc_partitions_def
  ADD CONSTRAINT proc_partitions_def_proc_table_id_fkey FOREIGN KEY (proc_table_id)
      REFERENCES sys_syn_dblink.proc_tables_def (proc_table_id) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

CREATE TABLE sys_syn_dblink.proc_workers_def (
        proc_table_id           text            NOT NULL,
        partition_id            smallint        NOT NULL,
        worker_id               smallint        NOT NULL,
        logged_tablespace       name,
        unlogged_tablespace     name,
        node_id                 text,
        queue_id                smallint,
        comments                text            NOT NULL DEFAULT ''
);
COMMENT ON TABLE sys_syn_dblink.proc_workers_def IS '';
ALTER TABLE ONLY sys_syn_dblink.proc_workers_def
        ADD CONSTRAINT proc_workers_def_pkey PRIMARY KEY (proc_table_id, worker_id);
ALTER TABLE sys_syn_dblink.proc_workers_def
  ADD CONSTRAINT proc_workers_def_proc_table_id_fkey FOREIGN KEY (proc_table_id, partition_id)
      REFERENCES sys_syn_dblink.proc_partitions_def (proc_table_id, partition_id) MATCH SIMPLE
      ON UPDATE RESTRICT ON DELETE RESTRICT;

CREATE TABLE sys_syn_dblink.proc_columns_def (
        proc_table_id   text            NOT NULL,
        column_name     text            NOT NULL,
        in_column_type  sys_syn_dblink.in_column_type,
        format_type     text            NOT NULL,
        column_ordinal  smallint        NOT NULL,
        array_order     smallint,
        CONSTRAINT proc_columns_def_pkey PRIMARY KEY (proc_table_id, column_name)
);
COMMENT ON TABLE sys_syn_dblink.proc_columns_def IS '';

CREATE TABLE sys_syn_dblink.proc_foreign_keys (
        cluster_id              text    NOT NULL,
        foreign_proc_table_id   text    NOT NULL,
        foreign_key_index       smallint NOT NULL,
        primary_in_table_id     text    NOT NULL,
        foreign_column_name     text    NOT NULL,
        primary_column_name     text    NOT NULL
);
COMMENT ON TABLE sys_syn_dblink.proc_foreign_keys IS '';
ALTER TABLE ONLY sys_syn_dblink.proc_foreign_keys
        ADD CONSTRAINT proc_foreign_keys_pkey
                PRIMARY KEY (cluster_id, foreign_proc_table_id, foreign_key_index, foreign_column_name);
ALTER TABLE sys_syn_dblink.proc_foreign_keys
        ADD CONSTRAINT proc_foreign_keys_primary_fkey FOREIGN KEY (foreign_proc_table_id, foreign_column_name)
                REFERENCES sys_syn_dblink.proc_columns_def (proc_table_id, column_name)
                MATCH SIMPLE ON UPDATE RESTRICT ON DELETE RESTRICT;
ALTER TABLE sys_syn_dblink.proc_foreign_keys
        ADD CONSTRAINT proc_foreign_keys_proc_table_id_fkey FOREIGN KEY (foreign_proc_table_id)
                REFERENCES sys_syn_dblink.proc_tables_def (proc_table_id)
                MATCH SIMPLE ON UPDATE RESTRICT ON DELETE RESTRICT;



INSERT INTO sys_syn_dblink.table_types_def (
        table_type_id,                          attributes_array,
        table_create_proc_schema,       table_create_proc_name,         table_drop_proc_schema, table_drop_proc_name,
        put_sql_proc_schema,                    put_sql_proc_name,
        initial_table_settings)
VALUES ('sys_syn-direct',                       false,
        'sys_syn_dblink',               'table_create_sql_direct',      'sys_syn_dblink',       'table_drop_sql_direct',
        'sys_syn_dblink',                       'put_sql_direct',
        ''),(
        'sys_syn-direct',                       true,
        'sys_syn_dblink',               'table_create_sql_direct_array','sys_syn_dblink',       'table_drop_sql_direct_array',
        'sys_syn_dblink',                       'put_sql_direct_array',
        ''),(
        'sys_syn-temporal',                     true,
        'sys_syn_dblink',               'table_create_sql_temporal',    'sys_syn_dblink',       'table_drop_sql_temporal',
        'sys_syn_dblink',                       'put_sql_temporal_array',
        $$
                sys_syn.temporal.active_table_name      => %1,
                sys_syn.temporal.history_table_name     => %1_history,
                sys_syn.temporal.range_1.column_name    => %1
        $$),(
        'sys_syn-temporal',                     false,
        'sys_syn_dblink',               'table_create_sql_temporal',    'sys_syn_dblink',       'table_drop_sql_temporal',
        'sys_syn_dblink',                       'put_sql_temporal',
        $$
                sys_syn.temporal.active_table_name      => %1,
                sys_syn.temporal.history_table_name     => %1_history,
                sys_syn.temporal.range_1.column_name    => %1
        $$),(
        'sys_syn-bitemporal',                   true,
        'sys_syn_dblink',               'table_create_sql_bitemporal',  'sys_syn_dblink',       'table_drop_sql_bitemporal',
        'sys_syn_dblink',                       'put_sql_bitemporal_array',
        $$
                sys_syn.bitemporal.active_table_name    => %1,
                sys_syn.bitemporal.immutable_table_name => %1_immutable,
                sys_syn.bitemporal.history_table_name   => %1_history,
                sys_syn.bitemporal.current_view_name    => %1_current,
                sys_syn.bitemporal.active_view_name     => %1_active,
                sys_syn.bitemporal.range_1.column_name          => %1,
                sys_syn.bitemporal.range_1.lower.column_name    => %1,
                sys_syn.bitemporal.range_2.column_name          => %1,
                sys_syn.bitemporal.range_2_active.column_name   => %1
        $$);



CREATE FUNCTION sys_syn_dblink.table_types_def_check_new () RETURNS TRIGGER AS $$
BEGIN
        IF NEW.table_type_id LIKE 'sys_syn%' THEN
                RAISE EXCEPTION 'Rules starting with sys_syn are reserved for the sys_syn_dblink extension.'
                USING HINT = 'Choose a table_type_id that does not begin with sys_syn.';
        END IF;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION sys_syn_dblink.table_types_def_check_new() IS '';

CREATE FUNCTION sys_syn_dblink.table_types_def_check_old () RETURNS TRIGGER AS $$
BEGIN
        IF OLD.table_type_id LIKE 'sys_syn%' THEN
                RAISE EXCEPTION 'Rules starting with sys_syn are reserved for the sys_syn_dblink extension.'
                USING HINT = 'Choose a table_type_id that does not begin with sys_syn.';
        END IF;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION sys_syn_dblink.table_types_def_check_old() IS '';

CREATE CONSTRAINT TRIGGER table_types_def_check_new
        AFTER INSERT OR UPDATE ON sys_syn_dblink.table_types_def
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE PROCEDURE sys_syn_dblink.table_types_def_check_new();

CREATE CONSTRAINT TRIGGER table_types_def_check_old
        AFTER DELETE ON sys_syn_dblink.table_types_def
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE PROCEDURE sys_syn_dblink.table_types_def_check_old();
CREATE FUNCTION sys_syn_dblink.put_table_transforms_check_new () RETURNS TRIGGER AS $$
BEGIN
        IF NEW.rule_group_id LIKE 'sys_syn%' THEN
                RAISE EXCEPTION 'Rules starting with sys_syn are reserved for the sys_syn_dblink extension.'
                USING HINT = 'Choose a rule_group_id that does not begin with sys_syn.';
        END IF;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION sys_syn_dblink.put_table_transforms_check_new() IS '';

CREATE FUNCTION sys_syn_dblink.put_table_transforms_check_old () RETURNS TRIGGER AS $$
BEGIN
        IF OLD.rule_group_id LIKE 'sys_syn%' THEN
                RAISE EXCEPTION 'Rules starting with sys_syn are reserved for the sys_syn_dblink extension.'
                USING HINT = 'Choose a rule_group_id that does not begin with sys_syn.';
        END IF;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION sys_syn_dblink.put_table_transforms_check_old() IS '';

CREATE CONSTRAINT TRIGGER put_table_transforms_check_new
        AFTER INSERT OR UPDATE ON sys_syn_dblink.put_table_transforms
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE PROCEDURE sys_syn_dblink.put_table_transforms_check_new();

CREATE CONSTRAINT TRIGGER put_table_transforms_check_old
        AFTER DELETE ON sys_syn_dblink.put_table_transforms
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE PROCEDURE sys_syn_dblink.put_table_transforms_check_old();
CREATE FUNCTION sys_syn_dblink.put_column_transforms_check_new () RETURNS TRIGGER AS $$
BEGIN
        IF NEW.rule_group_id LIKE 'sys_syn%' THEN
                RAISE EXCEPTION 'Rules starting with sys_syn are reserved for the sys_syn_dblink extension.'
                USING HINT = 'Choose a rule_group_id that does not begin with sys_syn.';
        END IF;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION sys_syn_dblink.put_column_transforms_check_new() IS '';

CREATE FUNCTION sys_syn_dblink.put_column_transforms_check_old () RETURNS TRIGGER AS $$
BEGIN
        IF OLD.rule_group_id LIKE 'sys_syn%' THEN
                RAISE EXCEPTION 'Rules starting with sys_syn are reserved for the sys_syn_dblink extension.'
                USING HINT = 'Choose a rule_group_id that does not begin with sys_syn.';
        END IF;
        RETURN NULL;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION sys_syn_dblink.put_column_transforms_check_old() IS '';

CREATE CONSTRAINT TRIGGER put_column_transforms_check_new
        AFTER INSERT OR UPDATE ON sys_syn_dblink.put_column_transforms
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE PROCEDURE sys_syn_dblink.put_column_transforms_check_new();

CREATE CONSTRAINT TRIGGER put_column_transforms_check_old
        AFTER DELETE ON sys_syn_dblink.put_column_transforms
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE PROCEDURE sys_syn_dblink.put_column_transforms_check_old();



CREATE FUNCTION sys_syn_dblink.table_primary_key_name (proc_schema regnamespace, table_name text)
        RETURNS text AS
$BODY$
DECLARE
        _index_name text;
BEGIN
        SELECT  index_class.relname
        INTO    _index_name
        FROM    pg_class AS table_class JOIN
                pg_index AS table_index ON
                        table_index.indrelid = table_class.oid JOIN
                pg_class AS index_class ON
                        index_class.oid = table_index.indexrelid JOIN
                pg_index AS index_index ON
                        index_index.indexrelid = index_class.oid AND
                        index_index.indisprimary
        WHERE   table_class.relnamespace        = table_primary_key_name.proc_schema AND
                table_class.relname             = table_primary_key_name.table_name;

        IF _index_name IS NULL THEN
                RAISE EXCEPTION 'Cannot find a primary key in proc_schema ''%'' table_name ''%''.', proc_schema::text, table_name
                USING HINT = 'Check the proc_schema and table names and that the table has a primary key.';
        END IF;

        RETURN _index_name;
END
$BODY$
  LANGUAGE plpgsql STABLE
  COST 10;
COMMENT ON FUNCTION sys_syn_dblink.table_primary_key_name(proc_schema regnamespace, table_name text)
  IS '';

CREATE FUNCTION sys_syn_dblink.proc_columns_get (
        proc_table_id   text)
  RETURNS sys_syn_dblink.create_proc_column[] AS
$BODY$
DECLARE
        _proc_table_def                 sys_syn_dblink.proc_tables_def;
        _column_type_id_name            TEXT;
        _column_type_attr_name          TEXT;
        _column_type_nodiff_name        TEXT;
        _column_name                    TEXT;
        _format_type                    TEXT;
        _in_column_type                 sys_syn_dblink.in_column_type;
        _return_column                  sys_syn_dblink.create_proc_column;
        _return_columns                 sys_syn_dblink.create_proc_column[];
BEGIN
        _proc_table_def := (
                SELECT  proc_tables_def
                FROM    sys_syn_dblink.proc_tables_def
                WHERE   proc_tables_def.proc_table_id = proc_columns_get.proc_table_id);

        IF _proc_table_def IS NULL THEN
                RAISE EXCEPTION 'Cannot find proc_table_id ''%''', proc_columns_get.proc_table_id
                USING HINT = 'Check the proc_tables_def table.';
        END IF;

        _column_type_id_name            := _proc_table_def.proc_table_id || '_proc_id';
        _column_type_attr_name          := _proc_table_def.proc_table_id || '_proc_attributes';
        _column_type_nodiff_name        := _proc_table_def.proc_table_id || '_proc_no_diff';

        _return_columns                 := ARRAY[]::sys_syn_dblink.create_proc_column[];

        IF (    SELECT  COUNT(*)
                FROM    pg_catalog.pg_namespace JOIN
                        pg_catalog.pg_class ON
                                pg_class.relnamespace = pg_namespace.oid
                WHERE   ('"'||pg_namespace.nspname||'"')::regnamespace =  _proc_table_def.proc_schema AND
                        pg_class.relname = _column_type_id_name) = 0 THEN
                RAISE EXCEPTION 'Cannot find type ''%''.''%''.', _proc_table_def.proc_schema::text, _column_type_id_name
                USING HINT = 'Check the proc_schema and table names.';
        END IF;

        FOR     _column_name,           _format_type,
                _in_column_type IN
        SELECT  pg_attribute.attname,   format_type(pg_attribute.atttypid, pg_attribute.atttypmod),
                CASE pg_class.relname
                        WHEN _column_type_id_name       THEN 'Id'::sys_syn_dblink.in_column_type
                        WHEN _column_type_attr_name     THEN 'Attribute'::sys_syn_dblink.in_column_type
                        WHEN _column_type_nodiff_name   THEN 'NoDiff'::sys_syn_dblink.in_column_type
                END
        FROM    pg_catalog.pg_namespace JOIN
                pg_catalog.pg_class ON
                        pg_class.relnamespace = pg_namespace.oid JOIN
                pg_catalog.pg_attribute ON
                        (pg_attribute.attrelid = pg_class.oid)
        WHERE   ('"'||pg_namespace.nspname||'"')::regnamespace =  _proc_table_def.proc_schema AND
                pg_class.relname IN (_column_type_id_name, _column_type_attr_name, _column_type_nodiff_name) AND
                pg_attribute.attnum > 0 AND
                NOT pg_attribute.attisdropped
        ORDER BY CASE pg_class.relname
                        WHEN _column_type_id_name       THEN 1
                        WHEN _column_type_attr_name     THEN 2
                        WHEN _column_type_nodiff_name   THEN 3
                        ELSE 4
                END,
                pg_attribute.attnum
        LOOP

                _return_column.column_name          := _column_name;
                _return_column.data_type            := _format_type;
                _return_column.in_column_type       := _in_column_type;

                SELECT  proc_columns_def.array_order
                INTO    _return_column.array_order
                FROM    sys_syn_dblink.proc_columns_def
                WHERE   proc_columns_def.proc_table_id    = _proc_table_def.proc_table_id AND
                        proc_columns_def.column_name      = _column_name;

                _return_columns := array_append(_return_columns, _return_column);
        END LOOP;

        RETURN _return_columns;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.proc_columns_get(text)
  IS '';

CREATE FUNCTION sys_syn_dblink.rule_group_ids_get (_proc_table_def sys_syn_dblink.proc_tables_def)
  RETURNS text[] AS
$BODY$
DECLARE
        _rule_group_ids                 text[];
BEGIN
        _rule_group_ids := COALESCE(
                (
                        WITH RECURSIVE all_transform_rule_group_ids(parent_cluster_id, parent_in_group_id, rule_group_ids) AS (
                                SELECT  in_groups_def.parent_cluster_id,
                                        in_groups_def.parent_in_group_id,
                                        in_groups_def.rule_group_ids
                                FROM    sys_syn_dblink.in_groups_def
                                WHERE   in_groups_def.cluster_id        = _proc_table_def.cluster_id AND
                                        in_groups_def.in_group_id       = _proc_table_def.in_group_id
                                UNION ALL
                                SELECT  in_groups_def.parent_cluster_id,
                                        in_groups_def.parent_in_group_id,
                                        in_groups_def.rule_group_ids ||
                                                all_transform_rule_group_ids.rule_group_ids
                                FROM    sys_syn_dblink.in_groups_def, all_transform_rule_group_ids
                                WHERE   in_groups_def.cluster_id        = all_transform_rule_group_ids.parent_cluster_id AND
                                        in_groups_def.in_group_id       = all_transform_rule_group_ids.parent_in_group_id
                        )
                        SELECT  rule_group_ids
                        FROM    all_transform_rule_group_ids
                        WHERE   parent_cluster_id       IS NULL AND
                                parent_in_group_id      IS NULL
                ),
                '{}'::text[]
        );

        _rule_group_ids := _rule_group_ids || COALESCE(
                (
                        WITH RECURSIVE all_transform_rule_group_ids(parent_cluster_id, parent_out_group_id, rule_group_ids) AS(
                                SELECT  out_groups_def.parent_cluster_id,
                                        out_groups_def.parent_out_group_id,
                                        out_groups_def.rule_group_ids
                                FROM    sys_syn_dblink.out_groups_def
                                WHERE   out_groups_def.cluster_id       = _proc_table_def.cluster_id AND
                                        out_groups_def.out_group_id     = _proc_table_def.out_group_id
                                UNION ALL
                                SELECT  out_groups_def.parent_cluster_id,
                                        out_groups_def.parent_out_group_id,
                                        out_groups_def.rule_group_ids ||
                                                all_transform_rule_group_ids.rule_group_ids
                                FROM    sys_syn_dblink.out_groups_def, all_transform_rule_group_ids
                                WHERE   out_groups_def.cluster_id       = all_transform_rule_group_ids.parent_cluster_id AND
                                        out_groups_def.out_group_id     = all_transform_rule_group_ids.parent_out_group_id
                        )
                        SELECT  rule_group_ids
                        FROM    all_transform_rule_group_ids
                        WHERE   parent_cluster_id       IS NULL AND
                                parent_out_group_id     IS NULL
                ),
                '{}'::text[]
        );

        _rule_group_ids := _rule_group_ids || COALESCE(
                (
                        WITH RECURSIVE all_transform_rule_group_ids(parent_put_group_id, rule_group_ids) AS (
                                SELECT  put_groups_def.parent_put_group_id,
                                        put_groups_def.rule_group_ids
                                FROM    sys_syn_dblink.put_groups_def
                                WHERE   put_groups_def.put_group_id = _proc_table_def.put_group_id
                                UNION ALL
                                SELECT  put_groups_def.parent_put_group_id,
                                        put_groups_def.rule_group_ids ||
                                                all_transform_rule_group_ids.rule_group_ids
                                FROM    sys_syn_dblink.put_groups_def, all_transform_rule_group_ids
                                WHERE   put_groups_def.put_group_id = all_transform_rule_group_ids.parent_put_group_id
                        )
                        SELECT  rule_group_ids
                        FROM    all_transform_rule_group_ids
                        WHERE   parent_put_group_id IS NULL
                ),
                '{}'::text[]
        );

        RETURN _rule_group_ids;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.rule_group_ids_get(sys_syn_dblink.proc_tables_def)
  IS '';

CREATE FUNCTION sys_syn_dblink.put_columns_get (
        proc_table_id   text,
        add_columns     sys_syn_dblink.create_put_column[])
  RETURNS sys_syn_dblink.create_put_column[] AS
$BODY$
DECLARE
        _proc_table_def                 sys_syn_dblink.proc_tables_def;
        _rule_group_ids                 text[];
        _column_type_id_name            TEXT;
        _column_type_attr_name          TEXT;
        _column_type_nodiff_name        TEXT;
        _column_name                    TEXT;
        _format_type                    TEXT;
        _in_column_type                 sys_syn_dblink.in_column_type;
        _put_column_transform           sys_syn_dblink.put_column_transforms%ROWTYPE;
        _add_columns                    sys_syn_dblink.create_put_column[];
        _final_ids                      text[];
        _omit                           boolean;
        _last_priority                  smallint;
        _return_column                  sys_syn_dblink.create_put_column;
        _return_columns                 sys_syn_dblink.create_put_column[];
        _return_columns_id              sys_syn_dblink.create_put_column[];
        _return_columns_attr            sys_syn_dblink.create_put_column[];
        _return_columns_nodiff          sys_syn_dblink.create_put_column[];
        _variable_index                 smallint := 0;
BEGIN
        _proc_table_def := (
                SELECT  proc_tables_def
                FROM    sys_syn_dblink.proc_tables_def
                WHERE   proc_tables_def.proc_table_id = put_columns_get.proc_table_id);

        IF _proc_table_def IS NULL THEN
                RAISE EXCEPTION 'Cannot find proc_table_id ''%''.', put_columns_get.proc_table_id
                USING HINT = 'Check the proc_tables_def table.';
        END IF;

        _rule_group_ids := sys_syn_dblink.rule_group_ids_get(_proc_table_def);

        _column_type_id_name            := _proc_table_def.proc_table_id || '_proc_id';
        _column_type_attr_name          := _proc_table_def.proc_table_id || '_proc_attributes';
        _column_type_nodiff_name        := _proc_table_def.proc_table_id || '_proc_no_diff';

        _return_columns                 := ARRAY[]::sys_syn_dblink.create_put_column[];
        _return_columns_id              := ARRAY[]::sys_syn_dblink.create_put_column[];
        _return_columns_attr            := ARRAY[]::sys_syn_dblink.create_put_column[];
        _return_columns_nodiff          := ARRAY[]::sys_syn_dblink.create_put_column[];

        IF (    SELECT  COUNT(*)
                FROM    pg_catalog.pg_namespace JOIN
                        pg_catalog.pg_class ON
                                pg_class.relnamespace = pg_namespace.oid
                WHERE   ('"'||pg_namespace.nspname||'"')::regnamespace =  _proc_table_def.proc_schema AND
                        pg_class.relname = _column_type_id_name) = 0 THEN
                RAISE EXCEPTION 'Cannot find type ''%''.''%''.', _proc_table_def.proc_schema::text, _column_type_id_name
                USING HINT = 'Check the proc_schema and table names.';
        END IF;

        CREATE TEMPORARY TABLE put_sql_expressions_temp (
                in_column_type  sys_syn_dblink.in_column_type NOT NULL,
                delta_types     sys_syn_dblink.delta_type[] NOT NULL,
                variable_index  smallint NOT NULL,
                variable_name   text NOT NULL,
                data_type       text NOT NULL,
                expression      text NOT NULL,
                exception_traps sys_syn_dblink.exception_trap[] NOT NULL,
                declared        boolean DEFAULT FALSE NOT NULL
        ) ON COMMIT DROP;

        CREATE TEMPORARY TABLE primary_columns_temp (
                primary_in_table_id     text NOT NULL,
                primary_column_name     text NOT NULL)
        ON COMMIT DROP;

        FOR     _column_name,           _format_type,
                _in_column_type IN
        SELECT  pg_attribute.attname,   format_type(pg_attribute.atttypid, pg_attribute.atttypmod),
                CASE pg_class.relname
                        WHEN _column_type_id_name       THEN 'Id'::sys_syn_dblink.in_column_type
                        WHEN _column_type_attr_name     THEN 'Attribute'::sys_syn_dblink.in_column_type
                        WHEN _column_type_nodiff_name   THEN 'NoDiff'::sys_syn_dblink.in_column_type
                END
        FROM    pg_catalog.pg_namespace JOIN
                pg_catalog.pg_class ON
                        pg_class.relnamespace = pg_namespace.oid JOIN
                pg_catalog.pg_attribute ON
                        (pg_attribute.attrelid = pg_class.oid)
        WHERE   ('"'||pg_namespace.nspname||'"')::regnamespace =  _proc_table_def.proc_schema AND
                pg_class.relname IN (_column_type_id_name, _column_type_attr_name, _column_type_nodiff_name) AND
                pg_attribute.attnum > 0 AND
                NOT pg_attribute.attisdropped
        ORDER BY CASE pg_class.relname
                        WHEN _column_type_id_name       THEN 1
                        WHEN _column_type_attr_name     THEN 2
                        WHEN _column_type_nodiff_name   THEN 3
                        ELSE 4
                END,
                pg_attribute.attnum
        LOOP

                _return_column.data_type                := _format_type;
                _return_column.in_column_type           := _in_column_type;
                _return_column.column_name              := _column_name;
                _return_column.pos_method               := 'Here'::sys_syn_dblink.column_position_method;
                _return_column.pos_before               := FALSE;
                _return_column.pos_ref_column_names_like:= NULL;
                _return_column.pos_in_column_type       := NULL;

                DELETE FROM primary_columns_temp;

                INSERT INTO primary_columns_temp
                SELECT  proc_foreign_keys.primary_in_table_id,    proc_foreign_keys.primary_column_name
                FROM    sys_syn_dblink.proc_foreign_keys
                WHERE   proc_foreign_keys.cluster_id            = _proc_table_def.cluster_id AND
                        proc_foreign_keys.foreign_proc_table_id = _proc_table_def.proc_table_id AND
                        proc_foreign_keys.foreign_column_name   = _column_name;

                CASE _in_column_type
                        WHEN 'Id'::sys_syn_dblink.in_column_type THEN
                                _return_column.value_expression := 'id.' || quote_ident(_column_name);
                        WHEN 'Attribute'::sys_syn_dblink.in_column_type THEN
                                IF _proc_table_def.attributes_array THEN
                                        _return_column.value_expression := 'attribute_rows.' || quote_ident(_column_name);
                                ELSE
                                        _return_column.value_expression := 'attributes.' || quote_ident(_column_name);
                                END IF;
                        WHEN 'NoDiff'::sys_syn_dblink.in_column_type THEN
                                _return_column.value_expression := 'no_diff.' || quote_ident(_column_name);
                        ELSE    _return_column.value_expression := quote_ident(_column_name);
                END CASE;

                SELECT  proc_columns_def.array_order
                INTO    _return_column.array_order
                FROM    sys_syn_dblink.proc_columns_def
                WHERE   proc_columns_def.proc_table_id  = _proc_table_def.proc_table_id AND
                        proc_columns_def.column_name    = _column_name;

                _add_columns                            := ARRAY[]::sys_syn_dblink.create_put_column[];
                _final_ids                              := ARRAY[]::TEXT[];
                _omit                                   := FALSE;
                _last_priority                          := -1;

                FOR     _put_column_transform IN
                SELECT  *
                FROM    sys_syn_dblink.put_column_transforms
                WHERE   (       put_column_transforms.rule_group_id IS NULL OR
                                put_column_transforms.rule_group_id = ANY(_rule_group_ids)
                        )
                ORDER BY put_column_transforms.priority
                LOOP

                        IF      (_put_column_transform.proc_table_id_like       IS NULL OR
                                        _proc_table_def.proc_table_id           LIKE _put_column_transform.proc_table_id_like) AND
                                (_put_column_transform.in_table_id_like         IS NULL OR
                                        _proc_table_def.in_table_id             LIKE _put_column_transform.in_table_id_like) AND
                                (_put_column_transform.out_group_id_like        IS NULL OR
                                        _proc_table_def.out_group_id            LIKE _put_column_transform.out_group_id_like) AND
                                (_put_column_transform.in_group_id_like         IS NULL OR
                                        _proc_table_def.in_group_id             LIKE _put_column_transform.in_group_id_like) AND
                                (_put_column_transform.proc_schema_like         IS NULL OR
                                        _proc_table_def.proc_schema::text       LIKE _put_column_transform.proc_schema_like) AND
                                (_put_column_transform.put_schema_like          IS NULL OR
                                        _proc_table_def.put_schema::text        LIKE _put_column_transform.put_schema_like) AND
                                (_put_column_transform.put_table_name_like      IS NULL OR
                                        _proc_table_def.put_table_name          LIKE _put_column_transform.put_table_name_like) AND
                                (_put_column_transform.table_type_id_like       IS NULL OR
                                        _proc_table_def.table_type_id           LIKE _put_column_transform.table_type_id_like) AND
                                (_put_column_transform.attributes_array         IS NULL OR
                                        _proc_table_def.attributes_array        = _put_column_transform.attributes_array) AND
                                (_put_column_transform.cluster_id_like          IS NULL OR
                                        _proc_table_def.cluster_id              LIKE _put_column_transform.cluster_id_like) AND
                                (_put_column_transform.dblink_connname_like     IS NULL OR
                                        _proc_table_def.dblink_connname         LIKE _put_column_transform.dblink_connname_like) AND
                                (_put_column_transform.remote_schema_like       IS NULL OR
                                        _proc_table_def.remote_schema           LIKE _put_column_transform.remote_schema_like) AND
/*                              (_put_column_transform.queue_id                 IS NULL OR
                                        _proc_table_def.queue_id                = _put_column_transform.queue_id) AND*/
                                (_put_column_transform.in_column_type           IS NULL OR
                                        _return_column.in_column_type =         _put_column_transform.in_column_type) AND
                                (_put_column_transform.column_name_like         IS NULL OR
                                        _return_column.column_name              LIKE _put_column_transform.column_name_like) AND
                                (_put_column_transform.data_type_like           IS NULL OR
                                        _return_column.data_type                LIKE _put_column_transform.data_type_like) AND
                                (_put_column_transform.primary_in_table_id_like IS NULL OR
                                        EXISTS (SELECT 1 FROM primary_columns_temp AS tmp WHERE
                                                tmp.primary_in_table_id LIKE _put_column_transform.primary_in_table_id_like AND
                                                tmp.primary_column_name LIKE _put_column_transform.primary_column_name_like) OR
                                (_proc_table_def.in_table_id            LIKE _put_column_transform.primary_in_table_id_like AND
                                _return_column.column_name              LIKE _put_column_transform.primary_column_name_like
                                ))
                                THEN

                                IF _put_column_transform.priority = _last_priority THEN
                                        RAISE EXCEPTION
                                'More than 1 rule meets the criteria of proc_table_id ''%'' column ''%'' on the same priority (%).',
                                                proc_table_id, _return_column.column_name, _put_column_transform.priority
                                                USING HINT =
        'Change one of the rule''s priority.  If multiple rules are activated on the same priority, the code may be indeterminate.';
                                END IF;

                                IF _final_ids && _put_column_transform.final_ids THEN
                                        CONTINUE;
                                END IF;

                                _final_ids := _final_ids || _put_column_transform.final_ids;
                                _last_priority := _put_column_transform.priority;

                                IF _put_column_transform.new_data_type IS NOT NULL THEN
                                        _return_column.data_type := _put_column_transform.new_data_type;
                                END IF;

                                IF _put_column_transform.new_in_column_type IS NOT NULL THEN
                                        _return_column.in_column_type := _put_column_transform.new_in_column_type;
                                END IF;

                                IF _put_column_transform.new_column_name IS NOT NULL THEN
                                        _return_column.column_name := _put_column_transform.new_column_name;
                                END IF;

                                IF _put_column_transform.pos_method IS NOT NULL THEN
                                        _return_column.pos_method := _put_column_transform.pos_method;
                                END IF;

                                IF _put_column_transform.pos_before IS NOT NULL THEN
                                        _return_column.pos_before := _put_column_transform.pos_before;
                                END IF;

                                IF _put_column_transform.pos_ref_column_names_like IS NOT NULL THEN
                                        _return_column.pos_ref_column_names_like := _put_column_transform.pos_ref_column_names_like;
                                END IF;

                                IF _put_column_transform.pos_in_column_type IS NOT NULL THEN
                                        _return_column.pos_in_column_type := _put_column_transform.pos_in_column_type;
                                END IF;

                                IF _put_column_transform.expression IS NOT NULL THEN
                                        _return_column.value_expression :=
                                                replace(_put_column_transform.expression, '%1', _return_column.value_expression);
                                END IF;

                                _add_columns := _add_columns || _put_column_transform.add_columns;

                                IF _put_column_transform.omit IS NOT NULL THEN
                                        _omit := _put_column_transform.omit;
                                END IF;

                                IF _put_column_transform.final_rule THEN
                                        EXIT;
                                END IF;

                                IF _put_column_transform.variable_name IS NOT NULL THEN

                                        INSERT INTO put_sql_expressions_temp (
                                                in_column_type,
                                                delta_types,
                                                variable_index,
                                                variable_name,
                                                data_type,
                                                expression,
                                                exception_traps
                                        ) VALUES (
                                                _return_column.in_column_type,
                                                COALESCE(_put_column_transform.variable_delta_types,
                                                        ARRAY['Add','Change','Delete']::sys_syn_dblink.delta_type[]),
                                                _variable_index,
                                                _put_column_transform.variable_name,
                                                _return_column.data_type,
                                                _return_column.value_expression,
                                                COALESCE(_put_column_transform.variable_exception_traps,
                                                        '{}'::sys_syn_dblink.exception_trap[])
                                        );

                                        _variable_index := _variable_index + 1;
                                        _return_column.value_expression := _put_column_transform.variable_name;

                                END IF;

                        END IF;

                END LOOP;

                IF _omit THEN
                        _return_columns := _return_columns || _add_columns;
                ELSE
                        _return_columns := _return_columns || _return_column || _add_columns;
                END IF;

        END LOOP;

        DROP TABLE primary_columns_temp;

        FOR     _return_column IN
        SELECT  *
        FROM    unnest(add_columns || _return_columns) AS add_columns
        LOOP
                IF _return_column.pos_method = 'Here'::sys_syn_dblink.column_position_method THEN
                        _return_column.pos_method := 'InColumnType'::sys_syn_dblink.column_position_method;
                        _return_column.pos_in_column_type = _return_column.in_column_type;
                END IF;
                IF _return_column.pos_method = 'InColumnType'::sys_syn_dblink.column_position_method THEN
                        _return_column.pos_method := NULL;
                        IF _return_column.pos_in_column_type = 'Id'::sys_syn_dblink.in_column_type THEN
                                IF _return_column.pos_before THEN
                                        _return_columns_id := _return_column || _return_columns_id;
                                ELSE
                                        _return_columns_id := _return_columns_id || _return_column;
                                END IF;
                        ELSIF _return_column.pos_in_column_type = 'Attribute'::sys_syn_dblink.in_column_type THEN
                                IF _return_column.pos_before THEN
                                        _return_columns_attr := _return_column || _return_columns_attr;
                                ELSE
                                        _return_columns_attr := _return_columns_attr || _return_column;
                                END IF;
                        ELSIF _return_column.pos_in_column_type = 'NoDiff'::sys_syn_dblink.in_column_type THEN
                                IF _return_column.pos_before THEN
                                        _return_columns_nodiff := _return_column || _return_columns_nodiff;
                                ELSE
                                        _return_columns_nodiff := _return_columns_nodiff || _return_column;
                                END IF;
                        ELSE
                                RAISE EXCEPTION 'Unknown sys_syn_dblink.in_column_type.';
                        END IF;
                        _return_column.pos_in_column_type       := NULL;
                        _return_column.pos_before               := NULL;
                END IF;
        END LOOP;

        RETURN _return_columns_id || _return_columns_attr || _return_columns_nodiff;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.put_columns_get(text, sys_syn_dblink.create_put_column[])
  IS '';

CREATE FUNCTION sys_syn_dblink.proc_columns_format (
        put_columns             sys_syn_dblink.create_proc_column[],
        format_text             text,
        column_delimiter        text)
  RETURNS text AS
$BODY$
DECLARE
        _column         sys_syn_dblink.create_proc_column;
        _sql_buffer     TEXT := '';
BEGIN
        IF format_text IS NULL THEN
                RAISE EXCEPTION 'sys_syn_dblink.format_text cannot be null.';
        END IF;

        IF column_delimiter IS NULL THEN
                RAISE EXCEPTION 'sys_syn_dblink.column_delimiter cannot be null.';
        END IF;

        FOR     _column IN
        SELECT  put_column_rows.*
        FROM    unnest(proc_columns_format.put_columns) AS put_column_rows
        LOOP
                IF _sql_buffer != '' THEN
                        _sql_buffer := _sql_buffer || column_delimiter;
                END IF;

                _sql_buffer := _sql_buffer || REPLACE(
                        REPLACE(proc_columns_format.format_text, '%FORMAT_TYPE%', _column.data_type),
                        '%COLUMN_NAME%', quote_ident(_column.column_name));
        END LOOP;

        IF _sql_buffer = '' THEN
                RAISE EXCEPTION 'sys_syn_dblink.proc_columns_format SQL "".';
        END IF;

        IF _sql_buffer IS NULL THEN
                RAISE EXCEPTION 'sys_syn_dblink.proc_columns_format SQL null.';
        END IF;

        RETURN _sql_buffer;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.proc_columns_format(sys_syn_dblink.create_proc_column[], text, text)
  IS '';

CREATE FUNCTION sys_syn_dblink.put_columns_format (
        put_columns             sys_syn_dblink.create_put_column[],
        format_text             text,
        column_delimiter        text)
  RETURNS text AS
$BODY$
DECLARE
        _column         sys_syn_dblink.create_put_column;
        _sql_buffer     TEXT := '';
BEGIN
        IF format_text IS NULL THEN
                RAISE EXCEPTION 'sys_syn_dblink.format_text cannot be null.';
        END IF;

        IF column_delimiter IS NULL THEN
                RAISE EXCEPTION 'sys_syn_dblink.column_delimiter cannot be null.';
        END IF;

        FOR     _column IN
        SELECT  put_column_rows.*
        FROM    unnest(put_columns_format.put_columns) AS put_column_rows
        LOOP
                IF _sql_buffer != '' THEN
                        _sql_buffer := _sql_buffer || column_delimiter;
                END IF;

                _sql_buffer := _sql_buffer || sys_syn_dblink.put_column_format(_column, put_columns_format.format_text);
        END LOOP;

        IF _sql_buffer = '' THEN
                RAISE EXCEPTION 'sys_syn_dblink.put_columns_format SQL "".';
        END IF;

        IF _sql_buffer IS NULL THEN
                RAISE EXCEPTION 'sys_syn_dblink.put_columns_format SQL null.';
        END IF;

        RETURN _sql_buffer;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.put_columns_format(sys_syn_dblink.create_put_column[], text, text)
  IS '';

CREATE FUNCTION sys_syn_dblink.put_column_format (
        put_column      sys_syn_dblink.create_put_column,
        format_text     text)
  RETURNS text AS
$BODY$
DECLARE
        _sql_buffer     TEXT := '';
BEGIN
        IF format_text IS NULL THEN
                RAISE EXCEPTION 'sys_syn_dblink.format_text cannot be null.';
        END IF;

        _sql_buffer := REPLACE(REPLACE(REPLACE(
                put_column_format.format_text, '%VALUE_EXPRESSION%', put_column.value_expression),
                '%FORMAT_TYPE%', put_column.data_type),
                '%COLUMN_NAME%', quote_ident(put_column.column_name));

        IF _sql_buffer = '' THEN
                RAISE EXCEPTION 'sys_syn_dblink.put_column_format SQL "".';
        END IF;

        IF _sql_buffer IS NULL THEN
                RAISE EXCEPTION 'sys_syn_dblink.put_column_format SQL null.';
        END IF;

        RETURN _sql_buffer;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.put_column_format(sys_syn_dblink.create_put_column, text)
  IS '';


CREATE FUNCTION sys_syn_dblink.proc_table_create (
        in_table_id             text,
        out_group_id            text,
        put_group_id            text,
        proc_schema             regnamespace default null,
        proc_table_id           text    default null,
        put_schema              regnamespace default null,
        put_table_name          text    default null,
        table_type_id           text    default 'sys_syn-direct',
        table_settings          hstore  default ''::hstore,
        dblink_connname         text    default 'sys_syn',
--      hold_cache_min_rows     int     default    256,
        records_per_proc        int     default  50000,
        remote_sql_len_max      int     default 262144,
        comments                text    default '%1')
  RETURNS void AS
$BODY$
DECLARE
        _put_table_name                         TEXT;
        _sql_buffer                             TEXT;
        _sql_delimit                            BOOLEAN;
        _proc_worker_def                        sys_syn_dblink.proc_workers_def%ROWTYPE;
        _column_name                            TEXT;
        _type_id_name                           TEXT;
        _type_attributes_name                   TEXT;
        _type_no_diff_name                      TEXT;
        _proc_table_def                         sys_syn_dblink.proc_tables_def;
        _table_type_def                         sys_syn_dblink.table_types_def;
        _columns_proc                           sys_syn_dblink.create_proc_column[];
        _columns_put                            sys_syn_dblink.create_put_column[];
        _proc_columns_id                        sys_syn_dblink.create_proc_column[];
        _proc_columns_attribute                 sys_syn_dblink.create_proc_column[];
        _proc_columns_attribute_orderby         sys_syn_dblink.create_proc_column[];
        _proc_columns_attribute_unordered       sys_syn_dblink.create_proc_column[];
        _proc_columns_nodiff                    sys_syn_dblink.create_proc_column[];
        _put_table_transform                    sys_syn_dblink.put_table_transforms%ROWTYPE;
        _rule_group_ids                         text[];
        _add_put_columns                        sys_syn_dblink.create_put_column[];
        _final_ids                              text[];
        _omit                                   boolean;
        _last_priority                          smallint;
BEGIN
        SELECT  COALESCE(proc_table_create.proc_schema, current_schema()::regnamespace),
                COALESCE(proc_table_create.proc_table_id, in_table_def.in_table_id),
                in_table_def.cluster_id,                proc_table_create.dblink_connname,
                in_table_def.schema_name,               in_table_def.in_table_id,
                in_table_def.in_group_id,               in_table_def.attributes_array,
                in_table_def.out_group_id,

                proc_table_create.put_group_id,
                COALESCE(proc_table_create.put_schema, current_schema()::regnamespace),
                COALESCE(put_table_name, in_table_def.in_table_id),

                proc_table_create.table_type_id,
                proc_table_create.records_per_proc,     proc_table_create.remote_sql_len_max,
                in_table_def.claim_queue_count,
                replace(proc_table_create.comments, '%1', in_table_def.comments)

        INTO    _proc_table_def.proc_schema,
                _proc_table_def.proc_table_id,
                _proc_table_def.cluster_id,             _proc_table_def.dblink_connname,
                _proc_table_def.remote_schema,          _proc_table_def.in_table_id,
                _proc_table_def.in_group_id,            _proc_table_def.attributes_array,
                _proc_table_def.out_group_id,

                _proc_table_def.put_group_id,
                _proc_table_def.put_schema,
                _proc_table_def.put_table_name,

                _proc_table_def.table_type_id,
                _proc_table_def.records_per_proc,       _proc_table_def.remote_sql_len_max,
                _proc_table_def.queue_count,
                _proc_table_def.comments

        FROM    dblink(dblink_connname, $$
                        SELECT  in_tables_def.in_table_id,
                                in_tables_def.in_group_id,
                                in_tables_def.attributes_array,
                                CASE WHEN SUBSTRING(in_tables_def.schema::text, 1, 1) = '"' THEN
                                        replace(
                                            SUBSTRING(in_tables_def.schema::text, 2,LENGTH(in_tables_def.schema::text)-2),
                                            '""', '"')
                                        ELSE in_tables_def.schema::text END AS schema_name,
                                in_tables_def.comments,
                                out_tables_def.out_group_id,
                                out_tables_def.claim_queue_count,
                                settings.cluster_id
                        FROM    sys_syn.in_tables_def JOIN sys_syn.out_tables_def ON
                                        out_tables_def.in_table_id = in_tables_def.in_table_id JOIN
                                sys_syn.settings ON
                                        TRUE
                        WHERE   in_tables_def.in_table_id = $$||quote_literal(proc_table_create.in_table_id)||$$ AND
                                out_tables_def.out_group_id = $$||quote_literal(proc_table_create.out_group_id)||$$
                $$) AS in_table_def(in_table_id text, in_group_id text, attributes_array boolean, schema_name text, comments text,
                        out_group_id text, claim_queue_count smallint, cluster_id text);

        IF _proc_table_def.in_table_id IS NULL THEN
                RAISE EXCEPTION 'Cannot find in_table_id ''%'' out_group_id ''%''.', proc_table_create.in_table_id,
                        proc_table_create.out_group_id
                USING HINT = 'Check the sys_syn.in_tables_def table.';
        END IF;

        _rule_group_ids := sys_syn_dblink.rule_group_ids_get(_proc_table_def);

        _add_put_columns                        := ARRAY[]::sys_syn_dblink.create_put_column[];
        _proc_table_def.table_settings          := ''::hstore;
        _final_ids                              := ARRAY[]::TEXT[];
        _omit                                   := FALSE;
        _last_priority                          := -1;

        FOR     _put_table_transform IN
        SELECT  *
        FROM    sys_syn_dblink.put_table_transforms
        WHERE   (       put_table_transforms.rule_group_id IS NULL OR
                        put_table_transforms.rule_group_id = ANY(_rule_group_ids)
                )
        ORDER BY put_table_transforms.priority
        LOOP

                IF      (_put_table_transform.proc_table_id_like        IS NULL OR
                                _proc_table_def.proc_table_id           LIKE _put_table_transform.proc_table_id_like) AND
                        (_put_table_transform.in_table_id_like          IS NULL OR
                                _proc_table_def.in_table_id             LIKE _put_table_transform.in_table_id_like) AND
                        (_put_table_transform.out_group_id_like         IS NULL OR
                                _proc_table_def.out_group_id            LIKE _put_table_transform.out_group_id_like) AND
                        (_put_table_transform.in_group_id_like          IS NULL OR
                                _proc_table_def.in_group_id             LIKE _put_table_transform.in_group_id_like) AND
                        (_put_table_transform.proc_schema_like          IS NULL OR
                                _proc_table_def.proc_schema::text       LIKE _put_table_transform.proc_schema_like) AND
                        (_put_table_transform.put_schema_like           IS NULL OR
                                _proc_table_def.put_schema::text        LIKE _put_table_transform.put_schema_like) AND
                        (_put_table_transform.put_table_name_like       IS NULL OR
                                _proc_table_def.put_table_name          LIKE _put_table_transform.put_table_name_like) AND
                        (_put_table_transform.table_type_id_like        IS NULL OR
                                _proc_table_def.table_type_id           LIKE _put_table_transform.table_type_id_like) AND
                        (_put_table_transform.attributes_array          IS NULL OR
                                _proc_table_def.attributes_array        = _put_table_transform.attributes_array) AND
                        (_put_table_transform.cluster_id_like           IS NULL OR
                                _proc_table_def.cluster_id              LIKE _put_table_transform.cluster_id_like) AND
                        (_put_table_transform.dblink_connname_like      IS NULL OR
                                _proc_table_def.dblink_connname         LIKE _put_table_transform.dblink_connname_like) AND
                        (_put_table_transform.remote_schema_like        IS NULL OR
                                _proc_table_def.remote_schema           LIKE _put_table_transform.remote_schema_like)/* AND
                        (_put_table_transform.queue_id                  IS NULL OR
                                _proc_table_def.queue_id                = _put_table_transform.queue_id)*/
                        THEN

                        IF _put_table_transform.priority = _last_priority THEN
                                RAISE EXCEPTION
                                'More than 1 rule meets the criteria of proc_table_id ''%'' on the same priority (%).',
                                        _proc_table_def.proc_table_id, _put_table_transform.priority
                                USING HINT =
        'Change one of the rule''s priority.  If multiple rules are activated on the same priority, the code may be indeterminate.';
                        END IF;

                        IF _final_ids && _put_table_transform.final_ids THEN
                                CONTINUE;
                        END IF;

                        _final_ids := _final_ids || _put_table_transform.final_ids;
                        _last_priority := _put_table_transform.priority;

                        IF _put_table_transform.new_proc_schema IS NOT NULL THEN
                                _proc_table_def.proc_schema :=
                                        replace(_put_table_transform.new_proc_schema, '%1',_proc_table_def.proc_schema::text);
                        END IF;

                        IF _put_table_transform.new_put_schema IS NOT NULL THEN
                                _proc_table_def.put_schema :=
                                        replace(_put_table_transform.new_put_schema, '%1', _proc_table_def.put_schema::text);
                        END IF;

                        IF _put_table_transform.new_put_table_name IS NOT NULL THEN
                                _proc_table_def.put_table_name :=
                                        replace(_put_table_transform.new_put_table_name, '%1',_proc_table_def.put_table_name);
                        END IF;

                        IF _put_table_transform.new_table_type_id IS NOT NULL THEN
                                _proc_table_def.table_type_id :=
                                        replace(_put_table_transform.new_table_type_id, '%1', _proc_table_def.table_type_id);
                        END IF;

                        IF _put_table_transform.new_dblink_connname IS NOT NULL THEN
                                _proc_table_def.dblink_connname :=
                                     replace(_put_table_transform.new_dblink_connname, '%1', _proc_table_def.dblink_connname);
                        END IF;

/*                      IF _put_table_transform.new_hold_cache_min_rows IS NOT NULL THEN
                                _proc_table_def.hold_cache_min_rows := _put_table_transform.new_hold_cache_min_rows;
                        END IF;*/

                        IF _put_table_transform.new_records_per_proc IS NOT NULL THEN
                                _proc_table_def.records_per_proc := _put_table_transform.new_records_per_proc;
                        END IF;

                        IF _put_table_transform.new_remote_sql_len_max IS NOT NULL THEN
                                _proc_table_def.remote_sql_len_max := _put_table_transform.new_remote_sql_len_max;
                        END IF;

/*                      IF _put_table_transform.new_queue_id IS NOT NULL THEN
                                _proc_table_def.queue_id := _put_table_transform.new_queue_id;
                        END IF;*/

                        _add_put_columns := _add_put_columns || _put_table_transform.add_columns;
                        _proc_table_def.table_settings := _proc_table_def.table_settings || _put_table_transform.table_settings;

                        IF _put_table_transform.omit IS NOT NULL THEN
                                _omit := _put_table_transform.omit;
                        END IF;

                        IF _put_table_transform.final_rule THEN
                                EXIT;
                        END IF;

                        /*IF _put_table_transform.variable_name IS NOT NULL THEN

                                INSERT INTO put_sql_expressions_temp (
                                        in_column_type,
                                        delta_types,
                                        variable_index,
                                        variable_name,
                                        data_type,
                                        expression,
                                        exception_traps
                                ) VALUES (
                                        _return_column.in_column_type,
                                        COALESCE(_put_table_transform.variable_delta_types,
                                                ARRAY['Add','Change','Delete']::sys_syn_dblink.delta_type[]),
                                        _variable_index,
                                        _put_table_transform.variable_name,
                                        _return_column.data_type,
                                        _return_column.value_expression,
                                        COALESCE(_put_table_transform.variable_exception_traps,
                                                '{}'::sys_syn_dblink.exception_trap[])
                                );

                                _variable_index := _variable_index + 1;
                                _return_column.value_expression := _put_table_transform.variable_name;

                        END IF;*/

                END IF;

        END LOOP;

        IF _omit THEN
                RETURN;
        END IF;

        IF _proc_table_def.table_type_id IS NULL THEN
                RAISE EXCEPTION 'table_type_id is null.';
        END IF;

        _table_type_def := (
                SELECT  table_types_def
                FROM    sys_syn_dblink.table_types_def
                WHERE   table_types_def.table_type_id           = _proc_table_def.table_type_id AND
                        table_types_def.attributes_array        = _proc_table_def.attributes_array);

        IF _table_type_def IS NULL THEN
                RAISE EXCEPTION 'table_type_id not found.';
        END IF;

        _proc_table_def.table_settings := _table_type_def.initial_table_settings || _proc_table_def.table_settings ||table_settings;

        INSERT INTO sys_syn_dblink.proc_tables_def VALUES(_proc_table_def.*);

        _proc_table_def := (
                SELECT  proc_tables_def
                FROM    sys_syn_dblink.proc_tables_def
                WHERE   proc_tables_def.proc_table_id = _proc_table_def.proc_table_id);

        INSERT INTO sys_syn_dblink.proc_partitions_def
        SELECT  _proc_table_def.proc_table_id,
                                dblink_partitions.partition_id, dblink_partitions.comments
        FROM    dblink(dblink_connname, $$
                        SELECT  partition_id,                   comments
                        FROM    sys_syn.in_partitions_def
                        WHERE   in_table_id = $$||quote_literal(_proc_table_def.in_table_id)||$$
                $$) AS dblink_partitions(partition_id smallint, comments text);

        IF _proc_table_def.queue_count IS NULL THEN
                INSERT INTO sys_syn_dblink.proc_workers_def (
                        proc_table_id,                          partition_id,
                        worker_id,                              queue_id
                )
                SELECT  _proc_table_def.proc_table_id,          proc_partitions_def.partition_id,
                        proc_partitions_def.partition_id,       NULL
                FROM    sys_syn_dblink.proc_partitions_def
                WHERE   proc_partitions_def.proc_table_id = _proc_table_def.proc_table_id;
        ELSE
                INSERT INTO sys_syn_dblink.proc_workers_def (
                        proc_table_id,                          partition_id,
                        worker_id,
                        queue_id
                )
                SELECT  _proc_table_def.proc_table_id,          proc_partitions_def.partition_id,
                        ((partition_id - 1) * _proc_table_def.queue_count) + generate_series,
                        generate_series
                FROM    sys_syn_dblink.proc_partitions_def JOIN generate_series(1, _proc_table_def.queue_count) ON TRUE
                WHERE   proc_partitions_def.proc_table_id = _proc_table_def.proc_table_id;
        END IF;

        _type_id_name           := _proc_table_def.proc_table_id||'_proc_id';
        _type_attributes_name   := _proc_table_def.proc_table_id||'_proc_attributes';
        _type_no_diff_name      := _proc_table_def.proc_table_id||'_proc_no_diff';

        CREATE TEMPORARY TABLE out_queue_data_view_columns_temp ON COMMIT DROP AS
        SELECT  *
        FROM    dblink(dblink_connname, $$
                        SELECT  column_name,
                                CASE in_column_type
                                        WHEN    'Id'::sys_syn.in_column_type            THEN 1
                                        WHEN    'Attribute'::sys_syn.in_column_type     THEN 2
                                        WHEN    'NoDiff'::sys_syn.in_column_type        THEN 3
                                        WHEN    'TransIdIn'::sys_syn.in_column_type     THEN 4
                                        ELSE    NULL
                                END AS in_column_type,
                                format_type,
                                column_ordinal,
                                array_order
                        FROM    sys_syn.out_queue_data_view_columns_view
                        WHERE   out_queue_data_view_columns_view.in_table_id =
                                        $$||quote_literal(_proc_table_def.in_table_id)||$$ AND
                                out_queue_data_view_columns_view.out_group_id =
                                        $$||quote_literal(_proc_table_def.out_group_id)||$$
                $$) AS out_queue_data_view_columns(column_name text, in_column_type smallint, format_type text,
                        column_ordinal smallint, array_order smallint);

        IF (SELECT COUNT(*) FROM out_queue_data_view_columns_temp) = 0 THEN
                RAISE EXCEPTION 'data_view not found for in_table_id "%" out_group_id "%".', _proc_table_def.in_table_id,
                        _proc_table_def.out_group_id
                USING HINT = 'Make sure that the data_view was created.';
        END IF;

        INSERT INTO sys_syn_dblink.proc_columns_def
        SELECT  _proc_table_def.proc_table_id, view_columns.column_name,
                CASE view_columns.in_column_type
                        WHEN    1 THEN 'Id'::sys_syn_dblink.in_column_type
                        WHEN    2 THEN 'Attribute'::sys_syn_dblink.in_column_type
                        WHEN    3 THEN 'NoDiff'::sys_syn_dblink.in_column_type
                        WHEN    4 THEN 'TransIdIn'::sys_syn_dblink.in_column_type
                        ELSE    NULL
                END AS in_column_type,
                view_columns.format_type, view_columns.column_ordinal, view_columns.array_order
        FROM    out_queue_data_view_columns_temp AS view_columns
        ORDER BY view_columns.column_ordinal;

        DELETE
        FROM    sys_syn_dblink.proc_foreign_keys
        WHERE   proc_foreign_keys.foreign_proc_table_id = _proc_table_def.proc_table_id;

        INSERT INTO sys_syn_dblink.proc_foreign_keys
        SELECT  _proc_table_def.cluster_id,                     _proc_table_def.proc_table_id,
                dblink_foreign_keys.foreign_key_index,          dblink_foreign_keys.primary_in_table_id,
                dblink_foreign_keys.foreign_column_name,        dblink_foreign_keys.primary_column_name
        FROM    dblink(dblink_connname, $$
                        SELECT  primary_in_table_id,    foreign_key_index,
                                primary_column_name,    foreign_column_name
                        FROM    sys_syn.in_foreign_keys
                        WHERE   foreign_in_table_id = $$||quote_literal(_proc_table_def.in_table_id)||$$
                $$) AS dblink_foreign_keys(primary_in_table_id text, foreign_key_index smallint, primary_column_name text,
                        foreign_column_name text);

        _sql_buffer := 'CREATE TYPE ' || _proc_table_def.proc_schema || '.' || quote_ident(_type_id_name) || ' AS (';
        _sql_delimit := FALSE;
        FOR     _column_name IN
        SELECT  column_name
        FROM    out_queue_data_view_columns_temp
        WHERE   in_column_type = 1
        ORDER BY column_ordinal
        LOOP
                IF _sql_delimit THEN
                        _sql_buffer := _sql_buffer || ',';
                ELSE
                        _sql_delimit := TRUE;
                END IF;

                SELECT  _sql_buffer || '
        '||quote_ident(_column_name)||'       '||format_type||''
                INTO    _sql_buffer
                FROM    out_queue_data_view_columns_temp
                WHERE   column_name = _column_name;
        END LOOP;
        IF _sql_delimit = FALSE THEN
                RAISE EXCEPTION '1 or more ID columns are required.';
        END IF;
        _sql_buffer := _sql_buffer || ');
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'CREATE TYPE ' || _proc_table_def.proc_schema || '.' || quote_ident(_type_attributes_name) || ' AS (';
        _sql_delimit := FALSE;
        FOR     _column_name IN
        SELECT  column_name
        FROM    out_queue_data_view_columns_temp
        WHERE   in_column_type = 2
        ORDER BY column_ordinal
        LOOP
                IF _sql_delimit THEN
                        _sql_buffer := _sql_buffer || ',';
                ELSE
                        _sql_delimit := TRUE;
                END IF;

                SELECT  _sql_buffer || '
        '||quote_ident(_column_name)||'       '||format_type||''
                INTO    _sql_buffer
                FROM    out_queue_data_view_columns_temp
                WHERE   column_name = _column_name;
        END LOOP;
        _sql_buffer := _sql_buffer || ');
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'CREATE TYPE ' || _proc_table_def.proc_schema || '.' || quote_ident(_type_no_diff_name) || ' AS (';
        _sql_delimit := FALSE;
        FOR     _column_name IN
        SELECT  column_name
        FROM    out_queue_data_view_columns_temp
        WHERE   in_column_type = 3
        ORDER BY column_ordinal
        LOOP
                IF _sql_delimit THEN
                        _sql_buffer := _sql_buffer || ',';
                ELSE
                        _sql_delimit := TRUE;
                END IF;

                SELECT  _sql_buffer || '
        '||quote_ident(_column_name)||'       '||format_type||''
                INTO    _sql_buffer
                FROM    out_queue_data_view_columns_temp
                WHERE   column_name = _column_name;
        END LOOP;
        _sql_buffer := _sql_buffer || ');
';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        FOR     _proc_worker_def IN
        SELECT  *
        FROM    sys_syn_dblink.proc_workers_def
        WHERE   proc_workers_def.proc_table_id = _proc_table_def.proc_table_id
        LOOP
                PERFORM sys_syn_dblink.proc_table_create_worker(_proc_table_def.proc_table_id, _proc_worker_def.worker_id);
        END LOOP;

        DROP TABLE out_queue_data_view_columns_temp;

        _columns_proc   := sys_syn_dblink.proc_columns_get(_proc_table_def.proc_table_id);
        _columns_put    := sys_syn_dblink.put_columns_get(_proc_table_def.proc_table_id, _add_put_columns);

        SELECT  array_agg(proc_columns)
        INTO    _proc_columns_id
        FROM    unnest(_columns_proc) AS proc_columns
        WHERE   proc_columns.in_column_type = 'Id'::sys_syn_dblink.in_column_type;

        SELECT  array_agg(proc_columns)
        INTO    _proc_columns_attribute
        FROM    unnest(_columns_proc) AS proc_columns
        WHERE   proc_columns.in_column_type = 'Attribute'::sys_syn_dblink.in_column_type;

        SELECT  array_agg(proc_columns ORDER BY proc_columns.array_order)
        INTO    _proc_columns_attribute_orderby
        FROM    unnest(_columns_proc) AS proc_columns
        WHERE   proc_columns.in_column_type = 'Attribute'::sys_syn_dblink.in_column_type AND
                proc_columns.array_order IS NOT NULL;

        SELECT  array_agg(proc_columns)
        INTO    _proc_columns_attribute_unordered
        FROM    unnest(_columns_proc) AS proc_columns
        WHERE   proc_columns.in_column_type = 'Attribute'::sys_syn_dblink.in_column_type AND
                proc_columns.array_order IS NULL;

        SELECT  array_agg(proc_columns)
        INTO    _proc_columns_nodiff
        FROM    unnest(_columns_proc) AS proc_columns
        WHERE   proc_columns.in_column_type = 'NoDiff'::sys_syn_dblink.in_column_type;

        IF NOT EXISTS (
                SELECT
                FROM   pg_catalog.pg_class
                JOIN   pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                WHERE  pg_namespace.nspname     = _proc_table_def.put_schema::text AND
                       pg_class.relname         = _proc_table_def.put_table_name
        ) THEN
                EXECUTE 'SELECT '||_table_type_def.table_create_proc_schema::text||'.'||
                                quote_ident(_table_type_def.table_create_proc_name)||
                        '($1, $2, $3, $4)'
                INTO    _sql_buffer
                USING   _proc_table_def.put_schema::text,       _proc_table_def.put_table_name,         _columns_put,
                        _proc_table_def.table_settings;
                EXECUTE _sql_buffer;
        END IF;

        PERFORM sys_syn_dblink.proc_table_code(
                _proc_table_def,                        _table_type_def,
                _proc_columns_id,                       _proc_columns_attribute,        _proc_columns_attribute_orderby,
                _proc_columns_attribute_unordered,      _proc_columns_nodiff,
                _columns_put,
                _type_id_name,                          _type_attributes_name,          _type_no_diff_name,
                _proc_table_def.table_settings);

        FOR     _proc_worker_def IN
        SELECT  *
        FROM    sys_syn_dblink.proc_workers_def
        WHERE   proc_workers_def.proc_table_id = _proc_table_def.proc_table_id
        LOOP
                PERFORM sys_syn_dblink.proc_table_code(
                        _proc_table_def,                        _table_type_def,
                        _proc_columns_id,                       _proc_columns_attribute,        _proc_columns_attribute_orderby,
                        _proc_columns_attribute_unordered,      _proc_columns_nodiff,
                        _columns_put,
                        _type_id_name,                          _type_attributes_name,          _type_no_diff_name,
                        _proc_table_def.table_settings,         _proc_worker_def.worker_id,     _proc_worker_def.partition_id);
        END LOOP;

        DROP TABLE put_sql_expressions_temp;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.proc_table_create(
        in_table_id             text,
        out_group_id            text,
        put_group_id            text,
        proc_schema             regnamespace,
        proc_table_id           text,
        put_schema              regnamespace,
        put_table_name          text,
        table_type_id           text,
        table_settings          hstore,
        dblink_connname         text,
--      hold_cache_min_rows     int,
        records_per_proc        int,
        remote_sql_len_max      int,
        comments                text)
  IS '';


CREATE FUNCTION sys_syn_dblink.proc_table_create_worker (
        proc_table_id   text,
        worker_id       smallint)
  RETURNS void AS
$BODY$
DECLARE
        _proc_table_def         sys_syn_dblink.proc_tables_def;
        _object_id              TEXT;
        _worker_suffix          TEXT;
        _type_id_name           TEXT;
        _type_attributes_name   TEXT;
        _type_no_diff_name      TEXT;
        _sql_buffer             TEXT;
BEGIN
        _proc_table_def := (
                SELECT  proc_tables_def
                FROM    sys_syn_dblink.proc_tables_def
                WHERE   proc_tables_def.proc_table_id = proc_table_create_worker.proc_table_id);

        _object_id              := _proc_table_def.proc_table_id;
        _worker_suffix          := '_' || worker_id;
        _type_id_name           := _proc_table_def.proc_table_id||'_proc_id';
        _type_attributes_name   := _proc_table_def.proc_table_id||'_proc_attributes';
        _type_no_diff_name      := _proc_table_def.proc_table_id||'_proc_no_diff';

        _sql_buffer := 'CREATE UNLOGGED TABLE ' || _proc_table_def.proc_schema || '.' ||
                quote_ident(_object_id||'_processing'||_worker_suffix) || ' (
        id ' || _proc_table_def.proc_schema || '.' || quote_ident(_type_id_name) || ' NOT NULL,
        trans_id_in             integer NOT NULL,
        delta_type              sys_syn_dblink.delta_type NOT NULL,
        queue_priority          smallint,
        hold_updated            boolean,
        prior_hold_reason_count integer,
        prior_hold_reason_id    integer,
        prior_hold_reason_text  text,
        attributes ' || _proc_table_def.proc_schema || '.' || quote_ident(_type_attributes_name) ||
        CASE WHEN _proc_table_def.attributes_array THEN '[]' ELSE '' END || ',
        no_diff ' || _proc_table_def.proc_schema || '.' || quote_ident(_type_no_diff_name) || ',
        CONSTRAINT ' || quote_ident(_object_id||'_proc_pkey'||_worker_suffix) || ' PRIMARY KEY (id)
)';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'CREATE UNLOGGED TABLE ' || _proc_table_def.proc_schema || '.' ||
                quote_ident(_object_id||'_processed'||_worker_suffix) || ' (
        id ' || _proc_table_def.proc_schema || '.' || quote_ident(_type_id_name) || ' NOT NULL,
        hold_reason_id          integer,
        hold_reason_text        text,
        queue_priority          smallint,
        processed_time          timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT ' || quote_ident(_object_id||'_processed_pkey'||_worker_suffix) || ' PRIMARY KEY (id)
)';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'CREATE TABLE ' || _proc_table_def.proc_schema || '.' ||
                quote_ident(_object_id||'_queue_status'||_worker_suffix) || ' (
        queue_id smallint DEFAULT NULL)';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'CREATE UNIQUE INDEX ' || quote_ident(_object_id||'_queue_status_1_row_idx'||_worker_suffix) || '
        ON ' || _proc_table_def.proc_schema || '.' || quote_ident(_object_id||'_queue_status'||_worker_suffix) || '
        USING btree
        ((true));';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;

        _sql_buffer := 'INSERT INTO ' || _proc_table_def.proc_schema || '.' ||
                quote_ident(_object_id||'_queue_status'||_worker_suffix) || ' DEFAULT VALUES';
        RAISE DEBUG '%', _sql_buffer;
        EXECUTE _sql_buffer;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 20;
COMMENT ON FUNCTION sys_syn_dblink.proc_table_create_worker(text, smallint)
  IS '';


CREATE FUNCTION sys_syn_dblink.put_columns_query_unique_index (
        put_columns             sys_syn_dblink.create_put_column[],
        include_array_order     BOOLEAN DEFAULT FALSE)
        RETURNS sys_syn_dblink.create_put_column[] AS
$BODY$
DECLARE
        _columns_primary_id  sys_syn_dblink.create_put_column[];
        _columns_array_order sys_syn_dblink.create_put_column[];
BEGIN
        SELECT  array_agg(columns_id_rows)
        INTO    _columns_primary_id
        FROM    unnest(put_columns) AS columns_id_rows
        WHERE   in_column_type = 'Id'::sys_syn_dblink.in_column_type;

        IF include_array_order THEN
                SELECT  array_agg(columns_id_rows ORDER BY columns_id_rows.array_order)
                INTO    _columns_array_order
                FROM    unnest(put_columns) AS columns_id_rows
                WHERE   columns_id_rows.array_order IS NOT NULL;

                IF _columns_array_order IS NOT NULL THEN
                        _columns_primary_id = _columns_primary_id || _columns_array_order;
                END IF;
        END IF;

        RETURN _columns_primary_id;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 20;
COMMENT ON FUNCTION sys_syn_dblink.put_columns_query_unique_index(sys_syn_dblink.create_put_column[], BOOLEAN)
  IS '';

CREATE FUNCTION sys_syn_dblink.put_columns_query (
        put_columns             sys_syn_dblink.create_put_column[],
        in_column_types         sys_syn_dblink.in_column_type[],
        in_column_type_not      boolean default false,
        include_array_order     boolean default null)
        RETURNS sys_syn_dblink.create_put_column[] AS
$BODY$
DECLARE
        _columns sys_syn_dblink.create_put_column[];
BEGIN
        SELECT  array_agg(columns_id_rows)
        INTO    _columns
        FROM    unnest(put_columns) AS columns_id_rows
        WHERE   (ARRAY[in_column_type] <@ in_column_types != in_column_type_not) AND
                (include_array_order IS NULL OR (include_array_order = (array_order IS NOT NULL)));

        RETURN _columns;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 20;
COMMENT ON FUNCTION sys_syn_dblink.put_columns_query(sys_syn_dblink.create_put_column[], sys_syn_dblink.in_column_type[], BOOLEAN,
        BOOLEAN)
  IS '';


CREATE TEMPORARY TABLE put_sql_expressions_temp (
        in_column_type  sys_syn_dblink.in_column_type NOT NULL,
        delta_types     sys_syn_dblink.delta_type[] NOT NULL,
        variable_index  smallint NOT NULL,
        variable_name   text NOT NULL,
        data_type       text NOT NULL,
        expression      text NOT NULL,
        exception_traps sys_syn_dblink.exception_trap[] NOT NULL,
        declared        boolean DEFAULT FALSE NOT NULL)
ON COMMIT DROP;

CREATE FUNCTION sys_syn_dblink.put_sql_expressions (
        in_column_types         sys_syn_dblink.in_column_type[],
        delta_types             sys_syn_dblink.delta_type[],
        code_indent             smallint)
        RETURNS sys_syn_dblink.put_code_sql AS
$BODY$
DECLARE
        _put_code_sql           sys_syn_dblink.put_code_sql;
        _indent_sql             TEXT;
        _put_sql_expression     put_sql_expressions_temp%ROWTYPE;
        _exception_trap         sys_syn_dblink.exception_trap;
        _exception_statements   TEXT;
BEGIN
        _indent_sql := repeat(E'        ', code_indent);

        SELECT  string_agg($$
        $$||variable_name||' '||data_type||';', '' ORDER BY variable_index)
        INTO    _put_code_sql.declarations_sql
        FROM    put_sql_expressions_temp
        WHERE   declared = FALSE AND
                put_sql_expressions_temp.in_column_type = ANY (put_sql_expressions.in_column_types) AND
                put_sql_expressions_temp.delta_types && put_sql_expressions.delta_types;

        UPDATE  put_sql_expressions_temp
        SET     declared = TRUE
        WHERE   declared = FALSE AND
                put_sql_expressions_temp.in_column_type = ANY (put_sql_expressions.in_column_types) AND
                put_sql_expressions_temp.delta_types && put_sql_expressions.delta_types;

        _put_code_sql.declarations_sql = COALESCE(_put_code_sql.declarations_sql, '');
        _put_code_sql.logic_sql = '';

        FOR     _put_sql_expression IN
        SELECT  *
        FROM    put_sql_expressions_temp
        WHERE   put_sql_expressions_temp.in_column_type = ANY (put_sql_expressions.in_column_types) AND
                put_sql_expressions_temp.delta_types && put_sql_expressions.delta_types
        ORDER BY variable_index
        LOOP

                IF array_length(_put_sql_expression.exception_traps, 1) IS NULL THEN

                        _put_code_sql.logic_sql := $$
$$||_indent_sql||_put_sql_expression.variable_name||$$ := $$||_put_sql_expression.expression||$$;$$;

                ELSE

                        _put_code_sql.logic_sql := $$
$$||_indent_sql||$$BEGIN
$$||_indent_sql||'        '||_put_sql_expression.variable_name||$$ := $$||_put_sql_expression.expression||$$;
$$||_indent_sql||$$EXCEPTION$$;

                        FOR     _exception_trap IN
                        SELECT  *
                        FROM    unnest(_put_sql_expression.exception_traps)
                        LOOP
                                _exception_statements := _exception_trap.statements_sql;

                                IF substring(_exception_statements for 1) = '=' THEN
                                        _exception_statements := _put_sql_expression.variable_name || ' := ' ||
                                                substring(_exception_statements from 2) || ';';
                                END IF;

                                _put_code_sql.logic_sql := _put_code_sql.logic_sql || $$
$$||_indent_sql||$$        WHEN $$||array_to_string(_exception_trap.when_conditions, ', ')||$$ THEN
$$||_indent_sql||'                '||_exception_statements;

                        END LOOP;

                        _put_code_sql.logic_sql := _put_code_sql.logic_sql || $$
$$||_indent_sql||$$END;$$;

                END IF;
        END LOOP;

        RETURN _put_code_sql;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.put_sql_expressions(sys_syn_dblink.in_column_type[], sys_syn_dblink.delta_type[], smallint)
  IS '';

DROP TABLE put_sql_expressions_temp;

CREATE FUNCTION sys_syn_dblink.range_from_data_type (
    data_type text)
  RETURNS text AS
$BODY$
DECLARE
        _data_type_range text;
BEGIN
        IF data_type = 'integer' THEN
                RETURN 'int4range';
        ELSIF data_type = 'bigint' THEN
                RETURN 'int8range';
        ELSIF data_type = 'numeric' THEN
                RETURN 'numrange';
        ELSIF data_type = 'timestamp without time zone' THEN
                RETURN 'tsrange';
        ELSIF data_type = 'timestamp with time zone' THEN
                RETURN 'tstzrange';
        ELSIF data_type = 'date' THEN
                RETURN 'daterange';
        END IF;

        RAISE EXCEPTION 'sys_syn_dblink.range_from_data_type:  Data type does not have a range'
        USING HINT = 'Use integer, bigint, numeric, timestamp without time zone, timestamp with time zone, or date.  Or add the r'||
                'ange to sys_syn_dblink.range_from_data_type';
END
$BODY$
  LANGUAGE plpgsql IMMUTABLE
  COST 30;
COMMENT ON FUNCTION sys_syn_dblink.range_from_data_type(text)
  IS '';


-- direct

CREATE FUNCTION sys_syn_dblink.table_create_sql_direct (
        schema_name                     text,
        table_name                      text,
        put_columns                     sys_syn_dblink.create_put_column[],
        table_settings                  hstore)
        RETURNS text AS
$BODY$
DECLARE
        _table_name_sql         TEXT;
        _sql_buffer             TEXT;
        _columns_id             sys_syn_dblink.create_put_column[];
        _columns_attribute      sys_syn_dblink.create_put_column[];
BEGIN
        _table_name_sql         := schema_name::text || '.' || quote_ident(table_name);
        _columns_id             := sys_syn_dblink.put_columns_query_unique_index(put_columns);
        _columns_attribute      := sys_syn_dblink.put_columns_query(
                                        put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE);

        _sql_buffer := $$CREATE TABLE $$||_table_name_sql||$$ (
        $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME% %FORMAT_TYPE%,
        ', '')||sys_syn_dblink.put_columns_format(_columns_attribute, '%COLUMN_NAME% %FORMAT_TYPE%,
        ', '')||$$PRIMARY KEY ($$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ', ')||$$)
);
$$;

        RETURN _sql_buffer;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.table_create_sql_direct(text, text, sys_syn_dblink.create_put_column[], hstore)
  IS '';

CREATE FUNCTION sys_syn_dblink.table_drop_sql_direct (
        schema_name                     text,
        table_name                      text,
        table_settings                  hstore)
        RETURNS void AS
$BODY$
BEGIN
        EXECUTE 'DROP TABLE ' || schema_name::text || '.' || quote_ident(table_name);
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.table_drop_sql_direct(text, text, hstore)
  IS '';

CREATE FUNCTION sys_syn_dblink.put_sql_direct (
        schema_name                     text,
        table_name                      text,
        put_columns                     sys_syn_dblink.create_put_column[],
        schema_processed_name           text,
        type_id_name                    text,
        type_attributes_name            text,
        type_no_diff_name               text,
        table_settings                  hstore)
        RETURNS sys_syn_dblink.put_code_sql AS
$BODY$
DECLARE
        _table_name_sql         TEXT;
        _columns_id             sys_syn_dblink.create_put_column[];
        _columns_attribute      sys_syn_dblink.create_put_column[];
        _put_code_sql           sys_syn_dblink.put_code_sql;
        _put_code_attribute_sql sys_syn_dblink.put_code_sql;
BEGIN
        _table_name_sql         := schema_name::text || '.' || quote_ident(table_name);
        _columns_id             := sys_syn_dblink.put_columns_query_unique_index(put_columns);
        _columns_attribute      := sys_syn_dblink.put_columns_query(
                                        put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE);

        _put_code_attribute_sql := sys_syn_dblink.put_sql_expressions(
                ARRAY['Attribute']::sys_syn_dblink.in_column_type[],
                ARRAY['Add','Change','Delete']::sys_syn_dblink.delta_type[],
                2::smallint);

        _put_code_sql.declarations_sql := _put_code_attribute_sql.declarations_sql;
        _put_code_sql.logic_sql := $$
        IF delta_type = 'Delete'::sys_syn_dblink.delta_type THEN
                DELETE FROM $$||_table_name_sql||$$ AS out_table
                WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id,
        'out_table.%COLUMN_NAME% = %VALUE_EXPRESSION%', ' AND
                ')||$$;
        ELSE$$||_put_code_attribute_sql.logic_sql||$$
                INSERT INTO $$||_table_name_sql||$$ AS out_table (
                        $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ',
                        ')||sys_syn_dblink.put_columns_format(_columns_attribute, ',
                        %COLUMN_NAME%', '')||$$)
                VALUES ($$||sys_syn_dblink.put_columns_format(_columns_id, '%VALUE_EXPRESSION%', ',
                        ')||sys_syn_dblink.put_columns_format(_columns_attribute, ',
                        %VALUE_EXPRESSION%', '')||$$)
                ON CONFLICT ON CONSTRAINT $$||quote_ident(
                        sys_syn_dblink.table_primary_key_name(schema_name::regnamespace, table_name))||$$ DO UPDATE
                SET     $$||sys_syn_dblink.put_columns_format(_columns_attribute, '%COLUMN_NAME% = EXCLUDED.%COLUMN_NAME%', ',
                        ')||$$
                WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id,
        'out_table.%COLUMN_NAME% = %VALUE_EXPRESSION%', ' AND
                ')||$$;
        END IF;
$$;

        RETURN _put_code_sql;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.put_sql_direct(text, text, sys_syn_dblink.create_put_column[], text, text, text, text, hstore)
  IS '';

--- direct_array

CREATE FUNCTION sys_syn_dblink.table_create_sql_direct_array (
        schema_name                     text,
        table_name                      text,
        put_columns                     sys_syn_dblink.create_put_column[],
        table_settings                  hstore)
        RETURNS text AS
$BODY$
DECLARE
        _table_name_sql         TEXT;
        _columns_id             sys_syn_dblink.create_put_column[];
        _columns_attribute      sys_syn_dblink.create_put_column[];
        _sql_buffer             TEXT;
BEGIN
        _table_name_sql         := schema_name::text || '.' || quote_ident(table_name);
        _columns_id             := sys_syn_dblink.put_columns_query_unique_index(put_columns, TRUE);

        _sql_buffer := $$CREATE TABLE $$||_table_name_sql||$$ (
        $$||sys_syn_dblink.put_columns_format(put_columns, '%COLUMN_NAME% %FORMAT_TYPE%,
        ', '')||$$PRIMARY KEY ($$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ', ')||$$)
);
$$;

        RETURN _sql_buffer;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.table_create_sql_direct_array(text, text, sys_syn_dblink.create_put_column[], hstore)
  IS '';

CREATE FUNCTION sys_syn_dblink.table_drop_sql_direct_array (
        schema_name                     text,
        table_name                      text,
        table_settings                  hstore)
        RETURNS void AS
$BODY$
BEGIN
        EXECUTE 'DROP TABLE ' || schema_name::text || '.' || quote_ident(table_name);
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.table_drop_sql_direct_array(text, text, hstore)
  IS '';

CREATE FUNCTION sys_syn_dblink.put_sql_direct_array (
        schema_name                     text,
        table_name                      text,
        put_columns                     sys_syn_dblink.create_put_column[],
        schema_processed_name           text,
        type_id_name                    text,
        type_attributes_name            text,
        type_no_diff_name               text,
        table_settings                  hstore)
        RETURNS sys_syn_dblink.put_code_sql AS
$BODY$
DECLARE
        _table_name_sql         TEXT;
        _columns_id             sys_syn_dblink.create_put_column[];
        _columns_attribute      sys_syn_dblink.create_put_column[];
        _put_code_sql           sys_syn_dblink.put_code_sql;
BEGIN
        _table_name_sql         := schema_name::text || '.' || quote_ident(table_name);
        _columns_id             := sys_syn_dblink.put_columns_query_unique_index(put_columns);
        _columns_attribute      := sys_syn_dblink.put_columns_query(
                                        put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE);

        _put_code_sql.declarations_sql := '';
        _put_code_sql.logic_sql := $$
        DELETE FROM $$||_table_name_sql||$$ AS out_table
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id,
        'out_table.%COLUMN_NAME% = %VALUE_EXPRESSION%', ' AND
                ')||$$;

        IF delta_type != 'Delete'::sys_syn_dblink.delta_type THEN
                INSERT INTO $$||_table_name_sql||$$ AS out_table (
                        $$||sys_syn_dblink.put_columns_format(put_columns, '%COLUMN_NAME%', ',
                        ')||$$)
                SELECT  $$||sys_syn_dblink.put_columns_format(put_columns, '%VALUE_EXPRESSION%', ',
                        ')||$$
                FROM    unnest(attributes) AS attribute_rows;
        END IF;$$;

        RETURN _put_code_sql;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.put_sql_direct_array(text, text, sys_syn_dblink.create_put_column[], text, text, text, text,
        hstore) IS '';

--- temporal

CREATE FUNCTION sys_syn_dblink.table_create_sql_temporal (
        schema_name                     text,
        table_name                      text,
        put_columns                     sys_syn_dblink.create_put_column[],
        table_settings                  hstore)
        RETURNS text AS
$BODY$
DECLARE
        _table_name                     TEXT;
        _table_name_sql                 TEXT;
        _table_name_history             TEXT;
        _table_name_history_sql         TEXT;
        _columns_id                     sys_syn_dblink.create_put_column[];
        _columns_orderby                sys_syn_dblink.create_put_column[];
        _columns_unordered              sys_syn_dblink.create_put_column[];
        _column_name_sys_range          TEXT;
        _column_name_sys_range_sql      TEXT;
        _sql_buffer                     TEXT;
BEGIN
        _table_name             := REPLACE(
                table_settings -> 'sys_syn.temporal.active_table_name',
                '%1',
                table_name);
        _table_name_sql         := schema_name::text || '.' || quote_ident(_table_name);
        _table_name_history             := REPLACE(
                table_settings -> 'sys_syn.temporal.history_table_name',
                '%1',
                table_name);
        _table_name_history_sql := schema_name::text || '.' || quote_ident(_table_name_history);
        _columns_id             := sys_syn_dblink.put_columns_query_unique_index(put_columns);
        _columns_orderby        := sys_syn_dblink.put_columns_query(
                                        put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, TRUE);
        _columns_unordered      := sys_syn_dblink.put_columns_query(
                                        put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, FALSE);

        IF array_length(_columns_orderby, 1) != 1 THEN
                RAISE EXCEPTION
                        'sys_syn_dblink.table_create_sql_temporal:  attribute columns with an array_order does not total to 1.'
                USING HINT = 'A temporal table must have exactly 1 array_order.';
        END IF;

        IF _columns_orderby[1].array_order != 1::smallint THEN
                RAISE EXCEPTION 'sys_syn_dblink.table_create_sql_temporal:  array_order must be 1.'
                USING HINT = 'Set the array_order to 1.';
        END IF;

        _column_name_sys_range          := _columns_orderby[1].column_name;
        _column_name_sys_range          := REPLACE(
                table_settings -> 'sys_syn.temporal.range_1.column_name',
                '%1',
                _column_name_sys_range);
        _column_name_sys_range_sql      := quote_ident(_column_name_sys_range);

        _sql_buffer := $$CREATE TABLE $$||_table_name_history_sql||$$ (
        $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME% %FORMAT_TYPE% NOT NULL,
        ', '')||_column_name_sys_range_sql||$$ tstzrange NOT NULL,
        $$||sys_syn_dblink.put_columns_format(_columns_unordered, '%COLUMN_NAME% %FORMAT_TYPE%,
        ', '')||$$CONSTRAINT $$||quote_ident(_table_name_history||'_overlap')||$$ EXCLUDE USING GIST (
                $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME% WITH =', ',
                ')||$$,
                $$||_column_name_sys_range_sql||$$ WITH &&
        )
);

CREATE TABLE $$||_table_name_sql||$$ () INHERITS ($$||_table_name_history_sql||$$);

ALTER TABLE $$||_table_name_sql||$$
        ALTER COLUMN $$||_column_name_sys_range_sql||$$ SET DEFAULT tstzrange(current_timestamp, null);

ALTER TABLE $$||_table_name_sql||$$
        ADD PRIMARY KEY ($$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ', ')||$$);

CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON $$||_table_name_sql||$$
FOR EACH ROW EXECUTE PROCEDURE versioning(
        $$||quote_literal(_column_name_sys_range)||$$, $$||quote_literal(_table_name_history_sql)||$$, true
);
$$;

        RETURN _sql_buffer;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.table_create_sql_temporal(text, text, sys_syn_dblink.create_put_column[], hstore)
  IS '';

CREATE FUNCTION sys_syn_dblink.table_drop_sql_temporal (
        schema_name                     text,
        table_name                      text,
        table_settings                  hstore)
        RETURNS void AS
$BODY$
DECLARE
        _table_name                     TEXT;
        _table_name_sql                 TEXT;
        _table_name_history             TEXT;
        _table_name_history_sql         TEXT;
BEGIN
        _table_name             := REPLACE(
                table_settings -> 'sys_syn.temporal.active_table_name',
                '%1',
                table_name);
        _table_name_sql         := schema_name::text || '.' || quote_ident(_table_name);
        _table_name_history             := REPLACE(
                table_settings -> 'sys_syn.temporal.history_table_name',
                '%1',
                table_name);
        _table_name_history_sql := schema_name::text || '.' || quote_ident(_table_name_history);

        EXECUTE 'DROP TABLE ' || _table_name_sql;
        EXECUTE 'DROP TABLE ' || _table_name_history_sql;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.table_drop_sql_temporal(text, text, hstore)
  IS '';

CREATE FUNCTION sys_syn_dblink.put_sql_temporal (
        schema_name                     text,
        table_name                      text,
        put_columns                     sys_syn_dblink.create_put_column[],
        schema_processed_name           text,
        type_id_name                    text,
        type_attributes_name            text,
        type_no_diff_name               text,
        table_settings                  hstore)
        RETURNS sys_syn_dblink.put_code_sql AS
$BODY$
DECLARE
        _table_name                     TEXT;
        _table_name_sql                 TEXT;
        _table_name_history             TEXT;
        _table_name_history_sql         TEXT;
        _columns_id                     sys_syn_dblink.create_put_column[];
        _columns_orderby                sys_syn_dblink.create_put_column[];
        _columns_unordered              sys_syn_dblink.create_put_column[];
        _column_name_sys_range          TEXT;
        _column_name_sys_range_sql      TEXT;
        _put_code_sql                   sys_syn_dblink.put_code_sql;
        _array_index                    INTEGER;
BEGIN
        _table_name             := REPLACE(
                table_settings -> 'sys_syn.temporal.active_table_name',
                '%1',
                table_name);
        _table_name_sql         := schema_name::text || '.' || quote_ident(_table_name);
        _table_name_history             := REPLACE(
                table_settings -> 'sys_syn.temporal.history_table_name',
                '%1',
                table_name);
        _table_name_history_sql := schema_name::text || '.' || quote_ident(_table_name_history);
        _columns_id             := sys_syn_dblink.put_columns_query_unique_index(put_columns);
        _columns_orderby        := sys_syn_dblink.put_columns_query(
                                        put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, TRUE);
        _columns_unordered      := sys_syn_dblink.put_columns_query(
                                        put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, FALSE);

        _column_name_sys_range          := _columns_orderby[1].column_name;
        _column_name_sys_range          := REPLACE(
                table_settings -> 'sys_syn.temporal.range_1.column_name',
                '%1',
                _column_name_sys_range);
        _column_name_sys_range_sql      := quote_ident(_column_name_sys_range);

        _put_code_sql.declarations_sql := '';
        _put_code_sql.logic_sql := $$
        DELETE FROM $$||_table_name_sql||$$ AS out_table
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id,
        'out_table.%COLUMN_NAME% = %VALUE_EXPRESSION%', ' AND
                ')||$$;

        DELETE FROM $$||_table_name_history_sql||$$ AS out_table
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id,
        'out_table.%COLUMN_NAME% = %VALUE_EXPRESSION%', ' AND
                ')||$$;

        IF delta_type != 'Delete'::sys_syn_dblink.delta_type THEN
                PERFORM set_system_time($$||_columns_orderby[1].value_expression||$$);

                INSERT INTO $$||_table_name_sql||$$ AS out_table (
                        $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ',
                        ')||sys_syn_dblink.put_columns_format(_columns_unordered, ',
                        %COLUMN_NAME%', '')||$$)
                SELECT  $$||sys_syn_dblink.put_columns_format(_columns_id, '%VALUE_EXPRESSION%', ',
                        ')||sys_syn_dblink.put_columns_format(_columns_unordered, ',
                        %VALUE_EXPRESSION%', '')||$$;

                PERFORM set_system_time(NULL);
        END IF;$$;

        RETURN _put_code_sql;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.put_sql_temporal(text, text, sys_syn_dblink.create_put_column[], text, text, text, text, hstore)
  IS '';

CREATE FUNCTION sys_syn_dblink.put_sql_temporal_array (
        schema_name                     text,
        table_name                      text,
        put_columns                     sys_syn_dblink.create_put_column[],
        schema_processed_name           text,
        type_id_name                    text,
        type_attributes_name            text,
        type_no_diff_name               text,
        table_settings                  hstore)
        RETURNS sys_syn_dblink.put_code_sql AS
$BODY$
DECLARE
        _table_name                     TEXT;
        _table_name_sql                 TEXT;
        _table_name_history             TEXT;
        _table_name_history_sql         TEXT;
        _columns_id                     sys_syn_dblink.create_put_column[];
        _columns_orderby                sys_syn_dblink.create_put_column[];
        _columns_unordered              sys_syn_dblink.create_put_column[];
        _column_name_sys_range          TEXT;
        _column_name_sys_range_sql      TEXT;
        _put_code_sql                   sys_syn_dblink.put_code_sql;
        _array_index                    INTEGER;
BEGIN
        _table_name             := REPLACE(
                table_settings -> 'sys_syn.temporal.active_table_name',
                '%1',
                table_name);
        _table_name_sql         := schema_name::text || '.' || quote_ident(_table_name);
        _table_name_history             := REPLACE(
                table_settings -> 'sys_syn.temporal.history_table_name',
                '%1',
                table_name);
        _table_name_history_sql := schema_name::text || '.' || quote_ident(_table_name_history);
        _columns_id             := sys_syn_dblink.put_columns_query_unique_index(put_columns);
        _columns_orderby        := sys_syn_dblink.put_columns_query(
                                        put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, TRUE);
        _columns_unordered      := sys_syn_dblink.put_columns_query(
                                        put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, FALSE);

        _column_name_sys_range          := _columns_orderby[1].column_name;
        _column_name_sys_range          := REPLACE(
                table_settings -> 'sys_syn.temporal.range_1.column_name',
                '%1',
                _column_name_sys_range);
        _column_name_sys_range_sql      := quote_ident(_column_name_sys_range);

        _put_code_sql.declarations_sql := $$
        attribute_rows $$||quote_ident(schema_processed_name)||'.'||quote_ident(type_attributes_name)||$$;
        _system_time            timestamp with time zone;
        _system_time_future     timestamp with time zone;$$;
        _put_code_sql.logic_sql := $$
        DELETE FROM $$||_table_name_sql||$$ AS out_table
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id,
        'out_table.%COLUMN_NAME% = %VALUE_EXPRESSION%', ' AND
                ')||$$;

        DELETE FROM $$||_table_name_history_sql||$$ AS out_table
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id,
        'out_table.%COLUMN_NAME% = %VALUE_EXPRESSION%', ' AND
                ')||$$;

        IF delta_type != 'Delete'::sys_syn_dblink.delta_type THEN
                attribute_rows := attributes[array_length(attributes, 1)];
                _system_time := $$||_columns_orderby[1].value_expression||$$;

                PERFORM set_system_time(_system_time);

                INSERT INTO $$||_table_name_sql||$$ AS out_table (
                        $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ',
                        ')||sys_syn_dblink.put_columns_format(_columns_unordered, ',
                        %COLUMN_NAME%', '')||$$)
                SELECT  $$||sys_syn_dblink.put_columns_format(_columns_id, '%VALUE_EXPRESSION%', ',
                        ')||sys_syn_dblink.put_columns_format(_columns_unordered, ',
                        %VALUE_EXPRESSION%', '')||$$;

                FOR _array_index IN REVERSE array_length(attributes, 1) - 1 .. 1 LOOP
                        _system_time_future     := _system_time;
                        attribute_rows          := attributes[_array_index];
                        _system_time            := $$||_columns_orderby[1].value_expression||$$;

                        IF _system_time >= _system_time_future THEN
                                RAISE EXCEPTION 'The system time value is not equal to the next record.'
                                USING HINT = 'Fix the data so that every record has a unique system time value.';
                        END IF;

                        INSERT INTO $$||_table_name_history_sql||$$ AS out_table (
                                $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%,
                                ', '')||_column_name_sys_range_sql||
                                sys_syn_dblink.put_columns_format(_columns_unordered, ',
                                %COLUMN_NAME%', '')||$$)
                        SELECT  $$||sys_syn_dblink.put_columns_format(_columns_id, '%VALUE_EXPRESSION%,
                                ', '')||$$tstzrange(_system_time, _system_time_future)$$||
                                sys_syn_dblink.put_columns_format(_columns_unordered, ',
                                %VALUE_EXPRESSION%', '')||$$;
                END LOOP;

                PERFORM set_system_time(NULL);
        END IF;$$;

        RETURN _put_code_sql;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.put_sql_temporal_array(text, text, sys_syn_dblink.create_put_column[], text, text, text, text,
        hstore) IS '';

-- bitemporal

CREATE FUNCTION sys_syn_dblink.bitemporal_prefer_following (
        active_ranges   daterange[],
        exact_value     DATE,
        following_max   DATE,
        prior_min       DATE) RETURNS daterange AS $$
DECLARE
        _return daterange;
BEGIN
        _return := (SELECT * FROM unnest(active_ranges) AS ranges(range) WHERE ranges.range @> exact_value);
        IF _return IS NOT NULL THEN
                RETURN _return;
        END IF;

        _return := (
                SELECT  ranges.range
                FROM    unnest(active_ranges) AS ranges(range)
                WHERE   lower(ranges.range) > exact_value AND
                        lower(ranges.range) <= following_max
                ORDER BY 1
                LIMIT   1
        );
        IF _return IS NOT NULL THEN
                RETURN _return;
        END IF;

        _return := (
                SELECT  ranges.range
                FROM    unnest(active_ranges) AS ranges(range)
                WHERE   upper(ranges.range) < exact_value AND
                        upper(ranges.range) >= prior_min
                ORDER BY 1 DESC
                LIMIT   1
        );
        RETURN _return;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
COMMENT ON FUNCTION sys_syn_dblink.bitemporal_prefer_following(daterange[], DATE, DATE, DATE)
  IS '';

CREATE FUNCTION sys_syn_dblink.table_create_sql_bitemporal (
        schema_name                     text,
        table_name                      text,
        put_columns                     sys_syn_dblink.create_put_column[],
        table_settings                  hstore)
        RETURNS text AS
$BODY$
DECLARE
        _table_name                     TEXT;
        _table_name_sql                 TEXT;
        _table_name_immutable           TEXT;
        _table_name_immutable_sql       TEXT;
        _table_name_history             TEXT;
        _table_name_history_sql         TEXT;
        _view_name_current              TEXT;
        _view_name_current_sql          TEXT;
        _view_name_active               TEXT;
        _view_name_active_sql           TEXT;
        _columns_id                     sys_syn_dblink.create_put_column[];
        _columns_orderby                sys_syn_dblink.create_put_column[];
        _columns_unordered              sys_syn_dblink.create_put_column[];
        _column_name_sys_range          TEXT;
        _column_name_sys_range_sql      TEXT;
        _column_name_sys_lower          TEXT;
        _column_name_sys_lower_sql      TEXT;
        _column_type_range_2            TEXT;
        _column_name_range_2            TEXT;
        _column_name_range_2_sql        TEXT;
        _column_name_range_2_active     TEXT;
        _column_name_range_2_active_sql TEXT;
        _trigger_name_insert            TEXT;
        _trigger_name_insert_sql        TEXT;
        _trigger_name_update            TEXT;
        _trigger_name_update_sql        TEXT;
        _trigger_name_delete            TEXT;
        _trigger_name_delete_sql        TEXT;
        _sql_buffer                     TEXT;
BEGIN
        _table_name                     := REPLACE(
                table_settings -> 'sys_syn.bitemporal.active_table_name',
                '%1',
                table_name);
        _table_name_sql                 := schema_name::text || '.' || quote_ident(_table_name);
        _table_name_immutable           := REPLACE(
                table_settings -> 'sys_syn.bitemporal.immutable_table_name',
                '%1',
                table_name);
        _table_name_immutable_sql       := schema_name::text || '.' || quote_ident(table_name||'_immutable');
        _table_name_history             := REPLACE(
                table_settings -> 'sys_syn.bitemporal.history_table_name',
                '%1',
                table_name);
        _table_name_history_sql         := schema_name::text || '.' || quote_ident(_table_name_history);
        _columns_id                     := sys_syn_dblink.put_columns_query_unique_index(put_columns);
        _columns_orderby                := sys_syn_dblink.put_columns_query(
                                                put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, TRUE);
        _columns_unordered              := sys_syn_dblink.put_columns_query(
                                                put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, FALSE);
        _view_name_current              := REPLACE(
                table_settings -> 'sys_syn.bitemporal.current_view_name',
                '%1',
                table_name);
        _view_name_current_sql          := schema_name::text || '.' || quote_ident(_view_name_current);
        _view_name_active               := REPLACE(
                table_settings -> 'sys_syn.bitemporal.active_view_name',
                '%1',
                table_name);
        _view_name_active_sql           := schema_name::text || '.' || quote_ident(_view_name_active);
        _trigger_name_insert            := table_name||'_current_insert';
        _trigger_name_insert_sql        := schema_name::text || '.' || quote_ident(_trigger_name_insert);
        _trigger_name_update            := table_name||'_current_update';
        _trigger_name_update_sql        := schema_name::text || '.' || quote_ident(_trigger_name_update);
        _trigger_name_delete            := table_name||'_current_delete';
        _trigger_name_delete_sql        := schema_name::text || '.' || quote_ident(_trigger_name_delete);

        IF array_length(_columns_orderby, 1) != 3 THEN
                RAISE EXCEPTION
                        'sys_syn_dblink.table_create_sql_bitemporal:  attribute columns with an array_order does not total to 3.'
                USING HINT = 'A bitemporal table must have exactly 3 array_order.';
        END IF;

        IF _columns_orderby[1].array_order != 1::smallint THEN
                RAISE EXCEPTION 'sys_syn_dblink.table_create_sql_bitemporal:  array_order must be 1.'
                USING HINT = 'Set the array_order to 1.';
        END IF;
        IF _columns_orderby[2].array_order != 2::smallint THEN
                RAISE EXCEPTION 'sys_syn_dblink.table_create_sql_bitemporal:  array_order must be 2.'
                USING HINT = 'Set the array_order to 2.';
        END IF;
        IF _columns_orderby[3].array_order != 3::smallint THEN
                RAISE EXCEPTION 'sys_syn_dblink.table_create_sql_bitemporal:  array_order must be 3.'
                USING HINT = 'Set the array_order to 3.';
        END IF;

        _column_name_sys_range          := _columns_orderby[1].column_name;
        _column_name_sys_range          := REPLACE(
                table_settings -> 'sys_syn.bitemporal.range_1.column_name',
                '%1',
                _column_name_sys_range);
        _column_name_sys_range_sql      := quote_ident(_column_name_sys_range);

        _column_name_sys_lower          := _columns_orderby[1].column_name;
        _column_name_sys_lower          := REPLACE(
                table_settings -> 'sys_syn.bitemporal.range_1.lower.column_name',
                '%1',
                _column_name_sys_lower);
        _column_name_sys_lower_sql      := quote_ident(_column_name_sys_lower);

        _column_name_range_2            := _columns_orderby[2].column_name;
        _column_name_range_2            := REPLACE(
                table_settings -> 'sys_syn.bitemporal.range_2.column_name',
                '%1',
                _column_name_range_2);
        _column_name_range_2_sql        := quote_ident(_column_name_range_2);

        _column_type_range_2            := sys_syn_dblink.range_from_data_type(_columns_orderby[2].data_type);

        _column_name_range_2_active     := _column_name_range_2 || 's';
        _column_name_range_2_active     := REPLACE(
                table_settings -> 'sys_syn.bitemporal.range_2_active.column_name',
                '%1',
                _column_name_range_2_active);
        _column_name_range_2_active_sql := quote_ident(_column_name_range_2_active);

        _sql_buffer := 'CREATE TABLE '||_table_name_immutable_sql||$$ (
        $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME% %FORMAT_TYPE% NOT NULL,
        ', '')||_column_name_range_2_active_sql||' '||_column_type_range_2||$$[] NOT NULL,
        PRIMARY KEY ($$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ', ')||$$)
);

CREATE TABLE $$||_table_name_history_sql||$$ (
        $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME% %FORMAT_TYPE% NOT NULL,
        ', '')||_column_name_sys_range_sql||$$ tstzrange NOT NULL,
        $$||_column_name_range_2_sql||' '||_column_type_range_2||' NOT NULL,
        '||sys_syn_dblink.put_columns_format(_columns_unordered, '%COLUMN_NAME% %FORMAT_TYPE%,
        ', '')||$$CONSTRAINT $$||quote_ident(_table_name_history||'_overlap')||$$ EXCLUDE USING GIST (
                $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME% WITH =', ',
        ')||$$,
        $$||_column_name_sys_range_sql||$$ WITH &&,
        $$||_column_name_range_2_sql||$$ WITH &&
        ),
        FOREIGN KEY ($$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ', ')||$$) REFERENCES $$||
        _table_name_immutable_sql||$$ ($$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ', ')||
        $$) ON UPDATE RESTRICT ON DELETE RESTRICT
);
ALTER TABLE $$||_table_name_history_sql||$$ CLUSTER ON $$||quote_ident(_table_name_history||'_overlap')||$$;

CREATE TABLE $$||_table_name_sql||$$ (
        FOREIGN KEY ($$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ', ')||$$) REFERENCES $$||
        _table_name_immutable_sql||$$ ($$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ', ')||
        $$) ON UPDATE RESTRICT ON DELETE RESTRICT,
        CONSTRAINT $$||quote_ident(table_name||'_overlap')||$$ EXCLUDE USING GIST (
                $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME% WITH =', ',
        ')||$$,
        $$||_column_name_range_2_sql||$$ WITH &&
    )
) INHERITS ($$||_table_name_history_sql||$$);
ALTER TABLE $$||_table_name_sql||$$ CLUSTER ON $$||quote_ident(table_name||'_overlap')||$$;
ALTER TABLE $$||_table_name_sql||$$
        ALTER COLUMN $$||_column_name_sys_range_sql||$$ SET DEFAULT tstzrange(current_timestamp, null);

CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON $$||_table_name_sql||$$
FOR EACH ROW EXECUTE PROCEDURE versioning(
        $$||quote_literal(_column_name_sys_range)||$$, $$||quote_literal(_table_name_history_sql)||$$, true
);

CREATE OR REPLACE VIEW $$||_view_name_current_sql||$$ AS
SELECT  $$||sys_syn_dblink.put_columns_format(_columns_id, 'immutable_table.%COLUMN_NAME%,
        ', '')||$$lower(active_table.$$||_column_name_sys_range_sql||$$) AS $$||_column_name_sys_lower_sql||$$,
        $$||'active_table.'||_column_name_range_2_sql||sys_syn_dblink.put_columns_format(_columns_unordered, $$,
        active_table.%COLUMN_NAME%$$, '')||$$
FROM    $$||_table_name_immutable_sql||$$ AS immutable_table
JOIN    $$||_table_name_sql||$$ AS active_table ON
        $$||sys_syn_dblink.put_columns_format(_columns_id, $$active_table.%COLUMN_NAME% = immutable_table.%COLUMN_NAME% AND
        $$, '')||$$sys_syn_dblink.bitemporal_prefer_following(
                immutable_table.$$||_column_name_range_2_active_sql||
                $$, current_date, 'infinity'::DATE, '-infinity'::DATE) = active_table.$$||_column_name_range_2_sql||$$;

CREATE OR REPLACE VIEW $$||_view_name_active_sql||$$ AS
SELECT  $$||sys_syn_dblink.put_columns_format(_columns_id, 'immutable_table.%COLUMN_NAME%,
        ', '')||$$lower(active_table.$$||_column_name_sys_range_sql||$$) AS $$||_column_name_sys_lower_sql||$$,
        $$||'active_table.'||_column_name_range_2_sql||sys_syn_dblink.put_columns_format(_columns_unordered, $$,
        active_table.%COLUMN_NAME%$$, '')||$$
FROM    $$||_table_name_immutable_sql||$$ AS immutable_table
JOIN    $$||_table_name_sql||$$ AS active_table ON
        $$||sys_syn_dblink.put_columns_format(_columns_id, $$active_table.%COLUMN_NAME% = immutable_table.%COLUMN_NAME%$$, $$ AND
        $$)||$$;

CREATE FUNCTION $$||_trigger_name_insert_sql||$$() RETURNS trigger AS $DEFINITION$
BEGIN
        INSERT INTO $$||_table_name_immutable_sql||$$ (
                $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ', ')||$$,
                $$||_column_name_range_2_active_sql||$$)
        VALUES (
                $$||sys_syn_dblink.put_columns_format(_columns_id, 'new.%COLUMN_NAME%', ', ')||$$,
                ARRAY[new.$$||_column_name_range_2_sql||$$]);

        INSERT INTO $$||_table_name_sql||$$ (
                $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ', ')||$$,
                $$||_column_name_range_2_sql||$$
                $$||sys_syn_dblink.put_columns_format(_columns_unordered, ', %COLUMN_NAME%', '')||$$)
        VALUES (
                $$||sys_syn_dblink.put_columns_format(_columns_id, 'new.%COLUMN_NAME%', ', ')||$$,
                new.$$||_column_name_range_2_sql||$$
                $$||sys_syn_dblink.put_columns_format(_columns_unordered, ', new.%COLUMN_NAME%', '')||$$);

        RETURN new;
END;
$DEFINITION$ LANGUAGE plpgsql;

CREATE TRIGGER $$||_trigger_name_insert||$$
    INSTEAD OF INSERT ON $$||_view_name_current_sql||$$
    FOR EACH ROW
    EXECUTE PROCEDURE $$||_trigger_name_insert_sql||$$();

CREATE FUNCTION $$||_trigger_name_update_sql||$$() RETURNS trigger AS $DEFINITION$
BEGIN
        DELETE
        FROM    $$||_table_name_sql||$$ AS active_table
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id, 'active_table.%COLUMN_NAME% = new.%COLUMN_NAME% AND
                ', '')||
                $$active_table.$$||_column_name_range_2_sql||$$ <@ new.$$||_column_name_range_2_sql||$$;

        UPDATE  $$||_table_name_sql||$$ AS active_table
        SET     $$||_column_name_range_2_sql||$$ =
                        daterange(lower(active_table.$$||_column_name_range_2_sql||$$), lower(new.$$||_column_name_range_2_sql||$$))
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id, 'active_table.%COLUMN_NAME% = new.%COLUMN_NAME% AND
                ', '')||
                _column_name_range_2_sql||$$ @> lower(new.$$||_column_name_range_2_sql||$$);

        UPDATE  $$||_table_name_sql||$$ AS active_table
        SET     $$||_column_name_range_2_sql||$$ =
                        daterange(upper(new.$$||_column_name_range_2_sql||$$), upper(active_table.$$||_column_name_range_2_sql||$$))
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id, 'active_table.%COLUMN_NAME% = new.%COLUMN_NAME% AND
                ', '')||
                $$active_table.$$||_column_name_range_2_sql||$$ @> upper(new.$$||_column_name_range_2_sql||$$);

        INSERT INTO $$||_table_name_sql||$$ (
                $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ', ')||$$,
                $$||_column_name_range_2_sql||$$
                $$||sys_syn_dblink.put_columns_format(_columns_unordered, ', %COLUMN_NAME%', '')||$$)
        VALUES (
                $$||sys_syn_dblink.put_columns_format(_columns_id, 'new.%COLUMN_NAME%', ', ')||$$,
                new.$$||_column_name_range_2_sql||$$
                $$||sys_syn_dblink.put_columns_format(_columns_unordered, ', new.%COLUMN_NAME%', '')||$$);

        UPDATE  $$||_table_name_immutable_sql||$$ AS immutable_table
        SET     $$||_column_name_range_2_active_sql||$$ = (
                        SELECT  array_agg($$||_column_name_range_2_sql||$$ ORDER BY $$||_column_name_range_2_sql||$$ DESC)
                        FROM    $$||_table_name_sql||$$ AS active_table
                        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id,
                                'active_table.%COLUMN_NAME% = new.%COLUMN_NAME%', ' AND
                ')||$$)
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id, 'immutable_table.%COLUMN_NAME% = new.%COLUMN_NAME%', ' AND
                ')||$$;

        RETURN new;
END;
$DEFINITION$ LANGUAGE plpgsql;

CREATE TRIGGER $$||_trigger_name_update||$$
        INSTEAD OF UPDATE ON $$||_view_name_current_sql||$$
        FOR EACH ROW
        EXECUTE PROCEDURE $$||_trigger_name_update_sql||$$();

CREATE FUNCTION $$||_trigger_name_delete_sql||$$() RETURNS trigger AS $DEFINITION$
BEGIN
        DELETE FROM $$||_table_name_sql||$$ AS active_table
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id, 'active_table.%COLUMN_NAME% = old.%COLUMN_NAME%', ' AND
                ')||$$;

        DELETE FROM $$||_table_name_history_sql||$$ AS history_table
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id, 'history_table.%COLUMN_NAME% = old.%COLUMN_NAME%', ' AND
                ')||$$;

        DELETE FROM $$||_table_name_immutable_sql||$$ AS immutable_table
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id, 'immutable_table.%COLUMN_NAME% = old.%COLUMN_NAME%', ' AND
                ')||$$;

        RETURN new;
END;
$DEFINITION$ LANGUAGE plpgsql;

CREATE TRIGGER $$||_trigger_name_delete||$$
        INSTEAD OF DELETE ON $$||_view_name_current_sql||$$
        FOR EACH ROW
        EXECUTE PROCEDURE $$||_trigger_name_delete_sql||$$();
$$;

        RETURN _sql_buffer;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.table_create_sql_bitemporal(text, text, sys_syn_dblink.create_put_column[], hstore)
  IS '';

CREATE FUNCTION sys_syn_dblink.table_drop_sql_bitemporal (
        schema_name                     text,
        table_name                      text,
        table_settings                  hstore)
        RETURNS void AS
$BODY$
DECLARE
        _table_name                     TEXT;
        _table_name_sql                 TEXT;
        _table_name_immutable           TEXT;
        _table_name_immutable_sql       TEXT;
        _table_name_history             TEXT;
        _table_name_history_sql         TEXT;
        _view_name_current              TEXT;
        _view_name_current_sql          TEXT;
        _view_name_active               TEXT;
        _view_name_active_sql           TEXT;
        _trigger_name_insert            TEXT;
        _trigger_name_insert_sql        TEXT;
        _trigger_name_update            TEXT;
        _trigger_name_update_sql        TEXT;
        _trigger_name_delete            TEXT;
        _trigger_name_delete_sql        TEXT;
BEGIN
        _table_name                     := REPLACE(
                table_settings -> 'sys_syn.bitemporal.active_table_name',
                '%1',
                table_name);
        _table_name_sql                 := schema_name::text || '.' || quote_ident(_table_name);
        _table_name_immutable           := REPLACE(
                table_settings -> 'sys_syn.bitemporal.immutable_table_name',
                '%1',
                table_name);
        _table_name_immutable_sql       := schema_name::text || '.' || quote_ident(table_name||'_immutable');
        _table_name_history             := REPLACE(
                table_settings -> 'sys_syn.bitemporal.history_table_name',
                '%1',
                table_name);
        _table_name_history_sql         := schema_name::text || '.' || quote_ident(_table_name_history);
        _view_name_current              := REPLACE(
                table_settings -> 'sys_syn.bitemporal.current_view_name',
                '%1',
                table_name);
        _view_name_current_sql          := schema_name::text || '.' || quote_ident(_view_name_current);
        _view_name_active               := REPLACE(
                table_settings -> 'sys_syn.bitemporal.active_view_name',
                '%1',
                table_name);
        _view_name_active_sql           := schema_name::text || '.' || quote_ident(_view_name_active);
        _trigger_name_insert            := table_name||'_current_insert';
        _trigger_name_insert_sql        := schema_name::text || '.' || quote_ident(_trigger_name_insert);
        _trigger_name_update            := table_name||'_current_update';
        _trigger_name_update_sql        := schema_name::text || '.' || quote_ident(_trigger_name_update);
        _trigger_name_delete            := table_name||'_current_delete';
        _trigger_name_delete_sql        := schema_name::text || '.' || quote_ident(_trigger_name_delete);

        EXECUTE 'DROP VIEW ' || _view_name_current_sql;
        EXECUTE 'DROP VIEW ' || _view_name_active_sql;
        EXECUTE 'DROP TABLE ' || _table_name_sql;
        EXECUTE 'DROP TABLE ' || _table_name_history_sql;
        EXECUTE 'DROP TABLE ' || _table_name_immutable_sql;
        EXECUTE 'DROP FUNCTION ' || _trigger_name_insert_sql || '()';
        EXECUTE 'DROP FUNCTION ' || _trigger_name_update_sql || '()';
        EXECUTE 'DROP FUNCTION ' || _trigger_name_delete_sql || '()';
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.table_drop_sql_bitemporal(text, text, hstore)
  IS '';

CREATE FUNCTION sys_syn_dblink.put_sql_bitemporal_array (
        schema_name                     text,
        table_name                      text,
        put_columns                     sys_syn_dblink.create_put_column[],
        schema_processed_name           text,
        type_id_name                    text,
        type_attributes_name            text,
        type_no_diff_name               text,
        table_settings                  hstore)
        RETURNS sys_syn_dblink.put_code_sql AS
$BODY$
DECLARE
        _table_name_sql                 TEXT;
        _table_name_history_sql         TEXT;
        _view_name_current_sql          TEXT;
        _columns_id                     sys_syn_dblink.create_put_column[];
        _columns_orderby                sys_syn_dblink.create_put_column[];
        _columns_unordered              sys_syn_dblink.create_put_column[];
        _column_name_range_2            TEXT;
        _column_name_range_2_sql        TEXT;
        _put_code_sql                   sys_syn_dblink.put_code_sql;
        _array_index                    INTEGER;
BEGIN
        _table_name_sql                 := schema_name::text || '.' || quote_ident(table_name);
        _table_name_history_sql         := schema_name::text || '.' || quote_ident(table_name || '_history');
        _view_name_current_sql          := schema_name::text || '.' || quote_ident(table_name||'_current');
        _columns_id                     := sys_syn_dblink.put_columns_query_unique_index(put_columns);
        _columns_orderby                := sys_syn_dblink.put_columns_query(
                                                put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, TRUE);
        _columns_unordered              := sys_syn_dblink.put_columns_query(
                                                put_columns, ARRAY['Id']::sys_syn_dblink.in_column_type[], TRUE, FALSE);
        _column_name_range_2            := _columns_orderby[2].column_name;
        _column_name_range_2            := REPLACE(
                table_settings -> 'sys_syn.bitemporal.range_2.column_name',
                '%1',
                _column_name_range_2);
        _column_name_range_2_sql        := quote_ident(_column_name_range_2);


        IF array_length(_columns_orderby, 1) != 3 THEN
                RAISE EXCEPTION
                        'sys_syn_dblink.table_create_sql_bitemporal:  attribute columns with an array_order does not total to 3.'
                USING HINT = 'A bitemporal table must have exactly 3 array_order.';
        END IF;

        IF _columns_orderby[1].array_order != 1::smallint THEN
                RAISE EXCEPTION 'sys_syn_dblink.table_create_sql_bitemporal:  array_order must be 1.'
                USING HINT = 'Set the array_order to 1.';
        END IF;
        IF _columns_orderby[2].array_order != 2::smallint THEN
                RAISE EXCEPTION 'sys_syn_dblink.table_create_sql_bitemporal:  array_order must be 2.'
                USING HINT = 'Set the array_order to 2.';
        END IF;
        IF _columns_orderby[3].array_order != 3::smallint THEN
                RAISE EXCEPTION 'sys_syn_dblink.table_create_sql_bitemporal:  array_order must be 3.'
                USING HINT = 'Set the array_order to 3.';
        END IF;

        _put_code_sql.declarations_sql := $$
        attribute_rows $$||quote_ident(schema_processed_name)||'.'||quote_ident(type_attributes_name)||$$;
        _system_time            timestamp with time zone;
        _system_time_prior      timestamp with time zone;$$;

        _put_code_sql.logic_sql := $$
        DELETE FROM $$||_view_name_current_sql||$$ AS out_table
        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id,
        'out_table.%COLUMN_NAME% = %VALUE_EXPRESSION%', ' AND
                ')||$$;

        IF delta_type != 'Delete'::sys_syn_dblink.delta_type THEN
                attribute_rows := attributes[1];
                _system_time := $$||_columns_orderby[1].value_expression||$$;

                PERFORM set_system_time(_system_time);

                INSERT INTO $$||_view_name_current_sql||$$ AS out_table (
                        $$||sys_syn_dblink.put_columns_format(_columns_id, '%COLUMN_NAME%', ',
                        ')||$$,
                        $$||_column_name_range_2_sql||$$,
                        $$||sys_syn_dblink.put_columns_format(_columns_unordered, '%COLUMN_NAME%', ',
                        ')||$$)
                SELECT  $$||sys_syn_dblink.put_columns_format(_columns_id, '%VALUE_EXPRESSION%', ',
                        ')||$$,
                        daterange($$||_columns_orderby[2].value_expression||$$, $$||_columns_orderby[3].value_expression||$$),
                        $$||sys_syn_dblink.put_columns_format(_columns_unordered, '%VALUE_EXPRESSION%', ',
                        ')||$$;

                FOR _array_index IN  2 .. array_length(attributes, 1) LOOP
                        _system_time_prior      := _system_time;
                        attribute_rows          := attributes[_array_index];
                        _system_time            := $$||_columns_orderby[1].value_expression||$$;

                        PERFORM set_system_time(_system_time);

                        IF _system_time <= _system_time_prior THEN
                                RAISE EXCEPTION 'The system time value is not equal to the next record.'
                                USING HINT = 'Fix the data so that every record has a unique system time value.';
                        END IF;

                        UPDATE  $$||_view_name_current_sql||$$ AS current_view
                        SET     $$||_column_name_range_2_sql||$$ = daterange($$||_columns_orderby[2].value_expression||$$, $$||
                                _columns_orderby[3].value_expression||$$)$$||sys_syn_dblink.put_columns_format(_columns_unordered,',
                                %COLUMN_NAME% = %VALUE_EXPRESSION%', '')||$$
                        WHERE   $$||sys_syn_dblink.put_columns_format(_columns_id,'current_view.%COLUMN_NAME% = %VALUE_EXPRESSION%',
                                ' AND
                                ')||$$;
                END LOOP;

                PERFORM set_system_time(NULL);
        END IF;$$;

        RETURN _put_code_sql;
END
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
COMMENT ON FUNCTION sys_syn_dblink.put_sql_bitemporal_array(text, text, sys_syn_dblink.create_put_column[], text, text, text, text,
        hstore)
  IS '';

-- end


CREATE FUNCTION sys_syn_dblink.column_type_value_row (
        proc_table_id           text,
        value_expression        text) RETURNS text
        AS $BODY$
DECLARE
        _proc_table_def         sys_syn_dblink.proc_tables_def;
BEGIN
        _proc_table_def := (
                SELECT  proc_tables_def
                FROM    sys_syn_dblink.proc_tables_def
                WHERE   proc_tables_def.proc_table_id = column_type_value_row.proc_table_id);

        IF _proc_table_def IS NULL THEN
                RAISE EXCEPTION 'sys_syn_dblink.column_type_value_row proc_tables_def not found.';
        END IF;

        RETURN 'quote_literal(' || value_expression || ') || ''::' || quote_ident(_proc_table_def.remote_schema) || '.' ||
                quote_ident(_proc_table_def.proc_table_id || '_in_id') || '''';
END
$BODY$
  LANGUAGE plpgsql STABLE
  COST 80;
COMMENT ON FUNCTION sys_syn_dblink.column_type_value_row(
        proc_table_id           text,
        value_expression        text)
  IS '';


CREATE FUNCTION sys_syn_dblink.proc_table_code (
        proc_table_def                          sys_syn_dblink.proc_tables_def,
        table_type_def                          sys_syn_dblink.table_types_def,
        proc_columns_id                         sys_syn_dblink.create_proc_column[],
        proc_columns_attribute                  sys_syn_dblink.create_proc_column[],
        proc_columns_attribute_orderby          sys_syn_dblink.create_proc_column[],
        proc_columns_attribute_unordered        sys_syn_dblink.create_proc_column[],
        proc_columns_nodiff                     sys_syn_dblink.create_proc_column[],
        put_columns                             sys_syn_dblink.create_put_column[],
        type_id_name                            TEXT,
        type_attributes_name                    TEXT,
        type_no_diff_name                       TEXT,
        table_settings                          hstore) RETURNS void
        LANGUAGE plpgsql
        AS $_$
DECLARE
        _sql_data_type_array    TEXT;
        _object_id              TEXT;
        _name_put               TEXT;
        _name_proc_id           TEXT;
        _name_proc_attributes   TEXT;
        _name_proc_no_diff      TEXT;
        _sql_buffer             TEXT;
        _put_code_sql           sys_syn_dblink.put_code_sql;
BEGIN
        _object_id              := proc_table_def.proc_table_id;

        _name_put               := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_put');
        _name_proc_id           := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_proc_id');
        _name_proc_attributes   := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_proc_attributes');
        _name_proc_no_diff      := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_proc_no_diff');

        EXECUTE 'SELECT * FROM '||table_type_def.put_sql_proc_schema::text||'.'||quote_ident(table_type_def.put_sql_proc_name)||
                '($1, $2, $3, $4, $5, $6, $7, $8)'
        INTO    _put_code_sql
        USING   proc_table_def.put_schema::text,        proc_table_def.put_table_name,
                put_columns,                            proc_table_def.proc_schema::text,
                type_id_name,                           type_attributes_name,                   type_no_diff_name,
                table_settings;

        IF proc_table_def.attributes_array THEN
                _sql_data_type_array := '[]';
        ELSE
                _sql_data_type_array := '';
        END IF;

        _sql_buffer := $$
CREATE FUNCTION $$||_name_put||$$(
        trans_id_in     integer,
        delta_type      sys_syn_dblink.delta_type,
        queue_priority  smallint,
        hold_updated    boolean,
        prior_hold_reason_count integer,
        prior_hold_reason_id    integer,
        prior_hold_reason_text  text,
        id              $$||_name_proc_id||$$,
        attributes      $$||_name_proc_attributes||_sql_data_type_array||$$,
        no_diff         $$||_name_proc_no_diff||$$)
        RETURNS sys_syn_dblink.processed_status AS
$DEFINITION$
DECLARE
        _processed_status       sys_syn_dblink.processed_status;
        _exception_sql_state    text;
        _exception_message      text;
        _exception_detail       text;
        _exception_hint         text;
        _exception_context      text;$$||_put_code_sql.declarations_sql||$$
BEGIN$$||_put_code_sql.logic_sql||$$

        RETURN _processed_status;
EXCEPTION WHEN others THEN
        GET STACKED DIAGNOSTICS
                _exception_sql_state    = RETURNED_SQLSTATE,    _exception_message      = MESSAGE_TEXT,
                _exception_detail       = PG_EXCEPTION_DETAIL,  _exception_hint         = PG_EXCEPTION_HINT,
                _exception_context      = PG_EXCEPTION_CONTEXT;
        _processed_status.hold_reason_text := 'ERROR:  '||COALESCE(COALESCE(_exception_message, '')||COALESCE('
SQL state:  '||NULLIF(_exception_sql_state,''), '')||COALESCE('
Detail:  '||NULLIF(_exception_detail,''), '')||COALESCE('
Hint:  '||NULLIF(_exception_hint,''), '')||COALESCE('
Context:  '||NULLIF(_exception_context,''), ''), 'Null');
        RETURN _processed_status;
END
$DEFINITION$
        LANGUAGE plpgsql VOLATILE
        COST 5000;
$$;
        EXECUTE _sql_buffer;
END;
$_$;
COMMENT ON FUNCTION sys_syn_dblink.proc_table_code(sys_syn_dblink.proc_tables_def, sys_syn_dblink.table_types_def,
        sys_syn_dblink.create_proc_column[], sys_syn_dblink.create_proc_column[], sys_syn_dblink.create_proc_column[],
        sys_syn_dblink.create_proc_column[], sys_syn_dblink.create_proc_column[], sys_syn_dblink.create_put_column[],
        text, text, text, hstore)
  IS '';

CREATE FUNCTION sys_syn_dblink.proc_table_code (
        proc_table_def                          sys_syn_dblink.proc_tables_def,
        table_type_def                          sys_syn_dblink.table_types_def,
        proc_columns_id                         sys_syn_dblink.create_proc_column[],
        proc_columns_attribute                  sys_syn_dblink.create_proc_column[],
        proc_columns_attribute_orderby          sys_syn_dblink.create_proc_column[],
        proc_columns_attribute_unordered        sys_syn_dblink.create_proc_column[],
        proc_columns_nodiff                     sys_syn_dblink.create_proc_column[],
        put_columns                             sys_syn_dblink.create_put_column[],
        type_id_name                            TEXT,
        type_attributes_name                    TEXT,
        type_no_diff_name                       TEXT,
        table_settings                          hstore,
        worker_id                               smallint,
        partition_id                            smallint) RETURNS void
        LANGUAGE plpgsql
        AS $_$
DECLARE
        _sql_attributes_insert  TEXT;
        _sql_attributes_select  TEXT;
        _sql_remote_array       TEXT;
        _sql_dblink_array       TEXT;
        _sql_group_by           TEXT;
        _sql_data_type_array    TEXT;
        _object_id              TEXT;
        _remote_object_id       TEXT;
        _part_suffix            TEXT;
        _worker_suffix          TEXT;
        _name_claim             TEXT;
        _name_queue_status      TEXT;
        _name_remote_claim      TEXT;
        _name_pull              TEXT;
        _name_dblink_conn       TEXT;
        _name_processed         TEXT;
        _name_processing        TEXT;
        _name_remote_queue_data TEXT;
        _name_process           TEXT;
        _name_put               TEXT;
        _name_push_status       TEXT;
        _name_remote_queue_bulk TEXT;
        _name_proc_id           TEXT;
        _name_proc_attributes   TEXT;
        _name_proc_no_diff      TEXT;
        _sql_buffer             TEXT;
BEGIN
        _object_id              := proc_table_def.proc_table_id;
        _remote_object_id       := proc_table_def.in_table_id || '_' || proc_table_def.out_group_id;
        _part_suffix            := '_' || partition_id;
        _worker_suffix          := '_' || worker_id;

        _name_claim             := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_claim'||_worker_suffix);
        _name_queue_status      := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_queue_status'||_worker_suffix);
        _name_remote_claim      := quote_ident(proc_table_def.remote_schema) || '.' ||
                quote_ident(_remote_object_id||'_claim'||_part_suffix);
        _name_pull              := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_pull'||_worker_suffix);
        _name_dblink_conn       := quote_nullable(proc_table_def.dblink_connname);
        _name_processed         := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_processed'||_worker_suffix);
        _name_processing        := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_processing'||_worker_suffix);
        _name_remote_queue_data := quote_ident(proc_table_def.remote_schema) || '.' ||
                quote_ident(_remote_object_id||'_queue_data'||_part_suffix);
        _name_process           := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_process'||_worker_suffix);
        _name_put               := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_put');
        _name_push_status       := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_push_status'||_worker_suffix);
        _name_remote_queue_bulk := quote_ident(proc_table_def.remote_schema) || '.' ||
                quote_ident(_remote_object_id||'_queue_bulk'||_part_suffix);
        _name_proc_id           := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_proc_id');
        _name_proc_attributes   := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_proc_attributes');
        _name_proc_no_diff      := proc_table_def.proc_schema::text || '.' ||
                quote_ident(_object_id||'_proc_no_diff');

        _sql_buffer := $$
CREATE FUNCTION $$||_name_claim||$$(queue_id smallint DEFAULT NULL)
        RETURNS boolean AS
$DEFINITION$
DECLARE
        _possible_changes       boolean;
        _queue_id               smallint := queue_id;
BEGIN
        IF queue_id IS NULL THEN
                SELECT  proc_workers_def.queue_id
                INTO    _queue_id
                FROM    sys_syn_dblink.proc_workers_def
                WHERE   proc_workers_def.proc_table_id = $$||quote_literal(proc_table_def.proc_table_id)||$$ AND
                        proc_workers_def.worker_id = 1;
        END IF;

        PERFORM * FROM dblink($$||_name_dblink_conn||$$, 'SELECT 0 FROM sys_syn.in_trans_claim_start();') AS t1(c1 int);

        SELECT  possible_changes
        INTO    _possible_changes
        FROM    dblink($$||_name_dblink_conn||$$, 'SELECT $$||_name_remote_claim||
                        $$('||quote_nullable(_queue_id)||')') AS claim(possible_changes boolean);

        UPDATE  $$||_name_queue_status||$$ AS queue_status
        SET     queue_id = _queue_id;

        RETURN _possible_changes;
END;
$DEFINITION$
        LANGUAGE plpgsql VOLATILE
        COST 2000;
$$;
        EXECUTE _sql_buffer;

        IF proc_table_def.attributes_array THEN
                _sql_data_type_array := '[]';
                _sql_attributes_insert := ',
                attributes';
                _sql_attributes_select := ',
                array_agg(
                        ROW(
                                '||sys_syn_dblink.proc_columns_format(proc_columns_attribute, 'queue_data.%COLUMN_NAME%', ',
                                ')||'
                        )::'||_name_proc_attributes||'
                        ORDER BY sys_syn_attribute_array_ordinal
                ) AS sys_syn_attributes';
                _sql_remote_array := ',          queue_data.sys_syn_attribute_array_ordinal';
                _sql_dblink_array := ',  sys_syn_attribute_array_ordinal integer';

                SELECT  array_to_string(array_agg(generate_series), ',')
                INTO    _sql_group_by
                FROM    generate_series(8, 7 + array_length(proc_columns_id, 1));

                _sql_group_by := '
        GROUP BY 1,2,3,4,5,6,7,' || _sql_group_by;
        ELSE
                _sql_data_type_array := '';
                _sql_attributes_insert := sys_syn_dblink.proc_columns_format(proc_columns_attribute, ',
                        attributes.%COLUMN_NAME%', '');
                _sql_attributes_select := sys_syn_dblink.proc_columns_format(proc_columns_attribute, ',
                        queue_data.%COLUMN_NAME%', '');
                _sql_remote_array := '';
                _sql_dblink_array := '';
                _sql_group_by := '';
        END IF;

        _sql_buffer := $$
CREATE FUNCTION $$||_name_pull||$$()
        RETURNS boolean AS
$DEFINITION$
DECLARE
        _queue_id smallint;
BEGIN
        SELECT  queue_status.queue_id
        INTO    _queue_id
        FROM    $$||_name_queue_status||$$ AS queue_status;

        TRUNCATE $$||_name_processed||$$, $$||_name_processing||$$;

        -- Unused: sys_syn_hold_trans_id_first, sys_syn_hold_trans_id_last
        INSERT INTO $$||_name_processing||$$ (
                trans_id_in,
                delta_type,
                queue_priority,
                hold_updated,                   prior_hold_reason_count,
                prior_hold_reason_id,           prior_hold_reason_text$$||sys_syn_dblink.proc_columns_format(proc_columns_id, ',
                id.%COLUMN_NAME%', '')||_sql_attributes_insert||$$)
        SELECT  queue_data.sys_syn_trans_id_in,
                CASE queue_data.sys_syn_delta_type
                        WHEN    1::smallint THEN 'Add'::sys_syn_dblink.delta_type
                        WHEN    2::smallint THEN 'Change'::sys_syn_dblink.delta_type
                        WHEN    3::smallint THEN 'Delete'::sys_syn_dblink.delta_type
                        ELSE    NULL
                END AS delta_type,
                queue_data.sys_syn_queue_priority,
                queue_data.sys_syn_hold_updated,           queue_data.sys_syn_hold_reason_count,
                queue_data.sys_syn_hold_reason_id,         queue_data.sys_syn_hold_reason_text$$||
                sys_syn_dblink.proc_columns_format(proc_columns_id, ',
                queue_data.%COLUMN_NAME%', '')||_sql_attributes_select||$$
        FROM    dblink($$||_name_dblink_conn||$$, $DBL$
                        SELECT  queue_data.sys_syn_trans_id_in,
                                CASE queue_data.sys_syn_delta_type
                                        WHEN    'Add'::sys_syn.delta_type       THEN 1::smallint
                                        WHEN    'Change'::sys_syn.delta_type    THEN 2::smallint
                                        WHEN    'Delete'::sys_syn.delta_type    THEN 3::smallint
                                        ELSE    NULL
                                END AS sys_syn_delta_type,
                                queue_data.sys_syn_queue_priority,
                                queue_data.sys_syn_hold_updated,           queue_data.sys_syn_hold_reason_count,
                                queue_data.sys_syn_hold_reason_id,         queue_data.sys_syn_hold_reason_text$$||_sql_remote_array
                                || sys_syn_dblink.proc_columns_format(proc_columns_id, ',
                                queue_data.%COLUMN_NAME%', '')||sys_syn_dblink.proc_columns_format(proc_columns_attribute, ',
                                queue_data.%COLUMN_NAME%', '')||$$
                        FROM    $$||_name_remote_queue_data||$$ AS queue_data
                        WHERE   queue_data.sys_syn_queue_state = 'Claimed'::sys_syn.queue_state AND
                                queue_data.sys_syn_queue_id IS NOT DISTINCT FROM $DBL$||quote_nullable(_queue_id)) AS queue_data (
                sys_syn_trans_id_in int,
                sys_syn_delta_type smallint,
                sys_syn_queue_priority smallint,
                sys_syn_hold_updated boolean,   sys_syn_hold_reason_count integer,
                sys_syn_hold_reason_id integer, sys_syn_hold_reason_text text$$||_sql_dblink_array||
                sys_syn_dblink.proc_columns_format(proc_columns_id, ',
                %COLUMN_NAME% %FORMAT_TYPE%', '')||sys_syn_dblink.proc_columns_format(proc_columns_attribute, ',
                %COLUMN_NAME% %FORMAT_TYPE%', '')||$$
                )$$||_sql_group_by||$$;

        RETURN FOUND;
END
$DEFINITION$
        LANGUAGE plpgsql VOLATILE
        COST 5000;
$$;
        EXECUTE _sql_buffer;


        _sql_buffer := $$
CREATE FUNCTION $$||_name_process||$$()
        RETURNS boolean AS
$DEFINITION$
DECLARE
        _processing_rec         $$||_name_processing||$$%ROWTYPE;
        _processed_status       sys_syn_dblink.processed_status;
        _records_per_proc       int;
BEGIN
        SELECT  proc_tables_def.records_per_proc
        INTO    _records_per_proc
        FROM    sys_syn_dblink.proc_tables_def
        WHERE   proc_tables_def.proc_table_id = $$||quote_literal(proc_table_def.proc_table_id)||$$;

        FOR     _processing_rec IN
        SELECT  *
        FROM    $$||_name_processing||$$
        LIMIT   _records_per_proc
        LOOP
                _processed_status := $$||_name_put||$$(
                        _processing_rec.trans_id_in,                    _processing_rec.delta_type,
                        _processing_rec.queue_priority,                 _processing_rec.hold_updated,
                        _processing_rec.prior_hold_reason_count,        _processing_rec.prior_hold_reason_id,
                        _processing_rec.prior_hold_reason_text,         _processing_rec.id,
                        _processing_rec.attributes,                     _processing_rec.no_diff);

                INSERT INTO $$||_name_processed||$$ (
                        id,                                             hold_reason_id,
                        hold_reason_text,                               queue_priority,
                        processed_time)
                VALUES (_processing_rec.id,                             _processed_status.hold_reason_id,
                        _processed_status.hold_reason_text,             _processed_status.queue_priority,
                        COALESCE(_processed_status.processed_time, CURRENT_TIMESTAMP));

                DELETE FROM $$||_name_processing||$$ AS processing
                WHERE   processing.id = _processing_rec.id;
        END LOOP;

        RETURN FOUND;
END
$DEFINITION$
        LANGUAGE plpgsql VOLATILE
        COST 5000;
$$;
        EXECUTE _sql_buffer;


        _sql_buffer := $$
CREATE FUNCTION $$||_name_push_status||$$()
        RETURNS boolean AS
$DEFINITION$
DECLARE
        _queue_id               smallint;
        _proc_table_def         sys_syn_dblink.proc_tables_def;
        _processed_rec          $$||_name_processed||$$%ROWTYPE;
        _remote_sql_len_max     int;
        _remote_sql             text;
        _remote_sql_len         int;
        _sql_value              text;
        _sent_first_remote      boolean := false;
        _remote_dispatched      boolean;
        _remote_found           boolean;
BEGIN
        SELECT  queue_status.queue_id
        INTO    _queue_id
        FROM    $$||_name_queue_status||$$ AS queue_status;

        _proc_table_def := (
                SELECT  proc_tables_def
                FROM    sys_syn_dblink.proc_tables_def
                WHERE   proc_tables_def.proc_table_id = $$||quote_literal(proc_table_def.proc_table_id)||$$);

        _remote_sql_len_max     := 524288; -- _proc_table_def.remote_sql_len_max;
        _remote_sql             := $DBL$INSERT INTO $$||_name_remote_queue_bulk||$$(id,hold_reason_id,$DBL$ ||
                                        $DBL$hold_reason_text,queue_id,queue_priority,processed_time)VALUES$DBL$ || E'\n';
        _remote_sql_len := 0;

        FOR     _processed_rec IN
        SELECT  *
        FROM    $$||_name_processed||$$
        LOOP
                _sql_value := '(' ||
                        $$ || sys_syn_dblink.column_type_value_row(proc_table_def.proc_table_id, '_processed_rec.id') || $$ || ','||
                        quote_nullable(_processed_rec.hold_reason_id) || ',' ||
                        quote_nullable(_processed_rec.hold_reason_text) || ',' ||
                        quote_nullable(_queue_id) || ',' || quote_nullable(_processed_rec.queue_priority) || ',' ||
                        quote_nullable(_processed_rec.processed_time) || ')';

                IF _remote_sql_len = 0 THEN
                        _remote_sql_len := octet_length(_remote_sql) + octet_length(_sql_value);
                ELSE
                        _remote_sql_len := _remote_sql_len + octet_length(_sql_value) + 2;
                        _remote_sql := _remote_sql || E',\n';
                END IF;
                _remote_sql := _remote_sql || _sql_value;

                IF _remote_sql_len >= _remote_sql_len_max THEN
                        IF _sent_first_remote THEN
                                SELECT  dblink_get_result.dblink_send_query_result != 'INSERT 0 0'
                                INTO    _remote_found
                                FROM    dblink_get_result($$||_name_dblink_conn||$$)
                                                AS dblink_get_result(dblink_send_query_result text);

                                -- dblink_get_result must be called once for each query sent, and one additional time to obtain an
                                -- empty set result.
                                PERFORM dblink_get_result($$||_name_dblink_conn||$$);
                        END IF;

                        _remote_dispatched      := dblink_send_query($$||_name_dblink_conn||$$, _remote_sql) = 1;
                        _remote_sql             := $DBL$INSERT INTO $$||_name_remote_queue_bulk||$$(id,hold_reason_id,$DBL$ ||
                                                        $DBL$hold_reason_text,queue_id,queue_priority,processed_time)VALUES$DBL$||
                                                        E'\n';
                        _remote_sql_len         := 0;
                        _sent_first_remote      := TRUE;
                END IF;
        END LOOP;

        IF _sent_first_remote THEN
                SELECT  dblink_get_result.dblink_send_query_result != 'INSERT 0 0'
                INTO    _remote_found
                FROM    dblink_get_result($$||_name_dblink_conn||$$)
                                AS dblink_get_result(dblink_send_query_result text);

                -- dblink_get_result must be called once for each query sent, and one additional time to obtain an
                -- empty set result.
                PERFORM dblink_get_result($$||_name_dblink_conn||$$);
        END IF;

        IF _remote_sql_len != 0 THEN
                _remote_dispatched := dblink_send_query($$||_name_dblink_conn||$$, _remote_sql) = 1;

                SELECT  dblink_get_result.dblink_send_query_result != 'INSERT 0 0'
                INTO    _remote_found
                FROM    dblink_get_result($$||_name_dblink_conn||$$)
                                AS dblink_get_result(dblink_send_query_result text);

                -- dblink_get_result must be called once for each query sent, and one additional time to obtain an
                -- empty set result.
                PERFORM dblink_get_result($$||_name_dblink_conn||$$);
        END IF;

        SELECT  queue_bulk.found
        INTO    _remote_found
        FROM    dblink($$||_name_dblink_conn||$$, $DBL$
                        SELECT * FROM $$||_name_remote_queue_bulk||$$($DBL$||quote_nullable(_queue_id)||$DBL$::SMALLINT)
                $DBL$) AS queue_bulk(found boolean);

        TRUNCATE $$||_name_processed||$$;

        UPDATE  $$||_name_queue_status||$$ AS queue_status
        SET     queue_id = NULL;

        RETURN _remote_sql_len != 0 OR _sent_first_remote;
END
$DEFINITION$
        LANGUAGE plpgsql VOLATILE
        COST 5000;
$$;
        EXECUTE _sql_buffer;
END;
$_$;
COMMENT ON FUNCTION sys_syn_dblink.proc_table_code(sys_syn_dblink.proc_tables_def, sys_syn_dblink.table_types_def,
        sys_syn_dblink.create_proc_column[], sys_syn_dblink.create_proc_column[], sys_syn_dblink.create_proc_column[],
        sys_syn_dblink.create_proc_column[], sys_syn_dblink.create_proc_column[], sys_syn_dblink.create_put_column[],
        text, text, text, hstore, smallint, smallint)
  IS '';

CREATE FUNCTION sys_syn_dblink.proc_table_drop (proc_table_id text, drop_put_table boolean default false)
        RETURNS void
        LANGUAGE plpgsql
        AS $_$
DECLARE
        _proc_table_def                 sys_syn_dblink.proc_tables_def;
        _object_id                      TEXT;
        _sql_command_prefix             TEXT;
        _table_type_def                 sys_syn_dblink.table_types_def;
        _sql_buffer                     TEXT;
        _sql_name_type_proc_id          TEXT;
        _sql_name_type_proc_attributes  TEXT;
        _sql_name_type_proc_no_diff     TEXT;
        _proc_worker_def                sys_syn_dblink.proc_workers_def%ROWTYPE;
BEGIN
        _proc_table_def := (
                SELECT  proc_tables_def
                FROM    sys_syn_dblink.proc_tables_def
                WHERE   proc_tables_def.proc_table_id = proc_table_drop.proc_table_id);

        IF _proc_table_def IS NULL THEN
                RAISE EXCEPTION 'Cannot find proc_table_id ''%''.', proc_table_drop.proc_table_id
                USING HINT = 'Check to see if this ID is defined in the sys_syn_dblink.proc_tables_def table.';
        END IF;

        _object_id := _proc_table_def.proc_table_id;

        _sql_name_type_proc_id          := _proc_table_def.proc_schema::text || '.' || quote_ident(_object_id||'_proc_id');
        _sql_name_type_proc_attributes  := _proc_table_def.proc_schema::text || '.' || quote_ident(_object_id||'_proc_attributes')||
                CASE WHEN _proc_table_def.attributes_array THEN '[]' ELSE '' END;
        _sql_name_type_proc_no_diff     := _proc_table_def.proc_schema::text || '.' || quote_ident(_object_id||'_proc_no_diff');

        FOR     _proc_worker_def IN
        SELECT  *
        FROM    sys_syn_dblink.proc_workers_def
        WHERE   proc_workers_def.proc_table_id = _proc_table_def.proc_table_id
        LOOP
                PERFORM sys_syn_dblink.proc_table_drop_worker(proc_table_id, _proc_worker_def.worker_id);
        END LOOP;

        _sql_command_prefix := 'DROP FUNCTION ' || _proc_table_def.proc_schema::text || '.';
        EXECUTE _sql_command_prefix || quote_ident(_object_id||'_put') || '(integer, sys_syn_dblink.delta_type, smallint, boolea' ||
                'n, integer, integer, text, ' || _sql_name_type_proc_id || ', ' || _sql_name_type_proc_attributes ||
                ', ' || _sql_name_type_proc_no_diff || ')';

        IF drop_put_table THEN
                _table_type_def := (
                        SELECT  table_types_def
                        FROM    sys_syn_dblink.table_types_def
                        WHERE   table_types_def.table_type_id   = _proc_table_def.table_type_id AND
                                table_types_def.attributes_array= _proc_table_def.attributes_array);

                EXECUTE 'SELECT '||_table_type_def.table_drop_proc_schema::text||'.'||
                                quote_ident(_table_type_def.table_drop_proc_name)||
                        '($1, $2, $3)'
                INTO    _sql_buffer
                USING   _proc_table_def.put_schema::text, _proc_table_def.put_table_name, _proc_table_def.table_settings;
                EXECUTE _sql_buffer;
        END IF;

        _sql_command_prefix := 'DROP TYPE '|| _proc_table_def.proc_schema::text || '.';
        EXECUTE _sql_command_prefix || quote_ident(_object_id||'_proc_no_diff');
        EXECUTE _sql_command_prefix || quote_ident(_object_id||'_proc_attributes');
        EXECUTE _sql_command_prefix || quote_ident(_object_id||'_proc_id');

        DELETE
        FROM    sys_syn_dblink.proc_foreign_keys
        WHERE   proc_foreign_keys.foreign_proc_table_id = _proc_table_def.proc_table_id;

        DELETE
        FROM    sys_syn_dblink.proc_columns_def
        WHERE   proc_columns_def.proc_table_id = _proc_table_def.proc_table_id;

        DELETE
        FROM    sys_syn_dblink.proc_workers_def
        WHERE   proc_workers_def.proc_table_id = _proc_table_def.proc_table_id;

        DELETE
        FROM    sys_syn_dblink.proc_partitions_def
        WHERE   proc_partitions_def.proc_table_id = _proc_table_def.proc_table_id;

        DELETE
        FROM    sys_syn_dblink.proc_tables_def
        WHERE   proc_tables_def.proc_table_id = _proc_table_def.proc_table_id;
END;
$_$
        COST 40;
COMMENT ON FUNCTION sys_syn_dblink.proc_table_drop(text, boolean)
        IS '';

CREATE FUNCTION sys_syn_dblink.proc_table_drop_worker (proc_table_id text, worker_id smallint)
        RETURNS void
        LANGUAGE plpgsql
        AS $_$
DECLARE
        _proc_table_def                 sys_syn_dblink.proc_tables_def;
        _object_id                      TEXT;
        _worker_suffix                  TEXT;
        _sql_command_prefix             TEXT;
BEGIN
        _proc_table_def := (
                SELECT  proc_tables_def
                FROM    sys_syn_dblink.proc_tables_def
                WHERE   proc_tables_def.proc_table_id = proc_table_drop_worker.proc_table_id);

        IF _proc_table_def IS NULL THEN
                RAISE EXCEPTION 'Cannot find proc_table_id ''%''.', proc_table_drop_worker.proc_table_id
                USING HINT = 'Check the proc_tables_def table.';
        END IF;

        _object_id      := _proc_table_def.proc_table_id;
        _worker_suffix  := '_' || worker_id;

        _sql_command_prefix := 'DROP FUNCTION ' || _proc_table_def.proc_schema::text || '.';
        EXECUTE _sql_command_prefix || quote_ident(_object_id||'_claim'||_worker_suffix) || '(smallint)';
        EXECUTE _sql_command_prefix || quote_ident(_object_id||'_process'||_worker_suffix) || '()';
        EXECUTE _sql_command_prefix || quote_ident(_object_id||'_pull'||_worker_suffix) || '()';
        EXECUTE _sql_command_prefix || quote_ident(_object_id||'_push_status'||_worker_suffix) || '()';

        _sql_command_prefix := 'DROP TABLE '|| _proc_table_def.proc_schema::text || '.';
        EXECUTE _sql_command_prefix || quote_ident(_object_id||'_queue_status'||_worker_suffix);
        EXECUTE _sql_command_prefix || quote_ident(_object_id||'_processed'||_worker_suffix);
        EXECUTE _sql_command_prefix || quote_ident(_object_id||'_processing'||_worker_suffix);
END;
$_$
        COST 40;
COMMENT ON FUNCTION sys_syn_dblink.proc_table_drop_worker(text, smallint)
        IS '';


SELECT pg_catalog.pg_extension_config_dump('sys_syn_dblink.table_types_def', $$WHERE table_type_id NOT LIKE 'sys_syn-%'$$);
SELECT pg_catalog.pg_extension_config_dump('sys_syn_dblink.in_groups_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn_dblink.out_groups_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn_dblink.put_groups_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn_dblink.put_table_transforms', $$WHERE rule_group_id NOT LIKE 'sys_syn-%'$$);
SELECT pg_catalog.pg_extension_config_dump('sys_syn_dblink.put_column_transforms', $$WHERE rule_group_id NOT LIKE 'sys_syn-%'$$);
SELECT pg_catalog.pg_extension_config_dump('sys_syn_dblink.proc_tables_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn_dblink.proc_workers_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn_dblink.proc_partitions_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn_dblink.proc_columns_def', '');
SELECT pg_catalog.pg_extension_config_dump('sys_syn_dblink.proc_foreign_keys', '');
