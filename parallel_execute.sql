create table proc_map (proc_id number, proc_name varchar2(64), is_active varchar2(1));

insert into proc_map (proc_id, proc_name, is_active) values (1, 'insert_table1', 'Y');
insert into proc_map (proc_id, proc_name, is_active) values (2, 'insert_table2', 'Y');
insert into proc_map (proc_id, proc_name, is_active) values (3, 'insert_table3', 'Y');


create or replace procedure p_run_proc (ip_start in number, ip_end in number)  is
  v_proc_name proc_map.proc_name%type;
begin
  begin
    select t.proc_name into v_proc_name 
    from proc_map t
    where t.proc_id = ip_start;
  exception
    when no_data_found then null;
    when too_many_rows then null;
  end;

  if v_proc_name is not null
    then
      execute immediate 'begin ' || v_proc_name || '; end;';
  end if;
end;

declare
  v_task_name varchar2(4000) := dbms_parallel_execute.generate_task_name;
  v_sql varchar2(4000);
  v_run varchar2(4000);
  v_thread_count number;
  v_task_status number;
begin
  dbms_parallel_execute.create_task (task_name => v_task_name);

  v_sql := 'select t.proc_id as num_col 
                  ,t.proc_id as num_col
            from proc_map t 
            where t.is_active = ''Y'' 
            order by t.proc_id';

  dbms_parallel_execute.create_chunks_by_SQL (task_name => v_task_name, sql_stmt => v_sql, by_rowid => false);

  v_run := 'begin p_run_proc (ip_start => :start_id, ip_end => :end_id); end;';

  select count(*) into v_thread_count 
  from proc_map t
  where t.is_active = 'Y';

  dbms_parallel_execute.run_task (task_name => v_task_name
                                 ,sql_stmt => v_run
                                 ,language_flag => dbms_sql.native
                                 ,parallel_level => v_thread_count);

  v_task_status := dbms_parallel_execute.task_status (task_name => v_task_name);

  if v_task_status = dbms_parallel_execute.FINISHED
    then
      dbms_parallel_execute.drop_task (task_name => v_task_name);
    else
      raise_application_error (-20001, 'ORA in task ' || v_task_name);
  end if;

end;
