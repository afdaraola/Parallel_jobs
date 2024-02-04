
SELECT level start_id, level end_id FROM dual connect by level <=20

--Next, I am going to define one DB table, in which our procedure will write data:
CREATE TABLE parallel_execution_data(
    chunk_name  VARCHAR2(50),
    start_time TIMESTAMP,
    end_time   TIMESTAMP
); 


--As you can see, we will only log chunk name, start procedure time, and the time when the procedure completes its work. In fact, the procedure will just pause for a while, and then enter the completion time, thus simulating that it is doing some work.
--Next, here is stored procedure which will be called in parallel:

CREATE OR REPLACE PROCEDURE parallel_proc(p_chunk_name IN VARCHAR2)
IS
 v_now date;
BEGIN
    INSERT INTO parallel_execution_data(chunk_name, start_time)
    VALUES (p_chunk_name, SYSTIMESTAMP);

    --DBMS_LOCK.sleep(2) should be called, but I simply do not have calling privileges. Ugly, but it does the job:
     SELECT SYSDATE
      INTO v_now
      FROM DUAL;

      LOOP
        EXIT WHEN v_now + (2 * (1/86400)) <= SYSDATE;
      END LOOP;     


    UPDATE parallel_execution_data
       SET end_time = SYSTIMESTAMP
     WHERE chunk_name = p_chunk_name;

    DBMS_OUTPUT.put_line('Added row into table');
     
END;

--Finally, here is stored procedure which calls above procedure in parallel:

CREATE OR REPLACE PROCEDURE call_proc_in_parallel
IS
    l_pl_sql VARCHAR2(10000);
BEGIN
    DELETE from parallel_execution_data;


    DBMS_PARALLEL_EXECUTE.create_task(task_name =>'par_task');

    DBMS_PARALLEL_EXECUTE.create_chunks_by_sql(
        task_name   => 'par_task',
        sql_stmt    => 'SELECT level start_id, level end_id FROM dual connect by level <=20',
        by_rowid    => FALSE
    );

    l_pl_sql :=  q'[
                 begin
                     parallel_proc('The ' || to_char(:start_id) || '_' || to_char(:end_id) || '. chunk');
                  end;
                 ]';
    DBMS_PARALLEL_EXECUTE.run_task(
        task_name        => 'par_task',
        sql_stmt         => l_pl_sql,
        language_flag    => DBMS_SQL.native,
        parallel_level   => 5
    );

    DBMS_PARALLEL_EXECUTE.drop_task('par_task');
END;

--It is worth noting that the pl/sql block in which the stored procedure is called (l_pl_sql) must contain bind variables called :start_id and :end_id, otherwise code will not execute. Lets call above procedure in the anonymous pl/sql block, as follows:
begin
     call_proc_in_parallel;
end;

select * from parallel_execution_data order by start_time 
