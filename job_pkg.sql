--- PL/SQL job control modelled on Unix: submit one or more tasks, wait for them to complete.
-- Uses DBMS_JOB to submit tasks, DBMS_ALERT for jobs to pass back started/completed/failed messages.
-- The DBMS_ALERT mechanism is hidden from the submit/wait interface.
--
-- Object-based approach allows you to declare a job object then call myjob.submit() etc.
-- (See demo block for examples.)
--
-- Prerequisites: Execute permission on sys.dbms_alert.
--
-- William Robertson 2004, www.williamrobertson.net
 
-- Package (spec only) for constants used in type body:
create or replace package job_pkg
as
   k_error_code         constant pls_integer := -20000;
   k_delimiter          constant varchar2(3) := chr(0);  -- Delimiter for packing messages
   k_job_queue_interval constant interval day(0) to second(0) default interval '5' second;
 
   -- Job statuses:
   k_submitted          constant varchar2(30) := 'Submitted';
   k_not_submitted      constant varchar2(30) := 'Not submitted';
   k_waiting_confirm    constant varchar2(30) := 'Awaiting start confirmation';
   k_submit_failed      constant varchar2(30) := 'Submit failed';
   k_in_progress        constant varchar2(30) := 'In Progress';
   k_wait_timed_out     constant varchar2(30) := 'Wait timeout';
   k_failed             constant varchar2(30) := 'Abnormal Termination';
   k_completed_ok       constant varchar2(30) := 'Completed Successfully';
end job_pkg;
/
 
show errors
 
-- job_ot object type:
-- Submit one or more tasks, wait for them to complete.
-- Uses dbms_job to submit tasks, dbms_alert to pass back started/completed/failed messages.
-- The dbms_alert mechanism is hidden from the submit/wait interface.
--
-- Object-based approach allows you to declare a new job_ot object then call myjob.submit() etc.
 
create sequence job_seq start with 1 maxvalue 999 cycle;
 
create or replace type job_ot as object
   ( name            varchar2(30)
   , job_number      number             -- Number set by dbms_job.submit
   , handle          varchar2(30)       -- Generated unique id formed from name + generated sequence
   , command         varchar2(4000)     -- Command to execute
   , description     varchar2(500)      -- Text description
   , message         varchar2(1000)     -- Message returned from execution; error message if failed
   , wait_timeout    interval day to second(0)
   , created_time    timestamp
   , submitted_time  timestamp
   , started_time    timestamp
   , completed_time  timestamp
   , run_status      varchar2(50)       -- In progress, failed etc
 
   , member procedure submit
 
   , member procedure wait
     ( p_timeout interval day to second default null
     , p_alertname varchar2 default null )  -- What to wait for: defaults to self.handle, above
 
   , constructor function job_ot
     ( p_name             varchar2
     , p_command          varchar2
     , p_description      varchar2 default null
     , p_timeout          interval day to second default interval '1' minute )
     return self as result
 
   , static procedure execute
     ( p_command          varchar2   -- What to execute
     , p_receipt_alert    varchar2   -- Signal name for confirming receipt to submitting procedure
     , p_completed_alert  varchar2   -- Signal name for confirming job completion to submitting procedure
     , p_invoking_session varchar2 ) -- Session id of submitting process
 
   , member procedure print
)
/
 
show errors
 
create or replace type body job_ot as
   constructor function job_ot
      ( p_name          varchar2
      , p_command       varchar2
      , p_description   varchar2 default null
      , p_timeout       interval day to second default interval '1' minute )
      return self as result
   is
      v_seq  pls_integer;
      v_handle varchar2(3000);  -- Bigger than required to ensure assignment does not fail
   begin
      select job_seq.nextval
      into   v_seq
      from   dual;
 
      v_handle := substr(nvl(upper(p_name),'alert') || '_' || v_seq,1,30);
 
      -- "_receipt" must not be longer than 35 characters:
      if length(v_handle) > 27 then
         v_handle := substr(upper(p_name),1,14) || '_' || v_seq;
      end if;
      self.name := p_name;
      self.handle := v_handle;
      self.command := p_command;
      self.description := p_description;
      self.wait_timeout := nvl(p_timeout,interval '1' minute);
      self.created_time := systimestamp;
      self.run_status := job_pkg.k_not_submitted;
      return;
   end;
   member procedure submit
   is
      pragma autonomous_transaction;
      k_session_id constant varchar2(30) := dbms_session.unique_session_id;
      k_wait_timeout interval day(0) to second(0) := job_pkg.k_job_queue_interval * 2;
      cursor c_jobs(cp_command varchar2 := self.command)
      is
         select job
         from   user_jobs
         where  what = cp_command;
      v_command varchar2(5000);
      v_job_number   pls_integer;
      v_message      varchar2(1000);
      v_job_status   pls_integer;
      v_receipt_name varchar2(30);
   begin
      if self.command is null then
         raise_application_error
         ( job_pkg.k_error_code
         , 'Command must be specified' );
      elsif self.handle is null then
         raise_application_error
         ( job_pkg.k_error_code
         , 'Job handle must be specified' );
      end if;
      v_receipt_name := self.handle || '_receipt';
      v_command :=
         'job_ot.execute(''begin '
         || replace(rtrim(self.command,'; '),'''','''''')
         || '; end;'', '''
         || v_receipt_name || ''', '''
         || self.handle
         || ''','''
         || k_session_id || ''');';
      -- In case same command already in job queue, remove any jobs for same command:
      for r_job in c_jobs(self.command)
      loop
         dbms_job.remove(r_job.job);
      end loop;
      -- Register interest in both receipt and completion alerts.
      -- submit waits for receipt; calling procedure waits for completion.
      dbms_alert.register(v_receipt_name);
      dbms_alert.register(self.handle);
      commit;
      submitted_time := systimestamp;
      begin
         dbms_job.submit
         ( self.job_number  -- 'out' parameter: job number returned by dbms_job.submit
         , v_command        -- in format 'job_ot.execute('begin cmd; end;', receipt_alert, completed_alert);'
         , sysdate          -- First run
         , null );          -- Interval (none = no repeat)
         commit;
         run_status := job_pkg.k_submitted;
      exception
         when others then
            raise_application_error
            ( job_pkg.k_error_code
            , 'dbms_job.submit failed'
            , TRUE );
      end;
      -- Wait for background job just submitted to confirm that it is ready to begin:
      -- Job should send initial confirmation before doing anything (picked up here) followed by
      -- completion signal on success/failure at end (picked up by calling procedure).
      -- Note that when a member procedure fails with an exception, changes to the object state
      -- are lost, e.g. run_status will revert to 'Not submitted' - we cannot both raise an exception
      -- and change an attribute value.
      begin
         -- wait() will raise exceptions on failure
         self.wait(k_wait_timeout, v_receipt_name);
         dbms_alert.remove(v_receipt_name);
         if self.run_status = job_pkg.k_wait_timed_out then
            raise_application_error
            ( job_pkg.k_error_code
            , 'Submitted task failed to start (no confirmation received after ' ||
              ltrim(to_char(k_wait_timeout),'+') || ')'
            , TRUE );
         end if;
      end;
      started_time := systimestamp;
      run_status := job_pkg.k_in_progress;
      commit;
   exception
      when others then
         run_status := job_pkg.k_submit_failed;
         raise_application_error
         ( job_pkg.k_error_code
         , 'Failed to submit job ' || v_command
         , TRUE );
   end submit;
   member procedure wait
      ( p_timeout interval day to second default null
      , p_alertname varchar2 default null )  -- What to wait for: defaults to self.handle, above
   is
      k_alertname  constant varchar2(50) := nvl(p_alertname,self.handle);
      k_is_confirmation constant boolean := k_alertname = self.handle;
      k_timeout    constant interval day to second := nvl(p_timeout,self.wait_timeout);
      k_maxwait constant pls_integer :=
         nvl
         ( extract(day from k_timeout) * 86400
         + extract(hour from k_timeout) * 3600
         + extract(minute from k_timeout) * 60
         + extract(second from k_timeout)
         , dbms_alert.maxwait );
      v_delimiter_position pls_integer;
      v_message varchar2(32767);
      v_dbms_alert_status integer;
   begin
      if k_is_confirmation then
         run_status := job_pkg.k_waiting_confirm;
      end if;
      dbms_alert.waitone
      ( k_alertname
      , v_message
      , v_dbms_alert_status
      , k_maxwait );
      dbms_alert.remove(k_alertname);
      v_delimiter_position := instr(v_message, job_pkg.k_delimiter);
      -- The returned message may be in the form "status[delimiter]message":
      -- if so, extract into run_status and message:
      if v_delimiter_position > 0 then
         run_status := substr(v_message,1,v_delimiter_position -1);
         message :=    substr(v_message,v_delimiter_position + length(job_pkg.k_delimiter));
      else
         message := v_message;
      end if;
      if v_dbms_alert_status = 1 then
         -- Check status returned by dbms_alert: 1 = "timed out while waiting"
         self.run_status := job_pkg.k_wait_timed_out;
         self.message :=
         'Timeout after waiting ' || k_maxwait
         || ' seconds for job ' || self.name || ': "'
         || substr(self.command,1,80)
         || case when length(self.command) > 80 then '...' else null end
         || '"';
         -- Not good idea to have wait() fail, as then we lose any state changes that
         -- wait() made. Instead, the calling application should check thisjob.status.
      else
         if k_is_confirmation then
            completed_time := systimestamp;
            message := nvl(message,job_pkg.k_completed_ok);
         end if;
      end if;
   end wait;
   static procedure execute
      ( p_command          varchar2   -- What to execute
      , p_receipt_alert    varchar2   -- Signal name for confirming receipt to submitting procedure
      , p_completed_alert  varchar2   -- Signal name for confirming job completion to submitting procedure
      , p_invoking_session varchar2 ) -- Session id of submitting process
   is
      pragma autonomous_transaction;
      v_status varchar2(30) := job_pkg.k_in_progress;
      v_message varchar2(1000) := job_pkg.k_in_progress;
   begin
      dbms_application_info.set_module('job_ot.execute', p_command);
      -- Send initial message to confirm instruction received:
      -- (echo back the value of 'p_receipt_alert')
      dbms_alert.signal(p_receipt_alert,v_status);
      commit;
      begin
         execute immediate(p_command);
         v_message :=
         job_pkg.k_completed_ok ||
         job_pkg.k_delimiter ||
         job_pkg.k_completed_ok;
      exception
         when others then
            v_message :=
            job_pkg.k_failed ||
            job_pkg.k_delimiter ||
            sqlerrm;
      end;
      -- Send specified alert to inform waiting process that task has completed:
      dbms_alert.signal(p_completed_alert,v_message);
      commit;
   end execute;
   member procedure print
   is
      procedure print_item
         ( p_name varchar2
         , p_value varchar2 )
      is
      begin
         dbms_output.put_line(rpad(p_name || ':', 15) || p_value);
      end print_item;
   begin
      print_item('Job name', name);
      print_item('Command', command);
      print_item('Description', description);
      print_item('Message', message);
      print_item('Created', created_time);
      print_item('Submitted', submitted_time);
      print_item('Started', started_time);
      print_item('Completed', completed_time);
      print_item('Run status', run_status);
      dbms_output.put_line(chr(9));
   end print;
end;
/
show errors
set serverout on size 100000
prompt
prompt Demo/test (allow 20 seconds for submit/wait/output cycle to complete):
prompt
set echo on
declare
   -- Create some job objects:
   v_ok_job job_ot := new job_ot('should_work', 'null;', 'Background job test: successful job');
   v_invalid_job job_ot := new job_ot('should_fail', 'nosuchcommand', 'Background job test: handle failure');
   v_slow_job job_ot :=
   new job_ot
   ( 'should_timeout'
   , 'dbms_lock.sleep(20)'   -- User must have execute permission on sys.dbms_lock
   , 'Background job demo 2: trapping failure'
   , interval '5' second );  -- Job will fail to complete within time allowed
begin
   dbms_output.put_line('Defined the following jobs:' || chr(10));
   v_ok_job.print();         -- Display basic info about the job
   v_invalid_job.print();
   v_slow_job.print();
   dbms_output.new_line;
   dbms_output.put_line('Calling submit() method of each job in turn...' || chr(10));
   v_ok_job.submit();        -- Submit in background
   v_invalid_job.submit();
   v_slow_job.submit();
   dbms_output.put_line('Jobs have now been submitted in background.');
   dbms_output.put_line('We can now get on with something else until we are ready to wait for them to complete.');
   dbms_output.put_line(chr(10)||'...'||chr(10));
   dbms_output.put_line('Calling wait() method of each job in turn...' || chr(10));
   v_ok_job.wait();
   v_invalid_job.wait();
   v_slow_job.wait();
   dbms_output.put_line('Job details after all jobs have returned:' || chr(10));
   v_ok_job.print();
   v_invalid_job.print();
   v_slow_job.print();
end;
/
set echo off
