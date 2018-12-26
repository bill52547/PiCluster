-- 用 taskSimu id 查询 tasks 的任务 id
-- drop function tasksimu_id_cast(tasksimu_id integer);

create or replace function tasksimu_id_cast(tasksimu_id int)
returns table(task_id int) as $$
  select tasks.id from tasks
  inner join "taskSlurm" on tasks.id = "taskSlurm".task_id
  inner join "taskSimu" on "taskSlurm".id = "taskSimu"."taskSlurm_id"
  where "taskSimu".id = $1;
$$ language sql;

select tasksimu_id_cast(1);


-- 用 taskSimu.id 找所有的 task 依赖
-- drop function tasksimu_depends(tasksimu_id int);

create or replace function tasksimu_depends(tasksimu_id int)
returns table(task_id int, task_state state_enum) as $$
  select tasks.id, tasks.state
  from tasks
  where tasks.id in (select unnest(tasks.depends) from tasks
                     where tasks.id=(select tasksimu_id_cast($1)));
$$ language sql;

-- select tasksimu_depends(1);


-- num of not complete tasks per taskSimu
create or replace function depends_completion(tasksimu_id int)
returns bigint as $$
  select count(*)
  from tasksimu_depends($1)
  where task_state != 'Completed';
$$ language sql;

-- select depends_completion(1);


-- num of not completed tasks of a task
create or replace function dependency_checking(task_id int)
returns bigint as $$
  select count(*)
  from tasks
  where tasks.id in (select unnest(tasks.depends) from tasks
                     where tasks.id = task_id)
                     and tasks.state != 'Completed';
$$ language sql;

-- select dependency_checking(349);


-- 读 n 个 有depends 未完成的 taskSimu
create or replace function tasksimu_to_check(num_limit int)
returns table(tasksimu_id int) as $$
  select "taskSimu".id
  from "taskSimu"
  where depends_completion("taskSimu".id) != 0
  limit $1;
$$ language sql;

-- select tasksimu_to_check(2);


-- drop function read_from_list(ids integer[]);

-- 用 list 读 tasks
create or replace function read_from_list(ids integer[])
returns table(task tasks) as $$
  select *
  from tasks
  where tasks.id in (select unnest(ids));
$$ language sql;

-- select read_from_list(array[1,2,3]::integer[]);


-- 用task_id 读 taskslurm
create or replace function task_to_taskslurm(ids integer[])
returns table(taskslurm "taskSlurm") as $$
  select "taskSlurm"
  from "taskSlurm"
  inner join tasks on "taskSlurm".task_id = tasks.id
  where tasks.id in (select unnest(ids));
$$ language sql;

-- select task_to_taskslurm(array[1,2,3]::integer[]);