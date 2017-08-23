delete from foglamp.scheduled_processes where name in ('sleep1', 'sleep10');

insert into foglamp.scheduled_processes (name, script) values ('sleep1', '["sleep", "1"]');
insert into foglamp.scheduled_processes (name, script) values ('sleep10', '["sleep", "10"]');