CREATE OR REPLACE FUNCTION _pre_insert_train()
RETURNS TRIGGER
LANGUAGE PLPGSQL
AS $$
DECLARE
gap int;
BEGIN
gap = new.start_date::date - CURRENT_DATE::date;
if gap < 7 then
raise exception 'Invalid date. Train details should be put in at least one week prior!';
end if;
if new.number_of_ac_coaches + new.number_of_sl_coaches = 0 then
raise exception 'No coaches in the train!';
end if;
return new;
END;
$$;

CREATE TRIGGER pre_insert_train
BEFORE INSERT
ON train
FOR EACH ROW
EXECUTE PROCEDURE _pre_insert_train();

CREATE OR REPLACE FUNCTION _on_train_insertion()
RETURNS TRIGGER
LANGUAGE PLPGSQL
AS $$
DECLARE
seat record;
BEGIN
EXECUTE format('CREATE TABLE %I (pnr varchar(20) primary key, passengers varchar(256)[]);', 'booking_' || NEW.train_number::text);
EXECUTE format('CREATE TABLE %I (journey_date date, coach char(2), number_of_seats integer, primary key(journey_date, coach));', 'available_seats_' || NEW.train_number::text);
for seat in execute format('select * from gettotalseats(%L, %L, %L);', NEW.train_number, NEW.start_date, 'AC') loop
execute format('INSERT INTO %I VALUES(%L, %L, %L)',  'available_seats_' || NEW.train_number::text,NEW.start_date,  seat.coach_type, seat.berth_number);
end loop;
for seat in execute format('select * from gettotalseats(%L, %L, %L);', NEW.train_number, NEW.start_date, 'SL') loop
execute format('INSERT INTO %I VALUES(%L, %L, %L)',  'available_seats_' || NEW.train_number::text,NEW.start_date,  seat.coach_type, seat.berth_number);
end loop;
RETURN NEW;
END;
$$;

CREATE TRIGGER on_train_insertion
AFTER INSERT
ON train
FOR EACH ROW
EXECUTE PROCEDURE _on_train_insertion();

CREATE OR REPLACE FUNCTION gettotalseats(trainNumber varchar(5), cur_date date, coachType char(2))
RETURNS table(coach_type char(2), berth_number integer)
LANGUAGE plpgsql
as $$
DECLARE
rec record;
num_coach integer;
num_seats integer;
total_seats integer;
BEGIN
if coachType = 'AC' then
SELECT INTO num_coach
number_of_AC_coaches FROM train
WHERE train_number = trainNumber AND start_date = cur_date;
SELECT INTO num_seats
count(berth) FROM ac_coach_hash c;
else
SELECT INTO num_coach
number_of_SL_coaches FROM train
WHERE train_number = trainNumber AND start_date = cur_date;
SELECT INTO num_seats
count(berth) FROM sl_coach_hash c;
end if;
    RETURN QUERY SELECT coachType,num_seats*num_coach ;
END;
$$;

CREATE OR REPLACE FUNCTION booktickets( trainID varchar(5), travel_date date, k integer, coacht char(2), names varchar(256)[])
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
seat record;
passenger varchar(256);
available_seats integer;
total_seats integer;
pnr bigint;
BEGIN
if trainID not in (select train_number from train) then
RAISE EXCEPTION 'No such train exists';
return 0;
end if;
if travel_date!=(select start_date from train where train_number=trainID) then
RAISE EXCEPTION 'Invalid Date';
return 0;
end if;
available_seats:=0;
execute format('LOCK TABLE %I IN ACCESS EXCLUSIVE MODE','available_seats_' || trainID::text);
execute format('LOCK TABLE %I IN ACCESS EXCLUSIVE MODE','booking_' || trainID::text);
execute format('select number_of_seats from %I where journey_date=%L and coach=%L', 'available_seats_' || trainID::text,travel_date, coacht) into available_seats;
execute format('select berth_number from gettotalseats(%L, %L, %L);',trainID, travel_date, coacht) into total_seats;
if k<=available_seats then
pnr:= getPNR(trainID, travel_date, coacht,k,total_seats-available_seats);
execute format('INSERT INTO %I VALUES(%L, %L)',  'booking_' || trainID::text,pnr,names);
execute format('UPDATE %I SET number_of_seats = %L WHERE journey_date=%L and coach=%L',  'available_seats_' || trainID::text,available_seats-k, travel_date, coacht);
return pnr;
else
RAISE EXCEPTION 'Sufficient tickets not available';
return 0;
end if;
END;
$$;

CREATE OR REPLACE FUNCTION getPNR(trainID varchar(5), travel_date date, coacht char(2), nob integer, seatstart integer)
RETURNS varchar(20)
LANGUAGE plpgsql
AS $$
DECLARE
coachType integer;
id integer;
pnr varchar(20);
BEGIN
if coacht = 'AC' then
coachType := 1;
else
coachType := 0;
end if;
id:=trainID::int;
pnr := (trim(to_char(id,'000000'))||trim(to_char(travel_date, 'ddmmyy'))||trim(to_char(coachType,'0'))||trim(to_char(nob,'00'))||trim(to_char(seatstart,'0000')));
return pnr;
END;
$$;

CREATE OR REPLACE FUNCTION getticket(pnr varchar(20))
returns table (
name char(5),
age int,
coacht char(2),
coachnumber int,
seatnumber int,
seattype char(2)
)
LANGUAGE plpgsql
AS $$
DECLARE
num integer;
tid integer;
coach integer;
tempc char(2);
tempr record;
seat integer;
num_seats integer;
BEGIN
select (substring(pnr,13,2))::int into num;
select (substring(pnr,12,1))::int into coach;
select (substring(pnr,15,4))::int into seat;
select (substring(pnr,1,5))::int into tid;
if coach = 1 then
SELECT INTO num_seats
count(berth) FROM ac_coach_hash c;
tempc:='AC';
else
SELECT INTO num_seats
count(berth) FROM sl_coach_hash c;
tempc:='SL';
end if;
for i in 1..num loop
for tempr in execute format('select * from %I as c where c.pnr=%L;', 'booking_'|| tid,pnr)  loop
name := tempr.passengers[i];
coacht := tempc;
coachnumber := ((seat+i)/num_seats)+1;
seatnumber := ((seat+i)%num_seats);
If coacht ='SL' then select type into seattype from sl_coach_hash where berth=seatnumber;
else select type into seattype from ac_coach_hash where berth=seatnumber;
End if;
return next;
end loop;
end loop;
END;
$$;

CREATE OR REPLACE FUNCTION searchdirecttrain(arr_st varchar(5), dep_st varchar(5), travel_date date)
RETURNS table (trainn varchar(5)
)
LANGUAGE plpgsql
AS $$
DECLARE
coachType integer;
id integer;
ta timestamp;
tb timestamp;
rec record;
train varchar(5);
BEGIN
for rec in (select * FROM (select train_number from schedule where station_code=arr_st and arrival_time>=travel_date) as A  natural join 
(select train_number from schedule where station_code=dep_st) as B) loop
select arrival_time into ta from schedule where station_code=arr_st;
select arrival_time into tb from schedule where station_code=dep_st;
if ta>tb then trainn=rec.train_number;
end if;
return next;
end loop;
END;
$$;

CREATE OR REPLACE FUNCTION _on_train_schedule_updation()
RETURNS TRIGGER
LANGUAGE PLPGSQL
AS $$
DECLARE
seat record;
BEGIN
EXECUTE format('CREATE TABLE %I (train_number varchar(5), arrival_time time);', 'station_' || NEW.station_code::text);
execute format('INSERT INTO %I VALUES(%L, %L)',  'station_' || NEW.station_code::text,NEW.train_number, NEW.arrival_time);
RETURN NEW;
END;
$$;

CREATE TRIGGER on_train_schedule_updation
AFTER INSERT
ON schedule
FOR EACH ROW
EXECUTE PROCEDURE _on_train_schedule_updation();

CREATE OR REPLACE FUNCTION searchhoptrain(arr_st varchar(5), dep_st varchar(5), travel_date date)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
coachType integer;
id integer;
ta timestamp;
tb timestamp;
rec record;
rec1 record;
rec2 record;
train varchar(5);
BEGIN
for rec in (select station_code FROM (select train_number from schedule where station_code=arr_st or station_code=dep_st) as A, schedule where schedule.train_number=A.train_number) loop
RAISE NOTICE 'The options for first train to station % are as follows:',rec.station_code;
for rec1 in (select * from searchdirecttrain(arr_st, rec.station_code, travel_date)) loop
RAISE NOTICE '%',rec1.trainn;
end loop;
RAISE NOTICE 'The options for second train are as follows:';
for rec2 in (select * from searchdirecttrain(rec.station_code, dep_st, travel_date)) loop
RAISE NOTICE '%',rec2.trainn;
end loop;
end loop;
END;
$$;