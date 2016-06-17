copy counts (valid) from '/data/psims/out';

select count(*) from counts where valid=0;
select count(*) from counts where valid>=1 and valid<=8;
select count(*) from counts where valid>=9 and valid<=16;
select count(*) from counts where valid>=17 and valid<=24;
select count(*) from counts where valid>=25 and valid<=32;
select count(*) from counts where valid=33;
select count(*) from counts where valid=34;

delete from counts;
