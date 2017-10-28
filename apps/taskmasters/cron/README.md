# cron taskmaster

cron taskmaster is a basic, general-purpose taskmaster for launching
tasks in the same way one would with the cron linux service. Scheduling
notation uses the same format as cron:

 
* * * * * (minute hour mday month wday) 

But, unlike cron the config for the cron taskmaster is in toml format. 
See the sample.toml file for a sample config.

