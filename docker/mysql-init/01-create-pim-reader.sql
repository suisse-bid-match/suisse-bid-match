CREATE USER IF NOT EXISTS 'pim_reader'@'%' IDENTIFIED BY 'pim_reader';
GRANT SELECT ON `pim_raw`.* TO 'pim_reader'@'%';
FLUSH PRIVILEGES;
