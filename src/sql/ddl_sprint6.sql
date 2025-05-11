DROP TABLE IF EXISTS STV202504020__STAGING.group_log CASCADE;
CREATE TABLE STV202504020__STAGING.group_log (
    group_id INT,
    user_id INT,
    user_id_from INT,
    event VARCHAR(10),
    event_datetime TIMESTAMP
)
ORDER BY group_id, user_id
SEGMENTED BY HASH(group_id) ALL NODES
PARTITION BY event_datetime::DATE
GROUP BY CALENDAR_HIERARCHY_DAY(event_datetime::DATE, 3, 2);



DROP TABLE IF EXISTS STV202504020__DWH.l_user_group_activity CASCADE;
CREATE TABLE STV202504020__DWH.l_user_group_activity (
    hk_l_user_group_activity INT PRIMARY KEY,
    hk_user_id INT NOT NULL,
    hk_group_id INT NOT NULL,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(20)
);
ALTER TABLE STV202504020__DWH.l_user_group_activity ADD CONSTRAINT fk_uga_users_hk_user_id 
    FOREIGN KEY (hk_user_id) REFERENCES STV202504020__DWH.h_users(hk_user_id);
ALTER TABLE STV202504020__DWH.l_user_group_activity ADD CONSTRAINT fk_uga_groups_hk_group_id 
    FOREIGN KEY (hk_group_id) REFERENCES STV202504020__DWH.h_groups(hk_group_id);


DROP TABLE IF EXISTS STV202504020__DWH.s_auth_history CASCADE;
CREATE TABLE STV202504020__DWH.s_auth_history (
    hk_l_user_group_activity INT,
    user_id_from INT NOT NULL,
    event VARCHAR(10),
    event_dt TIMESTAMP,
    load_dt TIMESTAMP NOT NULL,
    load_src VARCHAR(20)
);
ALTER TABLE STV202504020__DWH.s_auth_history ADD CONSTRAINT fk_auth_hist_uga 
    FOREIGN KEY (hk_l_user_group_activity) REFERENCES STV202504020__DWH.l_user_group_activity(hk_l_user_group_activity);