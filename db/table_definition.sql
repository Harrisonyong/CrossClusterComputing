# 作业投递表定义
CREATE TABLE dp_job_data_submit_table (
    primary_id             INTEGER  PRIMARY KEY AUTOINCREMENT,
    user_name              STRING,
    job_total_id           INTEGER  UNIQUE
                                    NOT NULL,
    job_name               STRING   NOT NULL,
    data_dir               STRING   NOT NULL,
    execute_file_path      STRING   NOT NULL,
    single_item_allocation STRING   NOT NULL,
    create_time            DATETIME,
    transfer_flag          STRING   NOT NULL,
    transfer_state         STRING   NOT NULL,
    transfer_begin_time    DATETIME,
    transfer_end_time      DATETIME
);



# 单条作业数据， 其中job_total_id为外键，关联dp_job_data_submit_table中job_total_id
CREATE TABLE dp_single_job_data_item_table (
    primary_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    job_total_id INTEGER REFERENCES dp_job_data_submit_table (job_total_id) 
                         NOT NULL,
    data_file    STRING
);

CREATE TABLE dp_cluster_status_table (
    primary_id   INTEGER       NOT NULL,
    cluster_name VARCHAR,
    state        VARCHAR,
    ip           VARCHAR (255),
    port         INTEGER,
    user         VARCHAR (255),
    password     VARCHAR (255),
    createtime   DATETIME,
    updatetime   DATETIME,
    PRIMARY KEY (
        primary_id
    )
);

CREATE TABLE dp_partition_table (
    primary_id     INTEGER       NOT NULL,
    cluster_name   VARCHAR (255),
    partition_name VARCHAR (255),
    avail          VARCHAR (255),
    nodes          INTEGER,
    nodes_avail    INTEGER,
    state          VARCHAR (255),
    createtime     DATETIME,
    updatetime     DATETIME,
    PRIMARY KEY (
        primary_id
    ),
    FOREIGN KEY (
        cluster_name
    )
    REFERENCES dp_cluster_status_table (cluster_name) 
);

# 运行作业表
CREATE TABLE dp_running_job_table (
    primary_id       INTEGER  PRIMARY KEY AUTOINCREMENT,
    job_total_id     INTEGER,
    partition_name   STRING,
    cluster_name     STRING,
    job_id           INTEGER  NOT NULL,
    state            STRING   NOT NULL,
    sbatch_file_path STRING   NOT NULL,
    file_list        STRING   NOT NULL,
    update_time      DATETIME,
    create_time      DATETIME
);
