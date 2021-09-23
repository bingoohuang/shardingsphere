--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE USER IF NOT EXISTS 'root'@'%' IDENTIFIED BY 'root';
GRANT All privileges ON *.* TO 'root'@'%';

SET character_set_database='utf8';
SET character_set_server='utf8';

DROP DATABASE IF EXISTS encrypt_write_ds_0;
DROP DATABASE IF EXISTS encrypt_write_ds_1;
DROP DATABASE IF EXISTS encrypt_write_ds_2;
DROP DATABASE IF EXISTS encrypt_write_ds_3;
DROP DATABASE IF EXISTS encrypt_write_ds_4;
DROP DATABASE IF EXISTS encrypt_write_ds_5;
DROP DATABASE IF EXISTS encrypt_write_ds_6;
DROP DATABASE IF EXISTS encrypt_write_ds_7;
DROP DATABASE IF EXISTS encrypt_write_ds_8;
DROP DATABASE IF EXISTS encrypt_write_ds_9;
DROP DATABASE IF EXISTS encrypt_read_ds_0;
DROP DATABASE IF EXISTS encrypt_read_ds_1;
DROP DATABASE IF EXISTS encrypt_read_ds_2;
DROP DATABASE IF EXISTS encrypt_read_ds_3;
DROP DATABASE IF EXISTS encrypt_read_ds_4;
DROP DATABASE IF EXISTS encrypt_read_ds_5;
DROP DATABASE IF EXISTS encrypt_read_ds_6;
DROP DATABASE IF EXISTS encrypt_read_ds_7;
DROP DATABASE IF EXISTS encrypt_read_ds_8;
DROP DATABASE IF EXISTS encrypt_read_ds_9;

CREATE DATABASE encrypt_write_ds_0;
CREATE DATABASE encrypt_write_ds_1;
CREATE DATABASE encrypt_write_ds_2;
CREATE DATABASE encrypt_write_ds_3;
CREATE DATABASE encrypt_write_ds_4;
CREATE DATABASE encrypt_write_ds_5;
CREATE DATABASE encrypt_write_ds_6;
CREATE DATABASE encrypt_write_ds_7;
CREATE DATABASE encrypt_write_ds_8;
CREATE DATABASE encrypt_write_ds_9;
CREATE DATABASE encrypt_read_ds_0;
CREATE DATABASE encrypt_read_ds_1;
CREATE DATABASE encrypt_read_ds_2;
CREATE DATABASE encrypt_read_ds_3;
CREATE DATABASE encrypt_read_ds_4;
CREATE DATABASE encrypt_read_ds_5;
CREATE DATABASE encrypt_read_ds_6;
CREATE DATABASE encrypt_read_ds_7;
CREATE DATABASE encrypt_read_ds_8;
CREATE DATABASE encrypt_read_ds_9;

CREATE DATABASE rdl_test_0;
CREATE DATABASE rdl_test_1;
CREATE DATABASE rdl_test_2;

CREATE TABLE encrypt_write_ds_0.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_0.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_0.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_0.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_0.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_0.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_0.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_0.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_0.t_single_table (single_id INT NOT NULL, id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (single_id));
CREATE TABLE encrypt_write_ds_0.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_write_ds_0.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_write_ds_0.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_write_ds_0.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_write_ds_0.t_user_3 (user_id);

CREATE TABLE encrypt_write_ds_1.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_1.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_1.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_1.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_1.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_1.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_1.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_1.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_1.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_write_ds_1.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_write_ds_1.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_write_ds_1.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_write_ds_1.t_user_3 (user_id);

CREATE TABLE encrypt_write_ds_2.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_2.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_2.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_2.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_2.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_2.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_2.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_2.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_2.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_write_ds_2.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_write_ds_2.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_write_ds_2.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_write_ds_2.t_user_3 (user_id);

CREATE TABLE encrypt_write_ds_3.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_3.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_3.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_3.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_3.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_3.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_3.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_3.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_3.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_write_ds_3.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_write_ds_3.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_write_ds_3.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_write_ds_3.t_user_3 (user_id);

CREATE TABLE encrypt_write_ds_4.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_4.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_4.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_4.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_4.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_4.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_4.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_4.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_4.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_write_ds_4.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_write_ds_4.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_write_ds_4.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_write_ds_4.t_user_3 (user_id);

CREATE TABLE encrypt_write_ds_5.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_5.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_5.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_5.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_5.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_5.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_5.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_5.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_5.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_write_ds_5.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_write_ds_5.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_write_ds_5.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_write_ds_5.t_user_3 (user_id);

CREATE TABLE encrypt_write_ds_6.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_6.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_6.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_6.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_6.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_6.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_6.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_6.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_6.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_write_ds_6.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_write_ds_6.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_write_ds_6.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_write_ds_6.t_user_3 (user_id);

CREATE TABLE encrypt_write_ds_7.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_7.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_7.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_7.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_7.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_7.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_7.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_7.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_7.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_write_ds_7.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_write_ds_7.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_write_ds_7.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_write_ds_7.t_user_3 (user_id);

CREATE TABLE encrypt_write_ds_8.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_8.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_8.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_8.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_8.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_8.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_8.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_8.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_8.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_write_ds_8.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_write_ds_8.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_write_ds_8.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_write_ds_8.t_user_3 (user_id);

CREATE TABLE encrypt_write_ds_9.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_9.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_9.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_9.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_9.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_9.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_9.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_write_ds_9.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_write_ds_9.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_write_ds_9.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_write_ds_9.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_write_ds_9.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_write_ds_9.t_user_3 (user_id);

CREATE TABLE encrypt_read_ds_0.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_0.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_0.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_0.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_0.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_0.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_0.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_0.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_0.t_single_table (single_id INT NOT NULL, id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (single_id));
CREATE TABLE encrypt_read_ds_0.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_read_ds_0.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_read_ds_0.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_read_ds_0.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_read_ds_0.t_user_3 (user_id);

CREATE TABLE encrypt_read_ds_1.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_1.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_1.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_1.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_1.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_1.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_1.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_1.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_1.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_read_ds_1.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_read_ds_1.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_read_ds_1.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_read_ds_1.t_user_3 (user_id);

CREATE TABLE encrypt_read_ds_2.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_2.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_2.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_2.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_2.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_2.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_2.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_2.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_2.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_read_ds_2.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_read_ds_2.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_read_ds_2.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_read_ds_2.t_user_3 (user_id);

CREATE TABLE encrypt_read_ds_3.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_3.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_3.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_3.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_3.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_3.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_3.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_3.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_3.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_read_ds_3.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_read_ds_3.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_read_ds_3.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_read_ds_3.t_user_3 (user_id);

CREATE TABLE encrypt_read_ds_4.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_4.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_4.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_4.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_4.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_4.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_4.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_4.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_4.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_read_ds_4.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_read_ds_4.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_read_ds_4.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_read_ds_4.t_user_3 (user_id);

CREATE TABLE encrypt_read_ds_5.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_5.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_5.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_5.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_5.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_5.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_5.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_5.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_5.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_read_ds_5.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_read_ds_5.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_read_ds_5.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_read_ds_5.t_user_3 (user_id);

CREATE TABLE encrypt_read_ds_6.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_6.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_6.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_6.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_6.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_6.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_6.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_6.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_6.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_read_ds_6.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_read_ds_6.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_read_ds_6.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_read_ds_6.t_user_3 (user_id);

CREATE TABLE encrypt_read_ds_7.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_7.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_7.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_7.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_7.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_7.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_7.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_7.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_7.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_read_ds_7.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_read_ds_7.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_read_ds_7.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_read_ds_7.t_user_3 (user_id);

CREATE TABLE encrypt_read_ds_8.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_8.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_8.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_8.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_8.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_8.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_8.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_8.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_8.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_read_ds_8.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_read_ds_8.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_read_ds_8.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_read_ds_8.t_user_3 (user_id);

CREATE TABLE encrypt_read_ds_9.t_user_0 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_9.t_user_item_0 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_9.t_user_1 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_9.t_user_item_1 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_9.t_user_2 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_9.t_user_item_2 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_9.t_user_3 (user_id INT NOT NULL, address_id INT NOT NULL, pwd_plain VARCHAR(45) NULL, pwd_cipher VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE encrypt_read_ds_9.t_user_item_3 (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE encrypt_read_ds_9.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX user_index_t_user_0 ON encrypt_read_ds_9.t_user_0 (user_id);
CREATE INDEX user_index_t_user_1 ON encrypt_read_ds_9.t_user_1 (user_id);
CREATE INDEX user_index_t_user_2 ON encrypt_read_ds_9.t_user_2 (user_id);
CREATE INDEX user_index_t_user_3 ON encrypt_read_ds_9.t_user_3 (user_id);
