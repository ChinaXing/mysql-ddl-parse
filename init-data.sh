#!/bin/bash
cat <<EOF | mysql -h192.168.88.103 -utest -ptest -P3306 
      drop database new_console; create database new_console; use new_console ; source new-console.sql;
      drop database prod_console; create database prod_console; use prod_console; source prod-console.sql;
      use prod_console;
      alter table ClusterConfig  add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境';
      alter table ProxyServerConfig  add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境';
      alter table Proxy  add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境';
      alter table Cluster  add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境';
      alter table MySQLHost add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境',
            drop OriginalID;
      alter table MySQLInstance add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境',
           drop OriginalID;
      alter table HaGroup  add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境',
            drop OriginalID;
      alter table InstanceGroup  add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境';
      alter table SchemaAccount  add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境';
      alter table SchemaMeta  add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境';
      alter table TableMeta add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境',
            add RelativeID INT(10) UNSIGNED NOT NULL, add Version INT(10) UNSIGNED NOT NULL COMMENT '当前版本';
      alter table ShardFunction  add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境';
      alter table TableRule  add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境';
      alter table Shards  add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境';
      alter table Shard  add Env VARBINARY(16) NOT NULL COMMENT '环境标识,包括PROD,QA,DAILY等环境';
      delete from prod_console.TableMeta where state != 'ONLINE';
      delete from SchemaMeta where state != 'ONLINE';
      delete from prod_console.TableMeta where SchemaID NOT IN (select ID from prod_console.SchemaMeta);
      delete from ProxyServerConfig where ClusterID NOT IN (SELECT ID from Cluster);
      delete from ClusterConfig where ClusterID NOT In (SELECT ID from Cluster);
      delete from  Proxy where ClusterID NOT In (SELECT ID from Cluster);
      alter table prod_console.TableMeta add index SchemaID_Name (SchemaID, Name);
      update Cluster set Env = 'PROD';
      update MySQLHost set Env = 'PROD';
      update MySQLInstance set Env = 'PROD';
      update InstanceGroup set Env = 'PROD';
      update HaGroup set Env = 'PROD';
      update SchemaMeta set Env = 'PROD';
      update SchemaAccount set Env = 'PROD';
      update TableMeta set Env = 'PROD';
      update TableRule set Env = 'PROD';
      update ShardFunction set Env = 'PROD';
      update Shard set Env = 'PROD';
      update Shards set Env = 'PROD';
      update Proxy set Env = 'PROD';
      update ProxyServerConfig set Env = 'PROD';
      update ClusterConfig set Env = 'PROD';
EOF
