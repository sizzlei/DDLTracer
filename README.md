# DDLTracer
DDL Tracer is a DB operation support tool developed to track, notify, and store history of MySQL schema changes.

DDL Tracer supports:
- Add/Modify/Drop Table
- Add/Modify/Drop Column

> Changes to the index and additions and changes to the schema are not supported (to be added in the future).

This is data that is not collected through the Binary Log, but is collected using the information in the Information Schema.

In DDL Tracer, the main routine is executed for each server, and the subroutine is executed for each schema of the target server.
```
DDL Tracer
|--DB Server1
    |== Schema1
    |== Schema2
|--DB Server2
    |== Schema1
    |== Schema2
```
## Usage
### INIT
After configuring, initialize and save the target DB Definition.
`ddltracer.run -auth=param -conf=./conf.yml -region=ap-northeast-2 -key=[Parameter Store Key] -mode=INIT`

### Start
Start your comparison logic.
`ddltracer.run -auth=param -conf=./conf.yml -region=ap-northeast-2 -key=[Parameter Store Key] -mode=START`

## Using Clean History
Execute as below to remove the existing history and initialize it.
`ddltracer.run -auth=param -conf=./conf.yml -region=ap-northeast-2 -key=[Parameter Store Key] -history-clean -mode=INIT`
Purge history is disabled by default.

## Flag
DDL tracer should be run with the flag value below.
- mode
    - INIT : Saves initial sqlite settings and current definition data.
    - START : It does the comparison logic.
- auth
    - CONF : When the authentication method is set to Conf, the user account and password are loaded from the file specified in conf Flag.
    - PARAM : 
If the authentication method is set to PARAM, account information is loaded from AWS Parameter Store with the values ​​set in the region flag and key flag.
- conf : Configure File Path
- region : Region where the Parameter Store Key set in the KEY Flag exists
- key : AWS Parameter Store Key
- history-clean : Initialize the change history.

### Example
```shell
# Auth - Param
ddltracer.run -auth=param -conf=./conf.yml -region=ap-northeast-2 -key=[Parameter Store Key] -mode=start

# Auth- Conf
ddltracer.run -auth=conf -conf=./conf.yml -mode=start
```
> When using Conf, plaintext passwords are stored in Conf.yml, which can be a security risk, so it is recommended to use it through Parameter Store.


## Configure
```YML
Global:
  User:
  Pass:
  WebhookUrl: [Webhook URL]
  DBPath: /sqlite
  Compare_interval: 60
  AddTable_Column_view: true
Targets:
  - Alias: test1
    Endpoint: test1.cluster-cgau50yc2g7n.ap-northeast-2.rds.amazonaws.com
    Port: 3306
    DB: ["test_db1","test_db2"]
```
### Global
This is a global setting that applies equally to all clusters.
- User : DB Connection User
    - Only information_schema query permission is required.
        - `GRANT SELECT ON information_schema.* TO ''@''`
    - Can be omitted if the authentication method is Param.
- Pass : DB Connection Password
- WebhookUrl : Slack Webhook URL
- DBPath : SQLite file path where table definition is saved
- Compare_interval : Schema Compare Interval
    - If it is set to 60 seconds or less, it is automatically set to 60 seconds.
- AddTable_Column_view : Determines whether to add column items to Notification when creating table.


### Target
Set the target DB and schema according to the specifications below.
```yaml
- Alias: test1
    Endpoint: test1.cluster-cgau50yc2g7n.ap-northeast-2.rds.amazonaws.com
    Port: 3306
    DB: ["test_db1","test_db2"]
```
- Alias : Target server Alias ​​, independent of AWS Identifier.
- Endpoint : It is the connection address of the comparison target, and if possible, select a replica server.
- Port : DB Connection Port
- DB : An array of schemas to compare within the target server.

## Compare Database
The database to be compared is created and stored in SQLite.
### Compare DB Definitions
The sqlite DB file is saved in the format shown below.
- `[Schema_name]_[Server_Alias].db`
#### column_definitions
```sql
CREATE TABLE column_definitions (
    table_name text not null,
    column_name text not null,
    def_info text not null,
    column_type text not null,
    nullallowed text not null,
    comment text not null,
    PRIMARY KEY (table_name,column_name)
);
```
#### table_definitions
```sql
CREATE TABLE table_definitions (
    table_name text not null,
    def_info text not null,
    comment text not null,
    PRIMARY KEY (table_name)
);
```
#### definition_history
```sql
CREATE TABLE definition_history (
    table_name text not null,
    column_name text null default null,
    status text not null,
    def_info text null default null,
    created_dt text not null
);
```

#### Index
```sql
CREATE INDEX idx_tablename_columnname ON definition_history (table_name,column_name)
```

## Notification
When changes to tables and columns occur, notifications are raised via webhooks.
> In the case of `Rename Table` and `ALTER TABLE ... CHANGE ...`, it is treated as DROP/ADD.