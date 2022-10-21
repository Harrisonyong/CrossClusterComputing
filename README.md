
# CrossClusterComputing
multiple server management

## 项目简介
&emsp;CCC系统核心目标解决多集群环境下进行大量同质作业调度的问题。项目整体采用FastAPI框架，采用paramico进行slurm集群的通信，使用SqlLite作为数据库技术来存储系统的状态。CCC项目在对外提供服务采用REST请求的方式。
> 由于本项目使用sqlite数据库，在观察数据库表状态时，使用SQLiteStudio软件即可。下载地址为 (下载地址)[https://www.sqlitestudio.pl/]，使用较为简单，不再赘述。

> 操作数据库使用的是sqlalchemy框架，具体信息参考文档 (下载地址)[https://www.osgeo.cn/sqlalchemy/orm/tutorial.html]
>

# 依赖
&emsp;如常见项目，在运行项目时，需要首先安装有关依赖
```bash
pip install -r requirements.txt
```
# 配置
在运行项目之前，请首先正确配置两个文件
## db.ini
&emsp;db.ini中存储的是项目使用的数据库信息
```ini
[db]
file=sqlite:///data\jobs.db
host=0.0.0.0
port=69
user=root
password=root
```
> 注：由于当前使用的是sqlite，因此当前实际只有file属性有效。

&emsp;在运行之前，请先在SQLiteStudio中使用sql编辑器运行db/table_definitions.txt中的表定义。创建数据表。
## service.config
&emsp;该配置文件中保存了接入的多个slurm信息，请确保slurm集群的标识以slurm开头，并且唯一

```ini
[slurm1]
host=10.101.12.48
port=22
user=root
password=szfyd@123

[slurm2]
host=10.101.12.48
port=22
user=root
password=szfyd@123
```
# 运行程序
```bash
python main.py
```

# 启动接口
```bash
http://127.0.0.1:8001/docs#/
```

