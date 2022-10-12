
# CrossClusterComputing
multiple server management

## 项目简介
&emsp;CCC系统核心目标解决多集群环境下进行大量同质作业调度的问题。项目整体采用FastAPI框架，采用paramico进行slurm集群的通信，使用SqlLite作为数据库技术来存储系统的状态。CCC项目在对外提供服务采用REST请求的方式。


# 依赖
&emsp;如常见项目，在运行项目时，需要首先安装有关依赖
```bash
pip install -r requirements.txt
```

# 运行程序
```bash
python main.py
```

# 启动接口
```bash
http://127.0.0.1:8001/docs#/
```

