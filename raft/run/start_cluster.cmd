@echo off
title ComDB Raft Cluster - Build & Run

:: 设置ZooKeeper路径
set ZK_HOME=A:\apache-zookeeper-3.7.2-bin
set ZK_SCRIPT=%ZK_HOME%\bin\zkServer.cmd

:: 先启动ZooKeeper
echo Starting ZooKeeper server...
start cmd /k "title ZooKeeper && cd /d %ZK_HOME%\bin && zkServer.cmd"

:: 等待ZooKeeper启动完成
echo Waiting for ZooKeeper to initialize...
timeout /t 10 >nul

:: 启动所有Raft节点
echo Starting ComDB Raft nodes...
start cmd /k "title Node1 && cd /d %~dp0 && node1.exe"
start cmd /k "title Node2 && cd /d %~dp0 && node2.exe"
start cmd /k "title Node3 && cd /d %~dp0 && node3.exe"
echo All nodes started. Check each terminal for logs.
echo ZooKeeper is running in separate window.
pause1