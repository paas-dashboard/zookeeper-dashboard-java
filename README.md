# zookeeper-dashboard
zookeeper dashboard project for fun.

## backend api command
### put node
```bash
curl -X PUT -H "Content-Type: application/json" http://localhost:10002/api/zookeeper/nodes -d '{"path":"/test","value":"test"}'
```
### get nodes
```bash
curl -X POST -H "Content-Type: application/json" localhost:10002/api/zookeeper/get-nodes -d '{"path":"/test"}'
```
### get node
```bash
curl -X POST -H "Content-Type: application/json" localhost:10002/api/zookeeper/get-node -d '{"path":"/test"}'
```
