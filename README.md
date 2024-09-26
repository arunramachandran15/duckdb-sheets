### Problem statement : 

- Implement a google sheet like frontend interface using Swelete
- Send real time updates in the sheets to using a data loader to DuckDB 
- Receive the real time updates using a web server and socket implementation
- Another user using the sheets should be able to see the updates in realtime and also anybody using the DuckDB for their processing with real time updates.
- To use docker and Kubernetes, implement DuckDB as statefulset in K8s (For db file persistent storage)

### UI / APIs :
- List all sheets interface (Integrate with GET /sheets api)
- Sheet interface (Integrate with Get /sheets/sheet-id api) (To fetch the initial sheet data and reconciliation with real time updates)
- Websocket client to send and receive real time updates from server
- Mount the duckdb file as a persistent volume in k8s.
