import redis
import json
from db_manager import DuckDBManager

redis_client = redis.Redis(host='localhost', port=6379, db=0)

async def listen_to_redis():
    pubsub = redis_client.pubsub()
    pubsub.subscribe('sheet_operations')

    for message in pubsub.listen():
        if message['type'] == 'message':
            print(f"Raw message from Redis: {message['data']}")
            try:
                operation = json.loads(message['data'])
                handle_operation(operation)
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON: {e}")
                print(f"Invalid JSON message: {message['data']}")

def handle_operation(operation):
    operation_type = operation['type']
    sheet_id = operation['sheet_id']

    db_manager = DuckDBManager(sheet_id)

    if operation_type == 'create_sheet':
        pass  
    elif operation_type == 'list_tabs':
        tabs = db_manager.list_tabs()
        print(tabs)
    elif operation_type == 'create_tab':
        tab_name = operation['tab_name']
        db_manager.create_tab(tab_name)
    elif operation_type == 'insert_or_update_cell':
        tab_id = operation['tab_id']
        row_index = operation['row_index']
        column_index = operation['column_index']
        value = operation['value']
        db_manager.insert_or_update_cell(tab_id, row_index, column_index, value)
    elif operation_type == 'read_tab':
        tab_id = operation['tab_id']
        data = db_manager.read_tab(tab_id)
        redis_client.publish('sheet_read_responses', json.dumps({
            'sheet_id': sheet_id,
            'tab_id': tab_id,
            'data': data
        }))
    elif operation_type == 'delete_tab':
        tab_id = operation['tab_id']
        db_manager.delete_tab(tab_id)
    elif operation_type == 'delete_sheet':
        db_manager.delete_sheet()
