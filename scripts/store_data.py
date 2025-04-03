import psycopg2
from psycopg2 import sql

def store_data(connection, data):
    """
    Store processed data into the PostgreSQL database.
    
    Parameters:
    connection (psycopg2.connection): The database connection object.
    data (list): A list of dictionaries containing the data to be stored.
    """
    with connection.cursor() as cursor:
        for record in data:
            insert_query = sql.SQL("""
                INSERT INTO assets (name, type, value, date)
                VALUES (%s, %s, %s, %s)
            """)
            cursor.execute(insert_query, (record['name'], record['type'], record['value'], record['date']))
        connection.commit()

if __name__ == "__main__":
    # Example usage
    connection = psycopg2.connect(
        dbname='fintrack_db',
        user='your_username',
        password='your_password',
        host='localhost',
        port='5432'
    )
    
    # Sample data to store
    sample_data = [
        {'name': 'Bitcoin', 'type': 'cryptocurrency', 'value': 45000, 'date': '2023-10-01'},
        {'name': 'Apple', 'type': 'stock', 'value': 150, 'date': '2023-10-01'}
    ]
    
    store_data(connection, sample_data)
    connection.close()