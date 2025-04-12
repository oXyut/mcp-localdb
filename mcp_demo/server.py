import psycopg2
from psycopg2 import sql
import anyio
import click
import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("PostgreSQL Explorer")

# PostgreSQL接続情報 - docker-compose.ymlから取得
DB_NAME = "devdb"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"  # サービス名をホスト名として使用
DB_PORT = "15432"

def get_connection():
    """PostgreSQLデータベースへの接続を返す"""
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )

# @mcp.resource("schema://main")
def get_schema() -> str:
    """データベーススキーマをリソースとして提供する"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 
            table_name, 
            column_name, 
            data_type 
        FROM 
            information_schema.columns 
        WHERE 
            table_schema = 'public'
        ORDER BY 
            table_name, 
            ordinal_position
    """)
    schema = cursor.fetchall()
    cursor.close()
    conn.close()
    
    result = []
    current_table = None
    for table, column, data_type in schema:
        if current_table != table:
            current_table = table
            result.append(f"TABLE {table} (")
        result.append(f"  {column} {data_type},")
    
    return "\n".join(result)


# @mcp.tool()
def query_data(sql: str) -> str:
    """安全にSQLクエリを実行する"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return "\n".join(str(row) for row in result)
    except Exception as e:
        return f"Error: {str(e)}"

@click.command()
@click.option("--port", default=8000, help="Port to listen on for SSE")
@click.option(
    "--transport",
    type=click.Choice(["stdio", "sse"]),
    default="stdio",
    help="Transport type",
)
def main(port: int, transport: str) -> int:
    app = Server("postgresql-explorer")

    @app.call_tool()
    async def call_tool(name: str, arguments: dict) -> list[types.TextContent]:
        if name == "query_data":
            if "sql" not in arguments:
                raise ValueError("Missing required argument 'sql'")
            result = query_data(arguments["sql"])
            return [types.TextContent(type="text", text=result)]
        else:
            raise ValueError(f"Unknown tool: {name}")

    @app.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name="query_data",
                description="安全にSQLクエリを実行する",
                inputSchema={
                    "type": "object",
                    "required": ["sql"],
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "実行するSQLクエリ",
                        }
                    },
                },
            )
        ]

    @app.read_resource()
    async def read_resource(uri: str) -> str:
        name = uri.lstrip("/")
        
        if name == "schema://main" or name == "schema/main":
            return get_schema()
        
        raise ValueError(f"Unknown resource: {uri}")

    if transport == "sse":
        from mcp.server.sse import SseServerTransport
        from starlette.applications import Starlette
        from starlette.routing import Mount, Route

        sse = SseServerTransport("/messages/")

        async def handle_sse(request):
            async with sse.connect_sse(
                request.scope, request.receive, request._send
            ) as streams:
                await app.run(
                    streams[0], streams[1], app.create_initialization_options()
                )

        starlette_app = Starlette(
            debug=True,
            routes=[
                Route("/sse", endpoint=handle_sse),
                Mount("/messages/", app=sse.handle_post_message),
            ],
        )

        import uvicorn

        uvicorn.run(starlette_app, host="0.0.0.0", port=port)
    else:
        from mcp.server.stdio import stdio_server

        async def arun():
            async with stdio_server() as streams:
                await app.run(
                    streams[0], streams[1], app.create_initialization_options()
                )

        anyio.run(arun)

    return 0

if __name__ == "__main__":
    main()