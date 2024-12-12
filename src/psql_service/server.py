import os
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from contextlib import closing
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
from pydantic import AnyUrl
from typing import Any
from dotenv import load_dotenv

logger = logging.getLogger('mcp-psql-server')
logger.info("Starting MCP PostgreSQL Server")

PROMPT_TEMPLATE = """
アシスタントの目標はPostgreSQLデータベースと接続し、適切に機能するModel Context Protocol (mcp)サーバーを提供することです。ユーザーはすでにプロンプトを使用し、トピックを提供しています。トピックは次のとおりです：{topic}。ユーザーはこれで作業を開始する準備ができました。

mcpとこのmcpサーバーに関する追加情報を以下に示します。
<mcp>
Prompts:
このプロンプトは「トピック」引数を受け入れ、テーブルの作成、データの分析、洞察の生成をユーザーにガイドします。例えば、ユーザーがトピックとして「小売販売」と入力すると、プロンプトが関連するデータベーステーブルの作成を支援し、分析プロセスをガイドします。プロンプトは基本的に、LLMとの対話を有益な方法で構築する対話型テンプレートとして機能します。

Resources:
このサーバーは、1つの主要リソース「memo://insights」を公開しています。これは、分析プロセス全体を通じて自動的に更新されるビジネスインサイトメモです。ユーザーがデータベースを分析し、洞察を発見するにつれ、メモリソースは新しい発見を反映してリアルタイムで更新されます。リソースは、会話に文脈を提供する生きた文書として機能します。

Tools:
このサーバーは、いくつかのSQL関連ツールを提供しています。
「read-query」：SELECTクエリを実行してデータベースからデータを読み込みます
「write-query」：INSERT、UPDATE、またはDELETEクエリを実行してデータを変更します
「create-table」：データベースに新しいテーブルを作成します
「list-tables」：既存のすべてのテーブルを表示します 
「describe-table」：特定のテーブルのスキーマを表示します
「append-insight」：新しいビジネスインサイトをメモリソースに追加します
</mcp>

<instructions>
あなたは、与えられたトピックに基づいて包括的なビジネスシナリオを生成する任務を負ったAIアシスタントす。あなたの目標は、データ駆動型のビジネス問題を伴う物語を作成し、それをサポートするデータベース構造を開発し、関連するクエリを生成し、ダッシュボードを作成し、最終的なソリューションを提供することです。

以下の各ステップでは、ユーザー入力を一時停止してシナリオ作成プロセスを導きますが、シナリオを完成まで導いてください。また、すべてのXMLタグはアシスタントが理解するためのものであり、最終的な出力には含めるべきではありません。

1. ユーザーはトピックを選択しました：{topic}。

2. ビジネス上の問題に関する説明を作成します。
a. 指定されたトピックに基づいて、ビジネス上の状況または問題を大まかに説明します。
b. データベースからデータを収集および分析する必要があるアクターを登場させます。
c. データがまだ準備されていない場合、それについて言及します。

3. データのセットアップ：
a. シナリオに必要なデータについて尋ねるのではなく、ツールを使用してデータを直接作成します。テーブルの更新を伴う場合は、作業を進めて良いか必ず確認を行なってください。
b. ビジネス上の問題に必要なデータを表すテーブルスキーマを設計します。
c. 適切なカラムとデータタイプを持つテーブルを少なくとも2～3個含めます。
d. PostgreSQLデータベースにテーブルを作成するツールを活用する。
e. 関連する合成データで各テーブルを埋めるINSERT文を作成する。
f. データが多様であり、ビジネス上の問題を代表するものであることを確認する。

4. ユーザー入力を一時停止する：
a. 作成したデータの概要をユーザーに伝える。
b. 次のステップとして、複数の選択肢をユーザーに提示する。
c. これらの複数の選択肢は自然言語で提示し、ユーザーがいずれかを選択すると、アシスタントが関連するクエリを生成し、適切なツールを活用してデータを取得する。

5. クエリを繰り返す：
a. ユーザーに1つの追加の複数選択肢のクエリオプションを提示します。短いデモなので、あまり何度も繰り返さないことが重要です。
b. 各クエリオプションの目的を説明します。
c. ユーザーがクエリオプションのいずれかを選択するまで待ちます。
d. 各クエリの後、結果について必ず意見を述べます。
e. データ分析から発見されたビジネス上の洞察をえるために、append-insightツールを使用します。

6. ダッシュボードの作成：
a. すべてのデータとクエリが揃ったので、ダッシュボードを作成する時が来ました。アーティファクトを使用して作成します。
b. 表、チャート、グラフなど、さまざまな視覚化を使用してデータを表現します。
c. ダッシュボードの各要素がビジネス上の問題とどのように関連しているかを説明します。
d. このダッシュボードは理論的には最終的なソリューションメッセージに含まれることになります。

7. 最終的なソリューションメッセージを作成する：
a. あなたがアプリンサイトツールを使用している間、リソースは次の場所で見つかりました：memo://insightsが更新されました。
b. 分析の各段階でメモが更新されたことをユーザーに通知することが重要です。
c. ユーザーに添付メニュー（クリップアイコン）に移動し、MCPメニュー（2つの電気プラグが接続されている）を選択し、統合を選択するように依頼します。「ビジネスインサイトメモ」を選択します。
d. これにより、生成されたメモがチャットに添付されます。このチャットを使用して、デモに関連する追加のコンテキストを追加することができます。
e. 最終的なメモをアーティファクトとしてユーザーに提示します。

8. シナリオを終了します。
a. ユーザーに、これはPostgreSQL MCP Serverでできることのほんの始まりにすぎないことを説明します。
</instructions>

シナリオ全体を通して一貫性を維持し、すべての要素（テーブル、データ、クエリ、ダッシュボード、ソリューション）が元のビジネス上の問題と与えられたトピックに密接に関連していることを確認してください。
提供されたXMLタグはアシスタントが理解できるようにするためのものです。すべての出力を可能な限り人間が読めるようにしてください。
"""

class PostgresDatabase:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.insights: list[str] = []

    def _init_database(self):
        """Initialize connection to the PostgreSQL database"""
        logger.debug("Initializing database connection")
        with closing(psycopg2.connect(self.db_url)) as conn:
            conn.close()

    def _synthesize_memo(self) -> str:
        """Synthesizes business insights into a formatted memo"""
        logger.debug(f"Synthesizing memo with {len(self.insights)} insights")
        if not self.insights:
            return "No business insights have been discovered yet."

        insights = "\n".join(f"- {insight}" for insight in self.insights)

        memo = "📊 Business Intelligence Memo 📊\n\n"
        memo += "Key Insights Discovered:\n\n"
        memo += insights

        if len(self.insights) > 1:
            memo += "\nSummary:\n"
            memo += f"Analysis has revealed {len(self.insights)} key business insights that suggest opportunities for strategic optimization and growth."

        logger.debug("Generated basic memo format")
        return memo

    def _execute_query(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Execute a SQL query and return results as a list of dictionaries"""
        logger.debug(f"Executing query: {query}")
        try:
            with closing(psycopg2.connect(self.db_url)) as conn:
                with closing(conn.cursor(cursor_factory=RealDictCursor)) as cursor:
                    if params:
                        cursor.execute(query, params)
                    else:
                        cursor.execute(query)

                    if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER')):
                        conn.commit()
                        affected = cursor.rowcount
                        logger.debug(f"Write query affected {affected} rows")
                        return [{"affected_rows": affected}]

                    results = cursor.fetchall()
                    logger.debug(f"Read query returned {len(results)} rows")
                    return results
        except Exception as e:
            logger.error(f"Database error executing query: {e}")
            raise

async def main():
    load_dotenv()
    PGDATABASE = os.getenv("PGDATABASE")
    PGUSER = os.getenv("PGUSER")
    PGPASSWORD = os.getenv("PGPASSWORD")
    PGHOST = os.getenv("PGHOST")
    PGPORT = os.getenv("PGPORT")
    db_url = (
        f"dbname={PGDATABASE} "
        f"user={PGUSER} "
        f"password={PGPASSWORD} "
        f"host={PGHOST} "
        f"port={PGPORT}"
    )
    logger.info(f"Starting PostgreSQL MCP Server with DB URL: {db_url}")

    db = PostgresDatabase(db_url)
    server = Server("psql-manager")

    # Register handlers
    logger.debug("Registering handlers")

    @server.list_resources()
    async def handle_list_resources() -> list[types.Resource]:
        logger.debug("Handling list_resources request")
        return [
            types.Resource(
                uri=AnyUrl("memo://insights"),
                name="Business Insights Memo",
                description="A living document of discovered business insights",
                mimeType="text/plain",
            )
        ]

    @server.read_resource()
    async def handle_read_resource(uri: AnyUrl) -> str:
        logger.debug(f"Handling read_resource request for URI: {uri}")
        if uri.scheme != "memo":
            logger.error(f"Unsupported URI scheme: {uri.scheme}")
            raise ValueError(f"Unsupported URI scheme: {uri.scheme}")

        path = str(uri).replace("memo://", "")
        if not path or path != "insights":
            logger.error(f"Unknown resource path: {path}")
            raise ValueError(f"Unknown resource path: {path}")

        return db._synthesize_memo()

    @server.list_prompts()
    async def handle_list_prompts() -> list[types.Prompt]:
        logger.debug("Handling list_prompts request")
        return [
            types.Prompt(
                name="mcp-demo",
                description="A prompt to seed the database with initial data and demonstrate what you can do with an PostgreSQL MCP Server + Claude",
                arguments=[
                    types.PromptArgument(
                        name="topic",
                        description="Topic to seed the database with initial data",
                        required=True,
                    )
                ],
            )
        ]

    @server.get_prompt()
    async def handle_get_prompt(name: str, arguments: dict[str, str] | None) -> types.GetPromptResult:
        logger.debug(f"Handling get_prompt request for {name} with args {arguments}")
        if name != "mcp-demo":
            logger.error(f"Unknown prompt: {name}")
            raise ValueError(f"Unknown prompt: {name}")

        if not arguments or "topic" not in arguments:
            logger.error("Missing required argument: topic")
            raise ValueError("Missing required argument: topic")

        topic = arguments["topic"]
        prompt = PROMPT_TEMPLATE.format(topic=topic)

        logger.debug(f"Generated prompt template for topic: {topic}")
        return types.GetPromptResult(
            description=f"Demo template for {topic}",
            messages=[
                types.PromptMessage(
                    role="user",
                    content=types.TextContent(type="text", text=prompt.strip()),
                )
            ],
        )

    @server.list_tools()
    async def handle_list_tools() -> list[types.Tool]:
        """List available tools"""
        return [
            types.Tool(
                name="read-query",
                description="Execute a SELECT query on the PostgreSQL database",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "SELECT SQL query to execute"},
                    },
                    "required": ["query"],
                },
            ),
            types.Tool(
                name="write-query",
                description="Execute an INSERT, UPDATE, or DELETE query on the PostgreSQL database",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "SQL query to execute"},
                    },
                    "required": ["query"],
                },
            ),
            types.Tool(
                name="create-table",
                description="Create a new table in the PostgreSQL database",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "CREATE TABLE SQL statement"},
                    },
                    "required": ["query"],
                },
            ),
            types.Tool(
                name="list-tables",
                description="List all tables in the PostgreSQL database",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            types.Tool(
                name="describe-table",
                description="Get the schema information for a specific table",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table_name": {"type": "string", "description": "Name of the table to describe"},
                    },
                    "required": ["table_name"],
                },
            ),
            types.Tool(
                name="append-insight",
                description="Add a business insight to the memo",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "insight": {"type": "string", "description": "Business insight discovered from data analysis"},
                    },
                    "required": ["insight"],
                },
            ),
        ]

    @server.call_tool()
    async def handle_call_tool(
        name: str, arguments: dict[str, Any] | None
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        """Handle tool execution requests"""
        try:
            if name == "list-tables":
                results = db._execute_query(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "describe-table":
                if not arguments or "table_name" not in arguments:
                    raise ValueError("Missing table_name argument")
                # パラメータをタプルとして渡すように修正
                results = db._execute_query(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = %s AND table_schema = 'public'
                    """,
                    (arguments["table_name"],)  # タプルとして渡す
                )
                return [types.TextContent(type="text", text=str(results))]

            elif name == "append-insight":
                if not arguments or "insight" not in arguments:
                    raise ValueError("Missing insight argument")

                db.insights.append(arguments["insight"])
                _ = db._synthesize_memo()

                # Notify clients that the memo resource has changed
                await server.request_context.session.send_resource_updated(AnyUrl("memo://insights"))

                return [types.TextContent(type="text", text="Insight added to memo")]

            if not arguments:
                raise ValueError("Missing arguments")

            if name == "read-query":
                if not arguments["query"].strip().upper().startswith("SELECT"):
                    raise ValueError("Only SELECT queries are allowed for read-query")
                results = db._execute_query(arguments["query"])
                return [types.TextContent(type="text", text=str(results))]

            elif name == "write-query":
                if arguments["query"].strip().upper().startswith("SELECT"):
                    raise ValueError("SELECT queries are not allowed for write-query")
                db._execute_query(arguments["query"])
                return [types.TextContent(type="text", text="Query executed successfully")]

            elif name == "create-table":
                if not arguments["query"].strip().upper().startswith("CREATE TABLE"):
                    raise ValueError("Only CREATE TABLE statements are allowed")
                db._execute_query(arguments["query"])
                return [types.TextContent(type="text", text="Table created successfully")]

            else:
                raise ValueError(f"Unknown tool: {name}")

        except Exception as e:
            return [types.TextContent(type="text", text=f"Error: {str(e)}")]

    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        logger.info("Server running with stdio transport")
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="psql-manager",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )
