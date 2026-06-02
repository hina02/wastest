from dataclasses import dataclass

from pydantic_ai import Agent, RunContext
from pydantic_ai.messages import ModelMessage

import lance_reader as lr
from config import CHAT_MODEL

PREAMBLE = """\
あなたはドキュメント検索アシスタントです。
ユーザーの質問に答えるために、必要に応じて以下のツールを使ってください。
- hybrid_search: クエリに関連する statement をハイブリッド検索する。
- get_content: statement_id からソース全文を取得する。
得られた情報を基に、簡潔かつ正確に回答してください。\
"""


@dataclass
class Deps:
    namespace: str


agent = Agent(CHAT_MODEL, deps_type=Deps, system_prompt=PREAMBLE)


@agent.tool
def hybrid_search(ctx: RunContext[Deps], query: str, limit: int = 10) -> list[dict]:
    """クエリに関連する statement をハイブリッド検索する"""
    results = lr.search_hybrid(ctx.deps.namespace, query, limit)
    return [
        {
            "statement_id": r["id"],
            "statement": r["statement"],
            "keywords": r.get("keywords") or [],
        }
        for r in results
    ]


@agent.tool
def get_content(ctx: RunContext[Deps], statement_id: str) -> str:
    """statement_id からソース全文を取得する"""
    return lr.get_content(ctx.deps.namespace, statement_id)


def run(
    message: str,
    namespace: str,
    msg_history: list[ModelMessage],
) -> tuple[str, list[ModelMessage]]:
    result = agent.run_sync(
        message,
        deps=Deps(namespace=namespace),
        message_history=msg_history,
    )
    return result.output, result.new_messages()
