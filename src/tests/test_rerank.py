from apps.api.services.retrieval.hybrid import RetrievalCandidate
from apps.api.services.retrieval.rerank import rerank_candidates


def _cand(
    *,
    chunk_id: str,
    notice_id: str,
    title: str,
    text: str,
    buyer_name: str | None = None,
    region: str | None = None,
    score: float = 0.5,
) -> RetrievalCandidate:
    return RetrievalCandidate(
        chunk_id=chunk_id,
        notice_id=notice_id,
        doc_id=None,
        title=title,
        url=None,
        doc_url=None,
        text=text,
        dense_score=score,
        bm25_score=0.0,
        final_score=score,
        metadata={"buyer_name": buyer_name, "region": region},
    )


def test_rerank_boosts_query_location_terms():
    question = "Zurich IT services tenders deadline next 30 days, what are key requirements?"
    candidates = [
        _cand(
            chunk_id="c1",
            notice_id="n1",
            title="Service de voituriers",
            text="French airport valet concession requirements and submission terms.",
            buyer_name="Genève Aéroport",
            score=0.9,
        ),
        _cand(
            chunk_id="c2",
            notice_id="n2",
            title="Rahmenvertrag für Dienstleistungen in Business Analyse",
            text="IT service tender with deadline and qualification requirements.",
            buyer_name="Kanton Zürich, Bildungsdirektion",
            score=0.82,
        ),
    ]

    out = rerank_candidates(question=question, candidates=candidates, filters=None, top_k=2)
    assert out[0].notice_id == "n2"
