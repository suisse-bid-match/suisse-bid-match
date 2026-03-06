<p align="center">
  <img src="assets/SuisseBidMatchBanner.png" alt="SuisseBidMatch banner" width="900" />
</p>

<p align="center">
  <strong>SuisseBidMatch</strong><br/>
  Spec- & rule-driven tender matching for Swiss public procurement (SIMAP).
</p>

<p align="center">
  <a href="https://fastapi.tiangolo.com/">
    <img alt="FastAPI" src="https://img.shields.io/badge/FastAPI-API-009688?style=for-the-badge&logo=fastapi&logoColor=white" />
  </a>
  <a href="https://qdrant.tech/documentation/">
    <img alt="Qdrant" src="https://img.shields.io/badge/Qdrant-Vector%20DB-DC244C?style=for-the-badge&logo=qdrant&logoColor=white" />
  </a>
  <a href="https://www.postgresql.org/">
    <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-DB-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" />
  </a>
  <a href="https://alembic.sqlalchemy.org/">
    <img alt="Alembic" src="https://img.shields.io/badge/Alembic-Migrations-6B5B95?style=for-the-badge" />
  </a>
  <a href="https://docs.docker.com/compose/">
    <img alt="Docker Compose" src="https://img.shields.io/badge/Docker%20Compose-Dev%20Env-2496ED?style=for-the-badge&logo=docker&logoColor=white" />
  </a>
</p>

> Unofficial project — not affiliated with simap.ch or Swiss authorities.

## What it does
Turn your internal **product specs, capabilities, and business rules** into:
- ranked best-fit SIMAP tenders
- explainable match reasons

## Architecture
<p align="center">
  <img src="assets/Architecture.png" alt="SuisseBidMatch architecture" width="900" />
</p>

## Current MVP scope
- SIMAP ingestion + tender normalization
- RAG chat with citations
- Checklist extraction
- Change impact analysis
- Postgres-only persistence + Alembic migrations

## How does it help Tender Procedure

```mermaid
flowchart LR
    A[Opportunity discovery]
    B[Qualification decision]
    C[Bid drafting]
    D[Submission control]
    E[Award follow-up]
    F[Delivery support]

    A --> B --> C --> D --> E --> F

    A1[AI: match SIMAP notices<br/>to company profile] --> A
    B1[AI: extract requirements,<br/>score fit, flag risks] --> B
    C1[AI: draft prequalification file,<br/>cover letter, technical response,<br/>compliance matrix] --> C
    D1[AI: checklist, missing docs,<br/>deadline reminders] --> D
    E1[AI: summarize award notice,<br/>appeal/debrief support] --> E
    F1[AI: extract obligations,<br/>milestones, reporting tasks] --> F
```


## Run it
Implementation lives in `src/`.

```bash
cd src
cp .env.example .env
docker compose up -d --build
```

Then open:
- API docs: `http://localhost:8000/docs`
- UI: `http://localhost:3000`
