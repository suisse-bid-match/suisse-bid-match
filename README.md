# suisse-bid-match-novel

End-to-end tender-to-product matching pipeline for lighting bids.

## Setup

```bash
cd suisse-bid-match-novel
python -m venv .venv
source .venv/bin/activate
pip install -r src/requirements.txt
```

## Run

```bash
python src/core/main.py "/home/daz/all_things_for_genai_hackathon/real_tenders/233 Beleuchtung-20260306T200148Z-3-001/233 Beleuchtung" \
  --config src/pipeline.yaml \
  --field-rules-json src/field_rules.json
```

Outputs are written under `src/runtime/<run_id>/`.

