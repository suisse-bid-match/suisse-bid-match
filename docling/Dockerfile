FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgl1 libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /srv
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
ENV DOCLING_ARTIFACTS_PATH=/srv/docling-artifacts
RUN mkdir -p ${DOCLING_ARTIFACTS_PATH} \
    && docling-tools models download -o ${DOCLING_ARTIFACTS_PATH}
COPY app ./app

EXPOSE 8020
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8020"]
