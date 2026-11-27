# One image for every part; the ROLE setting picks web / orchestrator / editor / writer
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv

# Install packages first, so Docker can reuse this layer when only the code changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Don't run as root inside the container (security)
RUN useradd --uid 10001 --create-home appuser
USER 10001

EXPOSE 8000
CMD ["python", "-m", "app.main"]
